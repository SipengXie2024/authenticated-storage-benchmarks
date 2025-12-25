//! Split 操作（节点分裂与 Parent Pull Up）

use super::core::PersistentHOTNode;
use super::types::{BiNode, NodeId};
use crate::bits::{pdep32, pext32};

impl PersistentHOTNode {
    // ========================================================================
    // Split 操作
    // ========================================================================

    /// 获取 root bit = 1 的 entries 掩码
    ///
    /// 返回 u32 位掩码，每一位表示对应 entry 的 root bit 是否为 1
    #[cfg(test)]
    pub(super) fn get_mask_for_larger_entries(&self) -> u32 {
        let root_mask = self.get_root_mask();
        if root_mask == 0 {
            return 0;
        }

        let mut result = 0u32;
        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & root_mask) != 0 {
                result |= 1 << i;
            }
        }
        result
    }

    /// 分裂节点
    ///
    /// 按 first_discriminative_bit 将节点分成两组：
    /// - left: root bit = 0 的 entries
    /// - right: root bit = 1 的 entries
    ///
    /// 返回 (discriminative_bit, left_node, right_node)
    ///
    /// # Panics
    ///
    /// 如果节点 span = 0（无法分裂）
    pub fn split(&self) -> (u16, PersistentHOTNode, PersistentHOTNode) {
        let disc_bit = self
            .first_discriminative_bit()
            .expect("Cannot split node with span=0");
        let root_mask = self.get_root_mask();

        // 收集两组 entries
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & root_mask) == 0 {
                left_indices.push(i);
            } else {
                right_indices.push(i);
            }
        }

        // 创建压缩后的子节点
        let left_node = self.compress_entries(&left_indices, disc_bit);
        let right_node = self.compress_entries(&right_indices, disc_bit);

        (disc_bit, left_node, right_node)
    }

    /// 压缩指定 entries 到新节点
    ///
    /// 移除 root_bit 对应的 discriminative bit，重新编码 sparse keys
    ///
    /// # Height 处理（与 C++ 一致）
    ///
    /// 新节点的 height 计算遵循 C++ `compressEntries` 的语义：
    ///
    /// - **单 entry Leaf**: `height = 1`（C++ 中 Leaf 的 getHeight()=0，节点=0+1=1）
    /// - **单 entry Internal**: `height = self.height`（保守估计，因为无法访问 store）
    /// - **多 entries 全 Leaf**: `height = 1`
    /// - **多 entries 有 Internal**: `height = self.height`（保守估计）
    ///
    /// 这是 HOT 的设计选择：C++ 的 `compressEntries` 也直接继承 `sourceNode.mHeight`，
    /// 不重新计算实际的子节点高度。这在 Parent Pull Up 场景中是正确的，因为
    /// 被 pull up 的节点的高度不会增加。
    ///
    /// # Panics
    ///
    /// 在 debug 模式下，如果 `indices` 为空会 panic。
    /// Split 后两侧必须非空是 HOT 的不变量。
    pub(super) fn compress_entries(&self, indices: &[usize], removed_bit: u16) -> PersistentHOTNode {
        debug_assert!(
            !indices.is_empty(),
            "HOT invariant violated: split should produce non-empty partitions"
        );
        if indices.is_empty() {
            return PersistentHOTNode::empty(self.height);
        }

        // 单个 entry：与 C++ 一致的 height 语义
        // C++ compressEntries 单 entry 时直接返回原 ChildPointer（不创建新节点）
        // C++ getHeight(): isLeaf() ? 0 : getNode()->mHeight
        if indices.len() == 1 {
            let idx = indices[0];
            let child = &self.children[idx];

            // Leaf 的 "height" = 0（C++ 语义），包装节点 height = 0 + 1 = 1
            // Internal 保守使用 self.height（无法访问 store 查询实际值）
            let height = match child {
                NodeId::Leaf(_) => 1,
                NodeId::Internal(_) => self.height,
            };

            let mut node = PersistentHOTNode::empty(height);
            node.children.push(*child);
            return node;
        }

        // 计算新的 extraction_masks（移除 removed_bit）
        let chunk = (removed_bit / 64) as usize;
        let bit_in_chunk = removed_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk;
        let bit_to_remove = 1u64 << u64_bit_pos;

        let mut new_masks = self.extraction_masks;
        new_masks[chunk] &= !bit_to_remove;

        // 计算 compression mask（用于 PEXT 重编码 sparse keys）
        // compression_mask = 移除 root_mask 对应位后的所有位
        let root_sparse_mask = self.get_mask_for_bit(removed_bit);
        let all_bits = self.get_all_mask_bits();
        let compression_mask = all_bits & !root_sparse_mask;

        // 计算新节点 height（与 C++ 一致）
        // - 如果所有选中的 children 都是 Leaf，height = 1
        // - 否则保守使用 self.height（无法访问 store 查询 Internal 节点的实际 height）
        let all_leaves = indices.iter().all(|&idx| self.children[idx].is_leaf());
        let height = if all_leaves { 1 } else { self.height };

        // 构建新节点
        let mut new_node = PersistentHOTNode {
            height,
            extraction_masks: new_masks,
            sparse_partial_keys: [0; 32],
            children: Vec::with_capacity(indices.len()),
        };

        for (new_idx, &old_idx) in indices.iter().enumerate() {
            // PEXT 重编码 sparse key
            let old_sparse = self.sparse_partial_keys[old_idx];
            let new_sparse = pext32(old_sparse, compression_mask);
            new_node.sparse_partial_keys[new_idx] = new_sparse;
            new_node.children.push(self.children[old_idx]);
        }

        new_node
    }

    // ========================================================================
    // Parent Pull Up
    // ========================================================================

    /// 将 BiNode 集成到当前节点（Parent Pull Up 操作）
    ///
    /// 这是 Parent Pull Up 的核心操作：将 split 后的两个子节点（BiNode）
    /// 集成到父节点中，替换原来的单个 child entry。
    ///
    /// # 参数
    ///
    /// - `old_child_index`: 原 child 在父节点中的索引（将被替换为 left）
    /// - `bi_node`: Split 产生的 BiNode，包含 left/right 子节点
    ///
    /// # 操作
    ///
    /// 1. 如果 `bi_node.discriminative_bit` 是新 bit：
    ///    - 添加到 extraction_masks
    ///    - PDEP 重编码所有现有 sparse_partial_keys
    /// 2. 替换 old_child_index 为 left（bit=0）
    /// 3. 在正确位置插入 right（bit=1），保持升序
    ///
    /// # 返回
    ///
    /// 新的 PersistentHOTNode，可能比原节点多一个 entry
    pub fn with_integrated_binode(
        &self,
        old_child_index: usize,
        bi_node: &BiNode,
    ) -> PersistentHOTNode {
        debug_assert!(old_child_index < self.len(), "old_child_index out of bounds");
        debug_assert!(
            self.len() < 32,
            "Cannot integrate BiNode into full node (would have 33 entries)"
        );

        let mut new_node = self.clone();
        let new_bit = bi_node.discriminative_bit;

        // Step 1: 检查是否需要添加新的 discriminative bit
        let bit_chunk = (new_bit / 64) as usize;
        let bit_in_chunk = new_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk; // MSB-first 转换
        let bit_mask = 1u64 << u64_bit_pos;
        let is_new_bit = (new_node.extraction_masks[bit_chunk] & bit_mask) == 0;

        // Step 2: 如果是新 bit，更新 extraction_masks 并重编码 sparse keys
        let new_bit_mask: u32 = if is_new_bit {
            // 先添加到 extraction_masks（这样 get_mask_for_bit 才能工作）
            new_node.extraction_masks[bit_chunk] |= bit_mask;

            // 获取新 bit 在 sparse key 中的 mask
            let new_bit_mask = new_node.get_mask_for_bit(new_bit);

            // 基于 mask 计算 PDEP deposit mask
            // deposit_mask 的作用：在 new_bit_mask 位置留一个 0，其余保持
            let old_all_bits = self.get_all_mask_bits();
            let low_mask = new_bit_mask - 1; // new_bit_mask 之前的位
            let high_mask = old_all_bits & !low_mask; // new_bit_mask 及之后的位
            let deposit_mask = (high_mask << 1) | low_mask;

            // 使用 PDEP 重编码所有现有 sparse keys
            for i in 0..new_node.len() {
                new_node.sparse_partial_keys[i] =
                    pdep32(new_node.sparse_partial_keys[i], deposit_mask);
            }

            new_bit_mask
        } else {
            // bit 已存在，直接获取其 mask
            new_node.get_mask_for_bit(new_bit)
        };

        // Step 3: 计算 left 和 right 的 sparse keys
        let old_sparse = new_node.sparse_partial_keys[old_child_index];
        // left: bit = 0，保持原值（PDEP 已在该位置填 0）
        let left_sparse = old_sparse;
        // right: bit = 1，设置新 bit
        let right_sparse = old_sparse | new_bit_mask;

        // Step 4: 替换 old_child_index 为 left
        new_node.sparse_partial_keys[old_child_index] = left_sparse;
        new_node.children[old_child_index] = bi_node.left;

        // Step 5: 找到 right 的插入位置（保持升序）
        let insert_pos = new_node.find_insert_position(right_sparse);

        // Step 6: 插入 right entry
        // 6a. 移动 sparse_partial_keys（固定数组，手动移动）
        let old_len = new_node.len();
        for i in (insert_pos..old_len).rev() {
            new_node.sparse_partial_keys[i + 1] = new_node.sparse_partial_keys[i];
        }
        new_node.sparse_partial_keys[insert_pos] = right_sparse;

        // 6b. 插入 child（Vec::insert 自动处理）
        new_node.children.insert(insert_pos, bi_node.right);

        // 更新 height：bi_node.height 已经是"实体化节点高度"（= 子节点高度 + 1）
        // 父节点 height = max(原 height, bi_node.height)
        // 在 Parent Pull Up 条件下（bi_node.height == parent.height），父节点高度不变
        new_node.height = std::cmp::max(new_node.height, bi_node.height);

        new_node
    }

    /// Split 满节点并同时集成 BiNode（用于 Parent Pull Up 时父节点已满的情况）
    ///
    /// 对应 C++ `HOTSingleThreaded.hpp` L520-536：当父节点满时，
    /// 先 split 父节点，然后将 BiNode 集成到正确的一侧。
    ///
    /// # 参数
    ///
    /// - `child_index`: BiNode 替换的 child 在当前节点中的索引
    /// - `bi_node`: 需要集成的 BiNode
    ///
    /// # 返回
    ///
    /// `(discriminative_bit, left_node, right_node)` - 分裂后的两个节点
    ///
    /// # 前置条件
    ///
    /// - `self.is_full()` 必须为 true
    /// - `self.span() >= 1`（有 discriminative bit 可分裂）
    pub fn split_with_binode(
        &self,
        child_index: usize,
        bi_node: &BiNode,
    ) -> (u16, PersistentHOTNode, PersistentHOTNode) {
        debug_assert!(self.is_full(), "split_with_binode requires full node");

        let disc_bit = self
            .first_discriminative_bit()
            .expect("Cannot split node with span=0");
        let root_mask = self.get_root_mask();

        // 确定 child_index 对应的 entry 在 split 后去哪一侧
        let old_sparse = self.sparse_partial_keys[child_index];
        let child_goes_right = (old_sparse & root_mask) != 0;

        // 收集两组 entries 的索引
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & root_mask) == 0 {
                left_indices.push(i);
            } else {
                right_indices.push(i);
            }
        }

        // 创建两个子节点，在目标侧集成 BiNode
        let (left_node, right_node) = if child_goes_right {
            // BiNode 在右侧
            let left_node = self.compress_entries(&left_indices, disc_bit);
            let right_node =
                self.compress_entries_with_binode(&right_indices, disc_bit, child_index, bi_node);
            (left_node, right_node)
        } else {
            // BiNode 在左侧
            let left_node =
                self.compress_entries_with_binode(&left_indices, disc_bit, child_index, bi_node);
            let right_node = self.compress_entries(&right_indices, disc_bit);
            (left_node, right_node)
        };

        (disc_bit, left_node, right_node)
    }

    /// 压缩 entries 并同时集成 BiNode
    ///
    /// 这是 `compress_entries` 的变体，在压缩的同时：
    /// 1. 将 `child_index` 对应的 entry 替换为 `bi_node.left`
    /// 2. 在正确位置插入 `bi_node.right`
    fn compress_entries_with_binode(
        &self,
        indices: &[usize],
        removed_bit: u16,
        child_index: usize,
        bi_node: &BiNode,
    ) -> PersistentHOTNode {
        debug_assert!(
            !indices.is_empty(),
            "compress_entries_with_binode requires non-empty indices"
        );

        // 找到 child_index 在 indices 中的位置
        let child_pos_in_indices = indices
            .iter()
            .position(|&i| i == child_index)
            .expect("child_index must be in indices");

        // 计算新的 extraction_masks（移除 removed_bit，添加 bi_node.discriminative_bit）
        let chunk = (removed_bit / 64) as usize;
        let bit_in_chunk = removed_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk;
        let bit_to_remove = 1u64 << u64_bit_pos;

        let mut new_masks = self.extraction_masks;
        new_masks[chunk] &= !bit_to_remove;

        // 添加 bi_node 的 discriminative_bit
        let new_bit = bi_node.discriminative_bit;
        let new_chunk = (new_bit / 64) as usize;
        let new_bit_in_chunk = new_bit % 64;
        let new_u64_bit_pos = 63 - new_bit_in_chunk;
        let bit_to_add = 1u64 << new_u64_bit_pos;
        let is_new_bit = (new_masks[new_chunk] & bit_to_add) == 0;
        if is_new_bit {
            new_masks[new_chunk] |= bit_to_add;
        }

        // 计算 compression mask（移除 removed_bit）
        let root_sparse_mask = self.get_mask_for_bit(removed_bit);
        let all_bits = self.get_all_mask_bits();
        let compression_mask = all_bits & !root_sparse_mask;

        // 计算新节点 height
        let height = std::cmp::max(self.height, bi_node.height);

        // 构建新节点
        let mut new_node = PersistentHOTNode {
            height,
            extraction_masks: new_masks,
            sparse_partial_keys: [0; 32],
            children: Vec::with_capacity(indices.len() + 1), // +1 for BiNode.right
        };

        // 获取新 bit 在 sparse key 中的 mask
        let new_bit_mask = new_node.get_mask_for_bit(new_bit);

        // 计算 PDEP deposit mask（如果添加了新 bit）
        let deposit_mask = if is_new_bit {
            let low_mask = new_bit_mask - 1;
            let high_mask = (compression_mask.count_ones() as u32)
                .checked_sub(1)
                .map(|n| (1u32 << (n + 1)) - 1)
                .unwrap_or(0)
                & !low_mask;
            (high_mask << 1) | low_mask
        } else {
            // bit 已存在，不需要重编码
            u32::MAX
        };

        let mut new_idx = 0;
        let mut right_inserted = false;

        for (pos_in_indices, &old_idx) in indices.iter().enumerate() {
            // PEXT 重编码原 sparse key
            let old_sparse = self.sparse_partial_keys[old_idx];
            let compressed_sparse = pext32(old_sparse, compression_mask);

            // 如果添加了新 bit，用 PDEP 为新 bit 留位置
            let reencoded_sparse = if is_new_bit {
                pdep32(compressed_sparse, deposit_mask)
            } else {
                compressed_sparse
            };

            if pos_in_indices == child_pos_in_indices {
                // 这是要被 BiNode 替换的 entry
                // left: bit = 0，保持原值
                let left_sparse = reencoded_sparse;
                // right: bit = 1，设置新 bit
                let right_sparse = reencoded_sparse | new_bit_mask;

                // 插入 left
                new_node.sparse_partial_keys[new_idx] = left_sparse;
                new_node.children.push(bi_node.left);
                new_idx += 1;

                // 找到 right 的插入位置（保持升序）
                // 暂时标记需要插入 right
                right_inserted = false;

                // 检查是否需要在这里插入 right
                // right_sparse 应该在后续 entries 之前还是之后？
                // 继续处理剩余 entries，在合适位置插入 right
                for &remaining_idx in &indices[pos_in_indices + 1..] {
                    let remaining_old_sparse = self.sparse_partial_keys[remaining_idx];
                    let remaining_compressed = pext32(remaining_old_sparse, compression_mask);
                    let remaining_reencoded = if is_new_bit {
                        pdep32(remaining_compressed, deposit_mask)
                    } else {
                        remaining_compressed
                    };

                    // 检查是否应该在这个 entry 之前插入 right
                    if !right_inserted && right_sparse < remaining_reencoded {
                        new_node.sparse_partial_keys[new_idx] = right_sparse;
                        new_node.children.push(bi_node.right);
                        new_idx += 1;
                        right_inserted = true;
                    }

                    new_node.sparse_partial_keys[new_idx] = remaining_reencoded;
                    new_node.children.push(self.children[remaining_idx]);
                    new_idx += 1;
                }

                // 如果 right 还没插入，放在最后
                if !right_inserted {
                    new_node.sparse_partial_keys[new_idx] = right_sparse;
                    new_node.children.push(bi_node.right);
                }

                break; // 已处理完所有 entries
            } else {
                new_node.sparse_partial_keys[new_idx] = reencoded_sparse;
                new_node.children.push(self.children[old_idx]);
                new_idx += 1;
            }
        }

        new_node
    }
}
