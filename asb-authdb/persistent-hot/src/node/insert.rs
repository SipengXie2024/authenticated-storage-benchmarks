//! 插入操作（Copy-on-Write）

use super::core::PersistentHOTNode;
use super::types::{InsertInformation, NodeId};
use crate::bits::pdep32;

impl PersistentHOTNode {
    /// Normal Insert: 添加新 entry，返回新节点
    ///
    /// 遵循 Copy-on-Write 原则：不修改 self，返回新节点。
    ///
    /// # 参数
    /// - `new_bit`: 新的 discriminative bit 位置（0-255）
    /// - `new_bit_value`: 新 key 在该 bit 位置的值（true=1, false=0）
    /// - `affected_index`: 受影响的 entry index（与新 key 共享前缀）
    /// - `child`: 新的 NodeId（叶子或内部节点）
    ///
    /// # Panics
    /// - 如果节点已满（debug 模式）
    pub fn with_new_entry(
        &self,
        new_bit: u16,
        new_bit_value: bool,
        affected_index: usize,
        child: NodeId,
    ) -> Self {
        debug_assert!(!self.is_full(), "Cannot add entry to full node");
        debug_assert!(affected_index < self.len(), "affected_index out of bounds");

        // 创建副本
        let mut new_node = self.clone();

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

            // 基于 mask 计算 PDEP deposit mask（替代 compute_deposit_mask）
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

        // Step 3: 计算新 entry 的 sparse partial key
        let affected_sparse = new_node.sparse_partial_keys[affected_index];
        let new_sparse_key = if new_bit_value {
            affected_sparse | new_bit_mask
        } else {
            affected_sparse & !new_bit_mask
        };

        // Step 4: 如果是新 bit，更新 affected entry 的 sparse key
        if is_new_bit && !new_bit_value {
            // 新 key 的 bit=0，则 affected entry 的 bit=1
            new_node.sparse_partial_keys[affected_index] |= new_bit_mask;
        }
        // 注：如果 new_bit_value=true，affected entry 保持 bit=0（PDEP 已填充 0）

        // Step 5: 找到插入位置（保持 sparse keys 升序）
        let insert_pos = new_node.find_insert_position(new_sparse_key);

        // Step 6: 插入新 entry
        // 6a. 移动 sparse_partial_keys（固定数组，手动移动）
        let old_len = new_node.len();
        for i in (insert_pos..old_len).rev() {
            new_node.sparse_partial_keys[i + 1] = new_node.sparse_partial_keys[i];
        }
        new_node.sparse_partial_keys[insert_pos] = new_sparse_key;

        // 6b. 插入 child（Vec::insert 自动处理）
        new_node.children.insert(insert_pos, child);

        new_node
    }

    /// 使用 InsertInformation 添加新 entry（用于 Normal Insert）
    ///
    /// 与 `with_new_entry` 不同，此方法更新 affected subtree 中**所有** entries 的 sparse key，
    /// 而不仅仅是 first_index_in_affected_subtree。这是 Normal Insert（isSingleEntry==false）
    /// 的正确行为。
    ///
    /// # 参数
    ///
    /// - `info`: InsertInformation，包含 affected_subtree_mask
    /// - `child`: 新 entry 的 NodeId（叶子或内部节点）
    pub fn with_new_entry_from_info(&self, info: &InsertInformation, child: NodeId) -> Self {
        debug_assert!(!self.is_full(), "Cannot add entry to full node");

        let mut new_node = self.clone();

        // Step 1: 检查是否需要添加新的 discriminative bit
        let new_bit = info.discriminative_bit;
        let bit_chunk = (new_bit / 64) as usize;
        let bit_in_chunk = new_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk; // MSB-first 转换
        let bit_mask = 1u64 << u64_bit_pos;
        let is_new_bit = (new_node.extraction_masks[bit_chunk] & bit_mask) == 0;

        // Step 2: 如果是新 bit，更新 extraction_masks 并重编码 sparse keys
        let mut deposit_mask: Option<u32> = None;
        let new_bit_mask: u32 = if is_new_bit {
            new_node.extraction_masks[bit_chunk] |= bit_mask;
            let new_bit_mask = new_node.get_mask_for_bit(new_bit);

            // 计算 PDEP deposit mask
            let old_all_bits = self.get_all_mask_bits();
            let low_mask = new_bit_mask - 1;
            let high_mask = old_all_bits & !low_mask;
            let deposit_mask_value = (high_mask << 1) | low_mask;
            deposit_mask = Some(deposit_mask_value);

            // 使用 PDEP 重编码所有现有 sparse keys
            for i in 0..new_node.len() {
                new_node.sparse_partial_keys[i] =
                    pdep32(new_node.sparse_partial_keys[i], deposit_mask_value);
            }

            new_bit_mask
        } else {
            new_node.get_mask_for_bit(new_bit)
        };

        // Step 3: 更新 affected subtree 中**所有** entries 的 sparse key
        // 这是与 with_new_entry 的关键区别！
        let affected_mask = info.affected_subtree_mask;
        for i in 0..new_node.len() {
            if (affected_mask & (1 << i)) != 0 {
                // 这个 entry 属于 affected subtree
                // 设置其新 bit 为 !new_bit_value（与新 key 相反）
                if info.new_bit_value {
                    // 新 key 的 bit=1，affected entries 的 bit=0（保持，因为 PDEP 填充了 0）
                    // 什么都不做
                } else {
                    // 新 key 的 bit=0，affected entries 的 bit=1
                    new_node.sparse_partial_keys[i] |= new_bit_mask;
                }
            }
        }

        // Step 4: 计算新 entry 的 sparse partial key
        // 基于 subtree_prefix + new_bit_value（对齐 C++ addEntry）
        let base_prefix = match deposit_mask {
            Some(mask) => pdep32(info.subtree_prefix_partial_key, mask),
            None => info.subtree_prefix_partial_key,
        };
        let new_sparse_key = if info.new_bit_value {
            base_prefix | new_bit_mask
        } else {
            base_prefix & !new_bit_mask
        };

        // Step 5: 计算插入位置（affected subtree 边界）
        let insert_pos = info.first_index_in_affected_subtree
            + if info.new_bit_value {
                info.number_entries_in_affected_subtree
            } else {
                0
            };

        // Step 6: 插入新 entry
        let old_len = new_node.len();
        for i in (insert_pos..old_len).rev() {
            new_node.sparse_partial_keys[i + 1] = new_node.sparse_partial_keys[i];
        }
        new_node.sparse_partial_keys[insert_pos] = new_sparse_key;
        new_node.children.insert(insert_pos, child);

        new_node
    }
}
