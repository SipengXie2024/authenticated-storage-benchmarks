//! Bitmask 风格操作（对齐 C++ HOT 实现）

use super::core::PersistentHOTNode;
use super::types::InsertInformation;

impl PersistentHOTNode {
    /// 返回最小的 discriminative bit index（用于 Split 分区）
    ///
    /// 对应 C++ 的 `mMostSignificantDiscriminativeBitIndex`。
    /// 注意：C++ 称之为 "most significant" 是因为它在 trie 中最先被检查，
    /// 而不是因为它在数值上最大。
    ///
    /// # 返回
    /// - `Some(bit_index)`: 最小的 key bit index
    /// - `None`: 节点没有 discriminative bits
    #[inline]
    pub fn first_discriminative_bit(&self) -> Option<u16> {
        for (chunk, &mask) in self.extraction_masks.iter().enumerate() {
            if mask != 0 {
                // mask 中最高的 u64 bit 对应最小的 key bit
                // 因为 key bit N → u64 bit (63 - N%64)
                let u64_msb = 63 - mask.leading_zeros() as u16;
                let key_bit_in_chunk = 63 - u64_msb;
                return Some((chunk as u16) * 64 + key_bit_in_chunk);
            }
        }
        None
    }

    /// 返回所有有效 bits 的 mask（连续的低位 1）
    ///
    /// 对应 C++ 的 `getAllMaskBits()`。
    /// 用于 PDEP/PEXT 的 conversion mask 计算。
    ///
    /// # 示例
    /// - span = 3 → 返回 0b111
    /// - span = 5 → 返回 0b11111
    #[inline]
    pub fn get_all_mask_bits(&self) -> u32 {
        let span = self.span();
        if span >= 32 {
            u32::MAX
        } else if span == 0 {
            0
        } else {
            (1u32 << span) - 1
        }
    }

    /// 获取某个 key bit 在 sparse key 中对应的 mask
    ///
    /// 对应 C++ 的 `getMaskFor(DiscriminativeBit)`。
    /// 返回只有一个 bit 为 1 的 mask，表示该 key bit 在 sparse key 中的位置。
    ///
    /// # 参数
    /// - `bit`: key bit index (0-255)
    ///
    /// # 返回
    /// - 如果该 bit 是 discriminative bit，返回对应的 mask
    /// - 如果该 bit 不是 discriminative bit，返回 0
    ///
    /// # 实现
    /// 构造只有目标 bit 的虚拟 key chunk，然后用 PEXT 提取。
    #[inline]
    pub fn get_mask_for_bit(&self, bit: u16) -> u32 {
        let chunk = (bit / 64) as usize;
        let bit_in_chunk = bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk; // MSB-first 转换

        if chunk >= 4 {
            return 0;
        }

        let mask = self.extraction_masks[chunk];
        let single_bit = 1u64 << u64_bit_pos;

        // 检查该 bit 是否在 mask 中
        if (mask & single_bit) == 0 {
            return 0;
        }

        // 使用 PEXT 计算该 bit 在 sparse key 中的位置
        // 先计算之前所有 chunks 贡献的 bits 数量
        let offset: u32 = self.extraction_masks[..chunk]
            .iter()
            .map(|m| m.count_ones())
            .sum();

        // 在当前 chunk 中，该 bit 之前（更低 u64 bit position）有多少个 1
        let lower_mask = single_bit - 1; // 比 single_bit 更低的所有位
        let bits_before = (mask & lower_mask).count_ones();

        1u32 << (offset + bits_before)
    }

    /// 获取 Split 分区用的 root mask
    ///
    /// 返回最小 discriminative bit 对应的 sparse key mask。
    ///
    /// **注意**：由于 PEXT 按 chunk 顺序处理，最小 key bit 不一定对应
    /// sparse key 的最高位。必须通过 `get_mask_for_bit` 计算实际位置。
    #[inline]
    pub fn get_root_mask(&self) -> u32 {
        match self.first_discriminative_bit() {
            Some(bit) => self.get_mask_for_bit(bit),
            None => 0,
        }
    }

    /// 获取插入信息（对应 C++ `getInsertInformation`）
    ///
    /// 计算新 key 插入时影响的 subtree 信息，用于判断应该采用哪种插入策略：
    /// - `isSingleEntry && isLeafEntry` → Leaf Pushdown
    /// - `isSingleEntry && !isLeafEntry` → 递归进入子节点
    /// - `!isSingleEntry` → Normal Insert（在当前节点添加新 entry）
    ///
    /// # 参数
    ///
    /// - `entry_index`: 搜索找到的 entry 索引
    /// - `discriminative_bit`: 新 key 与该 entry 的第一个不同 bit
    /// - `new_bit_value`: 新 key 在该 bit 处的值
    ///
    /// # 算法
    ///
    /// 1. 获取 entry 的 sparse key
    /// 2. 计算 prefix_bits_mask：所有 < discriminative_bit 的 bits
    /// 3. 计算 subtree_prefix_mask = entry_sparse & prefix_bits_mask
    /// 4. 找所有满足 (sparse & prefix) == subtree_prefix_mask 的 entries
    /// 5. 返回匹配的 entries 信息
    pub fn get_insert_information(
        &self,
        entry_index: usize,
        discriminative_bit: u16,
        new_bit_value: bool,
    ) -> InsertInformation {
        debug_assert!(entry_index < self.len(), "entry_index out of bounds");

        let existing_mask = self.sparse_partial_keys[entry_index];
        let prefix_bits = self.get_prefix_bits_mask(discriminative_bit);
        let subtree_prefix = existing_mask & prefix_bits;

        // 找所有满足 (sparse & prefix) == subtree_prefix 的 entries
        let mut affected_mask = 0u32;
        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & prefix_bits) == subtree_prefix {
                affected_mask |= 1 << i;
            }
        }

        debug_assert!(affected_mask != 0, "At least entry_index should match");

        InsertInformation {
            subtree_prefix_partial_key: subtree_prefix,
            first_index_in_affected_subtree: affected_mask.trailing_zeros() as usize,
            number_entries_in_affected_subtree: affected_mask.count_ones() as usize,
            discriminative_bit,
            new_bit_value,
            affected_subtree_mask: affected_mask,
        }
    }

    /// 获取 prefix bits mask（所有 < 给定 bit 的 discriminative bits 的 sparse mask）
    ///
    /// 用于 `get_insert_information`，对应 C++ `getPrefixBitsMask`。
    ///
    /// # 参数
    ///
    /// - `bit`: discriminative bit 位置
    ///
    /// # 返回
    ///
    /// 所有 key bit index < `bit` 的 discriminative bits 对应的 sparse key mask 的 OR
    pub(super) fn get_prefix_bits_mask(&self, bit: u16) -> u32 {
        let mut mask = 0u32;
        for disc_bit in self.discriminative_bits() {
            if disc_bit < bit {
                mask |= self.get_mask_for_bit(disc_bit);
            }
        }
        mask
    }

    /// 找到 sparse key 应该插入的位置（保持升序）
    ///
    /// 使用 SIMD 优化（AVX2 可用时）
    pub fn find_insert_position(&self, sparse_key: u32) -> usize {
        crate::simd::simd_find_insert_position(
            &self.sparse_partial_keys,
            sparse_key,
            self.len() as u8,
        )
    }
}
