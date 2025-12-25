//! 搜索操作

use super::core::PersistentHOTNode;
use super::types::{NodeId, SearchResult};
use crate::bits::pext64;
use crate::simd::{simd_search, SimdSearchResult};

impl PersistentHOTNode {
    // ========================================================================
    // Dense Key 提取（4×PEXT）
    // ========================================================================

    /// 从 U256 key 提取 dense partial key
    ///
    /// 使用 4 次 PEXT 操作，每次处理 64 bits
    #[inline]
    pub fn extract_dense_partial_key(&self, key: &[u8; 32]) -> u32 {
        let mut dense_key = 0u32;
        let mut bit_offset = 0u32;

        for (i, &mask) in self.extraction_masks.iter().enumerate() {
            if mask == 0 {
                continue;
            }

            // 加载对应的 8 字节（big-endian）
            let start = i * 8;
            let key_chunk = u64::from_be_bytes(key[start..start + 8].try_into().unwrap());

            // PEXT 提取这部分的 bits
            let extracted = pext64(key_chunk, mask);
            let bits_count = mask.count_ones();

            // 合并到 dense_key
            dense_key |= (extracted as u32) << bit_offset;
            bit_offset += bits_count;
        }

        dense_key
    }

    // ========================================================================
    // 搜索
    // ========================================================================

    /// 搜索匹配的 entry
    ///
    /// 使用 sparse partial key 匹配逻辑：`(dense & sparse) == sparse`
    pub fn search(&self, key: &[u8; 32]) -> SearchResult {
        let dense_key = self.extract_dense_partial_key(key);
        self.search_with_dense_key(dense_key)
    }

    /// 使用已计算的 dense key 搜索（SIMD 优化）
    #[inline]
    pub fn search_with_dense_key(&self, dense_key: u32) -> SearchResult {
        match simd_search(&self.sparse_partial_keys, dense_key, self.len() as u8) {
            SimdSearchResult::Found(index) => SearchResult::Found { index },
            SimdSearchResult::NotFound => SearchResult::NotFound { dense_key },
        }
    }

    /// 搜索并返回 child
    pub fn search_child(&self, key: &[u8; 32]) -> Option<&NodeId> {
        match self.search(key) {
            SearchResult::Found { index } => Some(&self.children[index]),
            SearchResult::NotFound { .. } => None,
        }
    }
}
