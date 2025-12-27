//! PersistentHOT: Height Optimized Trie 的持久化 Rust 实现
//!
//! 本 crate 实现了 HOT（Height Optimized Trie）的持久化版本，
//! 基于 Binna et al. 2018 年 SIGMOD 论文的设计。
//!
//! # 核心数据结构（v5 设计）
//!
//! - **NodeId**: enum 区分 Leaf/Internal，40 字节 = 8 字节版本号 + 32 字节内容哈希
//! - **LeafData**: 分离存储的叶子数据（key + value）
//! - **PersistentHOTNode**: 混合布局，最大 32 个 children
//!
//! # 核心设计决策
//!
//! 1. **Content-Addressed 存储**：NodeId = version + content_hash
//! 2. **版本隔离**：不同版本的节点通过 version 前缀区分
//! 3. **叶子分离**：LeafData 独立存储，NodeId::Leaf 引用
//! 4. **混合布局**：sparse_partial_keys[32] 固定（SIMD 友好），children 紧凑 Vec
//! 5. **SIMD 搜索**：AVX2 并行比较 32 个 partial keys
//! 6. **高度对齐 C++**：Leaf 高度 = 0，只含叶子的节点高度 = 1
//!
//! # 参考资料
//!
//! - 论文：Binna et al. "HOT: A Height Optimized Trie Index for
//!   Main-Memory Database Systems" (SIGMOD'18)

pub mod bits;
pub mod hash;
pub mod node;
pub mod simd;
pub mod store;
pub mod tree;

// bits.rs 导出
pub use bits::{
    compress_partial_keys, compute_compression_mask, compute_deposit_mask, expand_partial_keys,
    pdep32, pdep64, pext32, pext64,
};

// hash.rs 导出
pub use hash::{Blake3Hasher, HashOutput, Hasher, Keccak256Hasher};

// node.rs 导出
pub use node::{
    extract_bit, find_first_differing_bit, make_raw_id, BiNode, InsertInformation,
    LeafData, NodeId, PersistentHOTNode, SearchResult, NODE_ID_SIZE,
};

// simd.rs 导出
pub use simd::{has_avx2, simd_batch_search, simd_search, simd_search_scalar, SimdSearchResult};

// store.rs 导出
pub use store::{CachedNodeStore, CacheStats, MemoryNodeStore, NodeStore, Result as StoreResult, StoreError};

// kvdb-backend feature 启用时导出 KvNodeStore
#[cfg(feature = "kvdb-backend")]
pub use store::KvNodeStore;

// tree.rs 导出
pub use tree::HOTTree;
