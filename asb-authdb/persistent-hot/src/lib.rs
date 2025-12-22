//! PersistentHOT: Height Optimized Trie 的持久化 Rust 实现
//!
//! 本 crate 实现了 HOT（Height Optimized Trie）的持久化版本，
//! 基于 Binna et al. 2018 年 SIGMOD 论文的设计。
//!
//! # 项目背景
//!
//! 这是 Merkle-HOT (M-HOT) 项目的基础工程，最终目标是将 M-HOT
//! 加入 authenticated-storage-benchmarks 进行性能测评。
//!
//! # 与 C++ 原版的关系
//!
//! - 原版：`./hot/` 目录下的纯内存实现，使用 SIMD 优化
//! - 本版：持久化版本，底层使用 RocksDB，放弃 SIMD（I/O 时间主导）
//!
//! # 核心设计决策
//!
//! 1. **放弃 SIMD 优化**：持久化场景下，I/O 时间（1000-50000ns）远超
//!    节点内搜索时间（1-2ns），SIMD 带来的收益可忽略不计。
//!
//! 2. **使用 sparse_partial_keys 替代 present_mask**：
//!    存储每个 entry 的 sparse partial key，与 C++ 版本逻辑一致。
//!
//! 3. **Content-Addressed 存储**：节点 ID = 节点内容的哈希值，
//!    为后续 Merkle 化提供基础。
//!
//! 4. **模块化 Hash 支持**：可在 Blake3 和 Keccak256 之间切换，
//!    便于公平对比不同算法的性能。
//!
//! # 开发阶段
//!
//! - **阶段 1**（当前）：核心数据结构与序列化
//! - 阶段 2：存储抽象层（NodeStore trait）
//! - 阶段 3：Lookup 操作
//! - 阶段 4：Insert 操作（四种插入方式）
//! - 阶段 5：RocksDB 集成 + AuthDB trait 实现
//! - 阶段 6：Merkle 化
//!
//! # 使用示例
//!
//! ```rust
//! use persistent_hot::{PersistentHOTNode, ChildRef, Blake3Hasher, Hasher};
//!
//! // 创建一个简单的两叶子节点
//! let node = PersistentHOTNode::two_leaves(
//!     3, // discriminative bit position
//!     vec![0x00], b"value0".to_vec(),
//!     vec![0x10], b"value1".to_vec(),
//! );
//!
//! // 计算节点 ID（content-addressed）
//! let node_id = node.compute_node_id::<Blake3Hasher>();
//!
//! // 序列化和反序列化
//! let bytes = node.to_bytes().unwrap();
//! let decoded = PersistentHOTNode::from_bytes(&bytes).unwrap();
//! assert_eq!(node, decoded);
//!
//! // 在节点中搜索
//! let result = node.search(&[0x10]);
//! assert_eq!(result, Some(1));
//! ```
//!
//! # 参考资料
//!
//! - 论文：Binna et al. "HOT: A Height Optimized Trie Index for
//!   Main-Memory Database Systems" (SIGMOD'18)
//! - C++ 原版：`./hot/` 目录
//! - 学习指南：`./migration-walkthrough/HOT-walkthrough.md`

pub mod hash;
pub mod node;

// 重新导出常用类型
pub use hash::{Blake3Hasher, HashOutput, Hasher, Keccak256Hasher};
pub use node::{extract_bit, find_first_differing_bit, ChildRef, NodeId, PersistentHOTNode};
