//! PersistentHOT AuthDB wrapper for benchmark integration

use kvdb::KeyValueDB;
use persistent_hot::{Blake3Hasher, Keccak256Hasher, HOTTree};
use std::sync::Arc;

/// Column 分配
const COL_NODE: u32 = 0;  // 中间节点
const COL_LEAF: u32 = 1;  // 叶子节点

/// 创建使用 Blake3 哈希的 HOTTree 实例（性能更好）
pub fn new_blake3(backend: Arc<dyn KeyValueDB>) -> HOTTree<Blake3Hasher> {
    HOTTree::new(backend, COL_NODE, COL_LEAF)
}

/// 创建使用 Keccak256 哈希的 HOTTree 实例（与以太坊兼容）
pub fn new_keccak(backend: Arc<dyn KeyValueDB>) -> HOTTree<Keccak256Hasher> {
    HOTTree::new(backend, COL_NODE, COL_LEAF)
}
