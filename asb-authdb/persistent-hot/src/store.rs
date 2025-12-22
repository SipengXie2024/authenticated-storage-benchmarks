//! 节点存储抽象层
//!
//! 提供 `NodeStore` trait 用于节点的持久化存储和检索。
//! 两种实现：
//! - `MemoryNodeStore`: 内存存储，用于测试
//! - `RocksDBNodeStore`: 基于 RocksDB 的持久化存储

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::node::{NodeId, PersistentHOTNode};

/// 节点存储错误类型
#[derive(Debug, Clone)]
pub enum StoreError {
    /// 序列化错误
    SerializationError(String),
    /// 反序列化错误
    DeserializationError(String),
    /// 底层存储错误
    StorageError(String),
    /// 节点不存在
    NotFound,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            StoreError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            StoreError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            StoreError::NotFound => write!(f, "Node not found"),
        }
    }
}

impl std::error::Error for StoreError {}

/// 节点存储 Result 类型
pub type Result<T> = std::result::Result<T, StoreError>;

/// 节点存储 trait
///
/// 所有节点存储实现必须满足 `Send + Sync` 以支持并发访问。
///
/// # 核心操作
///
/// - `get_node`: 根据 NodeId 获取节点
/// - `put_node`: 存储节点（由于 content-addressed，NodeId 由节点内容决定）
/// - `flush`: 刷新缓冲区到持久化存储
///
/// # Content-Addressed 存储
///
/// NodeId 是节点内容的哈希，因此：
/// - 相同内容的节点具有相同的 NodeId
/// - put_node 是幂等的
/// - 节点一旦写入就不会改变（不可变）
pub trait NodeStore: Send + Sync {
    /// 获取节点
    ///
    /// # 返回
    /// - `Ok(Some(node))`: 找到节点
    /// - `Ok(None)`: 节点不存在
    /// - `Err(_)`: 发生错误（如反序列化失败）
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>>;

    /// 存储节点
    ///
    /// # 注意
    /// - 调用者负责确保 `id` 是 `node` 内容的正确哈希
    /// - 由于 content-addressed 特性，重复写入相同节点是安全的
    fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()>;

    /// 刷新缓冲区
    ///
    /// 将所有待写入的数据持久化到底层存储。
    /// 对于内存存储，此操作为空操作。
    fn flush(&mut self) -> Result<()>;

    /// 检查节点是否存在
    ///
    /// 默认实现通过 get_node 检查，子类可覆盖以提供更高效的实现。
    fn contains(&self, id: &NodeId) -> Result<bool> {
        Ok(self.get_node(id)?.is_some())
    }
}

/// 内存节点存储
///
/// 使用 `HashMap` 存储节点，主要用于测试。
/// 使用 `RwLock` 支持并发读写。
///
/// # 线程安全
///
/// 使用 `Arc<RwLock<HashMap>>` 实现内部可变性，
/// 允许在多线程环境中安全访问。
pub struct MemoryNodeStore {
    nodes: Arc<RwLock<HashMap<NodeId, Vec<u8>>>>,
}

impl MemoryNodeStore {
    /// 创建空的内存存储
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 获取存储的节点数量
    pub fn len(&self) -> usize {
        self.nodes.read().unwrap().len()
    }

    /// 检查存储是否为空
    pub fn is_empty(&self) -> bool {
        self.nodes.read().unwrap().is_empty()
    }

    /// 清空所有节点
    pub fn clear(&mut self) {
        self.nodes.write().unwrap().clear();
    }
}

impl Default for MemoryNodeStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MemoryNodeStore {
    fn clone(&self) -> Self {
        Self {
            nodes: Arc::clone(&self.nodes),
        }
    }
}

impl NodeStore for MemoryNodeStore {
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>> {
        let nodes = self.nodes.read().unwrap();
        match nodes.get(id) {
            Some(bytes) => {
                let node = PersistentHOTNode::from_bytes(bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()> {
        let bytes = node
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.nodes.write().unwrap().insert(*id, bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // 内存存储无需刷新
        Ok(())
    }

    fn contains(&self, id: &NodeId) -> Result<bool> {
        Ok(self.nodes.read().unwrap().contains_key(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Blake3Hasher;
    use crate::node::ChildRef;

    fn create_test_node() -> PersistentHOTNode {
        PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![3, 7],
            sparse_partial_keys: vec![0, 1, 2, 3],
            children: vec![
                ChildRef::Leaf {
                    key: vec![0x00],
                    value: b"value0".to_vec(),
                },
                ChildRef::Leaf {
                    key: vec![0x10],
                    value: b"value1".to_vec(),
                },
                ChildRef::Leaf {
                    key: vec![0x01],
                    value: b"value2".to_vec(),
                },
                ChildRef::Leaf {
                    key: vec![0x11],
                    value: b"value3".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn test_memory_store_put_and_get() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 存储节点
        store.put_node(&node_id, &node).unwrap();

        // 检查节点存在
        assert!(store.contains(&node_id).unwrap());

        // 获取节点
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_get_nonexistent() {
        let store = MemoryNodeStore::new();
        let fake_id = [0u8; 32];

        // 不存在的节点应该返回 None
        assert!(store.get_node(&fake_id).unwrap().is_none());
        assert!(!store.contains(&fake_id).unwrap());
    }

    #[test]
    fn test_memory_store_idempotent_put() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 多次写入相同节点
        store.put_node(&node_id, &node).unwrap();
        store.put_node(&node_id, &node).unwrap();
        store.put_node(&node_id, &node).unwrap();

        // 应该只存储一份
        assert_eq!(store.len(), 1);

        // 内容应该一致
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_multiple_nodes() {
        let mut store = MemoryNodeStore::new();

        // 创建多个不同的节点
        let nodes: Vec<PersistentHOTNode> = (0..10)
            .map(|i| PersistentHOTNode {
                height: 2,
                discriminative_bits: vec![i as u16],
                sparse_partial_keys: vec![0, 1],
                children: vec![
                    ChildRef::Leaf {
                        key: vec![i as u8],
                        value: format!("value{}", i).into_bytes(),
                    },
                    ChildRef::Leaf {
                        key: vec![i as u8 + 1],
                        value: format!("value{}", i + 1).into_bytes(),
                    },
                ],
            })
            .collect();

        // 存储所有节点
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>();
            store.put_node(&id, node).unwrap();
        }

        assert_eq!(store.len(), 10);

        // 验证所有节点都能正确检索
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>();
            let retrieved = store.get_node(&id).unwrap().unwrap();
            assert_eq!(&retrieved, node);
        }
    }

    #[test]
    fn test_memory_store_clear() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        store.put_node(&node_id, &node).unwrap();
        assert!(!store.is_empty());

        store.clear();
        assert!(store.is_empty());
        assert!(store.get_node(&node_id).unwrap().is_none());
    }

    #[test]
    fn test_memory_store_clone_shares_data() {
        let mut store1 = MemoryNodeStore::new();
        let store2 = store1.clone();

        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 在 store1 中写入
        store1.put_node(&node_id, &node).unwrap();

        // store2 应该也能看到
        assert!(store2.contains(&node_id).unwrap());
        let retrieved = store2.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_flush() {
        let mut store = MemoryNodeStore::new();
        // flush 对内存存储应该是 no-op
        assert!(store.flush().is_ok());
    }
}
