//! 节点存储抽象层
//!
//! 提供 `NodeStore` trait 用于节点的持久化存储和检索。
//! 实现：
//! - `MemoryNodeStore`: 内存存储，用于测试
//! - `KvNodeStore`: 基于 kvdb trait 的持久化存储（需要 `kvdb-backend` feature）

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(feature = "kvdb-backend")]
use kvdb::{DBTransaction, KeyValueDB};

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

// ============================================================================
// KvNodeStore 实现
// ============================================================================

/// 基于 kvdb 的节点存储
///
/// Key 格式：version_id (8 bytes, big-endian) | node_id (32 bytes)
/// 支持多版本存储和垃圾回收
///
/// # 示例
///
/// ```ignore
/// use kvdb_memorydb;
/// use persistent_hot::KvNodeStore;
///
/// let db = Arc::new(kvdb_memorydb::create(1));
/// let mut store = KvNodeStore::new(db, 0, 1);
/// ```
#[cfg(feature = "kvdb-backend")]
pub struct KvNodeStore {
    db: Arc<dyn KeyValueDB>,
    col: u32,
    version_id: u64,
}

#[cfg(feature = "kvdb-backend")]
impl KvNodeStore {
    /// 创建新的 KvNodeStore
    ///
    /// # 参数
    /// - `db`: kvdb 后端（RocksDB、MDBX、内存等）
    /// - `col`: 使用的 column family
    /// - `version_id`: 版本标识，用于多版本支持
    pub fn new(db: Arc<dyn KeyValueDB>, col: u32, version_id: u64) -> Self {
        Self { db, col, version_id }
    }

    /// 获取当前版本 ID
    pub fn version_id(&self) -> u64 {
        self.version_id
    }

    /// 设置版本 ID（用于版本切换）
    pub fn set_version_id(&mut self, version_id: u64) {
        self.version_id = version_id;
    }

    /// 构造存储 key
    ///
    /// Key 格式：version_id (8 bytes, big-endian) | node_id (32 bytes)
    fn make_key(&self, node_id: &NodeId) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&self.version_id.to_be_bytes());
        key[8..40].copy_from_slice(node_id);
        key
    }
}

#[cfg(feature = "kvdb-backend")]
impl NodeStore for KvNodeStore {
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>> {
        let key = self.make_key(id);
        match self.db.get(self.col, &key) {
            Ok(Some(bytes)) => {
                let node = PersistentHOTNode::from_bytes(&bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(node))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()> {
        let key = self.make_key(id);
        let bytes = node
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut tx = DBTransaction::new();
        tx.put(self.col, &key, &bytes);
        self.db
            .write(tx)
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    fn flush(&mut self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    fn contains(&self, id: &NodeId) -> Result<bool> {
        let key = self.make_key(id);
        match self.db.get(self.col, &key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }
}

// ============================================================================
// KvNodeStore 测试
// ============================================================================

#[cfg(all(test, feature = "kvdb-backend"))]
mod kv_tests {
    use super::*;
    use crate::hash::Blake3Hasher;
    use crate::node::ChildRef;
    use kvdb::KeyValueDB;

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
    fn test_kv_store_put_and_get() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
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
    fn test_kv_store_get_nonexistent() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let store = KvNodeStore::new(db, 0, 1);
        let fake_id = [0u8; 32];

        // 不存在的节点应该返回 None
        assert!(store.get_node(&fake_id).unwrap().is_none());
        assert!(!store.contains(&fake_id).unwrap());
    }

    #[test]
    fn test_kv_store_version_isolation() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(1));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 在版本 1 存储节点
        let mut store_v1 = KvNodeStore::new(Arc::clone(&db), 0, 1);
        store_v1.put_node(&node_id, &node).unwrap();

        // 版本 2 看不到版本 1 的数据
        let store_v2 = KvNodeStore::new(Arc::clone(&db), 0, 2);
        assert!(store_v2.get_node(&node_id).unwrap().is_none());

        // 版本 1 仍然可以看到数据
        assert!(store_v1.get_node(&node_id).unwrap().is_some());
    }

    #[test]
    fn test_kv_store_version_switch() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(1));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 在版本 1 存储节点
        let mut store = KvNodeStore::new(Arc::clone(&db), 0, 1);
        store.put_node(&node_id, &node).unwrap();

        // 切换到版本 2，看不到数据
        store.set_version_id(2);
        assert!(store.get_node(&node_id).unwrap().is_none());

        // 切回版本 1，可以看到数据
        store.set_version_id(1);
        assert!(store.get_node(&node_id).unwrap().is_some());
    }

    #[test]
    fn test_kv_store_key_format() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let store = KvNodeStore::new(db, 0, 0x0102030405060708);
        let node_id: NodeId = [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F,
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
            0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F,
        ];

        let key = store.make_key(&node_id);

        // 验证 key 长度为 40 字节
        assert_eq!(key.len(), 40);

        // 验证 version_id 在前 8 字节（big-endian）
        assert_eq!(&key[0..8], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

        // 验证 node_id 在后 32 字节
        assert_eq!(&key[8..40], &node_id);
    }

    #[test]
    fn test_kv_store_multiple_nodes() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);

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

        // 验证所有节点都能正确检索
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>();
            let retrieved = store.get_node(&id).unwrap().unwrap();
            assert_eq!(&retrieved, node);
        }
    }

    #[test]
    fn test_kv_store_flush() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        // flush 应该成功
        assert!(store.flush().is_ok());
    }

    #[test]
    fn test_kv_store_shared_db() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(2));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>();

        // 两个 store 使用不同的 column
        let mut store_col0 = KvNodeStore::new(Arc::clone(&db), 0, 1);
        let store_col1 = KvNodeStore::new(Arc::clone(&db), 1, 1);

        // 在 column 0 存储节点
        store_col0.put_node(&node_id, &node).unwrap();

        // column 1 看不到 column 0 的数据
        assert!(store_col1.get_node(&node_id).unwrap().is_none());

        // column 0 可以看到数据
        assert!(store_col0.get_node(&node_id).unwrap().is_some());
    }
}
