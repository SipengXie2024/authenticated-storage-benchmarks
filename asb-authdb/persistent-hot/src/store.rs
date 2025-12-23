//! 节点存储抽象层
//!
//! 提供 `NodeStore` trait 用于节点和叶子的持久化存储和检索。
//! 实现：
//! - `MemoryNodeStore`: 内存存储，用于测试
//! - `KvNodeStore`: 基于 kvdb trait 的持久化存储（需要 `kvdb-backend` feature）

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(feature = "kvdb-backend")]
use kvdb::{DBTransaction, KeyValueDB};

use crate::node::{LeafData, NodeId, PersistentHOTNode};
#[cfg(test)]
use crate::node::NODE_ID_SIZE;

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
/// - `get_node`: 根据 NodeId 获取内部节点
/// - `put_node`: 存储内部节点
/// - `get_leaf`: 根据 NodeId 获取叶子数据
/// - `put_leaf`: 存储叶子数据
/// - `flush`: 刷新缓冲区到持久化存储
///
/// # Content-Addressed 存储
///
/// NodeId 是节点内容的哈希（含 version），因此：
/// - 相同内容 + 相同 version 的节点具有相同的 NodeId
/// - put_node/put_leaf 是幂等的
/// - 节点一旦写入就不会改变（不可变）
pub trait NodeStore: Send + Sync {
    /// 获取内部节点
    ///
    /// # 返回
    /// - `Ok(Some(node))`: 找到节点
    /// - `Ok(None)`: 节点不存在
    /// - `Err(_)`: 发生错误（如反序列化失败）
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>>;

    /// 存储内部节点
    ///
    /// # 注意
    /// - 调用者负责确保 `id` 是 `node` 内容的正确哈希
    /// - 由于 content-addressed 特性，重复写入相同节点是安全的
    fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()>;

    /// 获取叶子数据
    fn get_leaf(&self, id: &NodeId) -> Result<Option<LeafData>>;

    /// 存储叶子数据
    fn put_leaf(&mut self, id: &NodeId, leaf: &LeafData) -> Result<()>;

    /// 刷新缓冲区
    ///
    /// 将所有待写入的数据持久化到底层存储。
    /// 对于内存存储，此操作为空操作。
    fn flush(&mut self) -> Result<()>;

    /// 检查内部节点是否存在
    ///
    /// 默认实现通过 get_node 检查，子类可覆盖以提供更高效的实现。
    fn contains_node(&self, id: &NodeId) -> Result<bool> {
        Ok(self.get_node(id)?.is_some())
    }

    /// 检查叶子是否存在
    fn contains_leaf(&self, id: &NodeId) -> Result<bool> {
        Ok(self.get_leaf(id)?.is_some())
    }
}

// ============================================================================
// MemoryNodeStore
// ============================================================================

/// 内存节点存储
///
/// 使用 `HashMap` 存储节点和叶子，主要用于测试。
/// 使用 `RwLock` 支持并发读写。
///
/// # 线程安全
///
/// 使用 `Arc<RwLock<HashMap>>` 实现内部可变性，
/// 允许在多线程环境中安全访问。
pub struct MemoryNodeStore {
    nodes: Arc<RwLock<HashMap<NodeId, Vec<u8>>>>,
    leaves: Arc<RwLock<HashMap<NodeId, Vec<u8>>>>,
}

impl MemoryNodeStore {
    /// 创建空的内存存储
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            leaves: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 获取存储的内部节点数量
    pub fn node_count(&self) -> usize {
        self.nodes.read().unwrap().len()
    }

    /// 获取存储的叶子数量
    pub fn leaf_count(&self) -> usize {
        self.leaves.read().unwrap().len()
    }

    /// 检查存储是否为空
    pub fn is_empty(&self) -> bool {
        self.nodes.read().unwrap().is_empty() && self.leaves.read().unwrap().is_empty()
    }

    /// 清空所有数据
    pub fn clear(&mut self) {
        self.nodes.write().unwrap().clear();
        self.leaves.write().unwrap().clear();
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
            leaves: Arc::clone(&self.leaves),
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

    fn get_leaf(&self, id: &NodeId) -> Result<Option<LeafData>> {
        let leaves = self.leaves.read().unwrap();
        match leaves.get(id) {
            Some(bytes) => {
                let leaf = LeafData::from_bytes(bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(leaf))
            }
            None => Ok(None),
        }
    }

    fn put_leaf(&mut self, id: &NodeId, leaf: &LeafData) -> Result<()> {
        let bytes = leaf
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.leaves.write().unwrap().insert(*id, bytes);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // 内存存储无需刷新
        Ok(())
    }

    fn contains_node(&self, id: &NodeId) -> Result<bool> {
        Ok(self.nodes.read().unwrap().contains_key(id))
    }

    fn contains_leaf(&self, id: &NodeId) -> Result<bool> {
        Ok(self.leaves.read().unwrap().contains_key(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Blake3Hasher;
    use crate::node::ChildRef;

    fn create_test_node() -> PersistentHOTNode {
        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        node.sparse_partial_keys[2] = 2;
        node.sparse_partial_keys[3] = 3;
        node.children.push(ChildRef::Leaf([0x00u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x10u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x01u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x11u8; NODE_ID_SIZE]));
        node
    }

    fn create_test_leaf() -> LeafData {
        let mut key = [0u8; 32];
        key[0] = 0xAB;
        LeafData::new(key, b"test value".to_vec())
    }

    #[test]
    fn test_memory_store_put_and_get_node() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 存储节点
        store.put_node(&node_id, &node).unwrap();

        // 检查节点存在
        assert!(store.contains_node(&node_id).unwrap());

        // 获取节点
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_put_and_get_leaf() {
        let mut store = MemoryNodeStore::new();
        let leaf = create_test_leaf();
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        // 存储叶子
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 检查叶子存在
        assert!(store.contains_leaf(&leaf_id).unwrap());

        // 获取叶子
        let retrieved = store.get_leaf(&leaf_id).unwrap().unwrap();
        assert_eq!(retrieved, leaf);
    }

    #[test]
    fn test_memory_store_get_nonexistent() {
        let store = MemoryNodeStore::new();
        let fake_id = [0u8; NODE_ID_SIZE];

        // 不存在的节点/叶子应该返回 None
        assert!(store.get_node(&fake_id).unwrap().is_none());
        assert!(store.get_leaf(&fake_id).unwrap().is_none());
        assert!(!store.contains_node(&fake_id).unwrap());
        assert!(!store.contains_leaf(&fake_id).unwrap());
    }

    #[test]
    fn test_memory_store_idempotent_put() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 多次写入相同节点
        store.put_node(&node_id, &node).unwrap();
        store.put_node(&node_id, &node).unwrap();
        store.put_node(&node_id, &node).unwrap();

        // 应该只存储一份
        assert_eq!(store.node_count(), 1);

        // 内容应该一致
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_multiple_nodes() {
        let mut store = MemoryNodeStore::new();

        // 创建多个不同的节点
        let nodes: Vec<PersistentHOTNode> = (0..10)
            .map(|i| {
                let mut node = PersistentHOTNode::empty(2);
                node.extraction_masks = PersistentHOTNode::masks_from_bits(&[i as u16]);
                node.sparse_partial_keys[0] = 0;
                node.sparse_partial_keys[1] = 1;
                node.children.push(ChildRef::Leaf([i as u8; NODE_ID_SIZE]));
                node.children.push(ChildRef::Leaf([(i + 1) as u8; NODE_ID_SIZE]));
                node
            })
            .collect();

        // 存储所有节点
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
            store.put_node(&id, node).unwrap();
        }

        assert_eq!(store.node_count(), 10);

        // 验证所有节点都能正确检索
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
            let retrieved = store.get_node(&id).unwrap().unwrap();
            assert_eq!(&retrieved, node);
        }
    }

    #[test]
    fn test_memory_store_clear() {
        let mut store = MemoryNodeStore::new();
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);
        let leaf = create_test_leaf();
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        store.put_node(&node_id, &node).unwrap();
        store.put_leaf(&leaf_id, &leaf).unwrap();
        assert!(!store.is_empty());

        store.clear();
        assert!(store.is_empty());
        assert!(store.get_node(&node_id).unwrap().is_none());
        assert!(store.get_leaf(&leaf_id).unwrap().is_none());
    }

    #[test]
    fn test_memory_store_clone_shares_data() {
        let mut store1 = MemoryNodeStore::new();
        let store2 = store1.clone();

        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 在 store1 中写入
        store1.put_node(&node_id, &node).unwrap();

        // store2 应该也能看到
        assert!(store2.contains_node(&node_id).unwrap());
        let retrieved = store2.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_memory_store_flush() {
        let mut store = MemoryNodeStore::new();
        // flush 对内存存储应该是 no-op
        assert!(store.flush().is_ok());
    }

    #[test]
    fn test_memory_store_separate_node_and_leaf() {
        let mut store = MemoryNodeStore::new();

        // 创建一个节点和一个叶子，使用相同的 ID（模拟冲突场景）
        let node = create_test_node();
        let leaf = create_test_leaf();

        // 使用不同的 version 确保 ID 不同
        let node_id = node.compute_node_id::<Blake3Hasher>(1);
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(2);

        store.put_node(&node_id, &node).unwrap();
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 两者应该独立存储
        assert_eq!(store.node_count(), 1);
        assert_eq!(store.leaf_count(), 1);

        // 各自能正确检索
        assert!(store.get_node(&node_id).unwrap().is_some());
        assert!(store.get_leaf(&leaf_id).unwrap().is_some());

        // 交叉查询应该返回 None
        assert!(store.get_node(&leaf_id).unwrap().is_none());
        assert!(store.get_leaf(&node_id).unwrap().is_none());
    }
}

// ============================================================================
// KvNodeStore 实现
// ============================================================================

/// Key 前缀：内部节点
#[cfg(feature = "kvdb-backend")]
const KEY_PREFIX_NODE: u8 = 0x00;

/// Key 前缀：叶子数据
#[cfg(feature = "kvdb-backend")]
const KEY_PREFIX_LEAF: u8 = 0x01;

/// 基于 kvdb 的节点存储
///
/// Key 格式：`[prefix: 1B][version_id: 8B big-endian][node_id: 40B]`
/// - prefix 0x00 = 内部节点
/// - prefix 0x01 = 叶子数据
///
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
        self.version_id = version_id
    }

    /// 构造内部节点存储 key
    ///
    /// Key 格式：`[0x00][version_id: 8B][node_id: 40B]` = 49 bytes
    fn make_node_key(&self, node_id: &NodeId) -> [u8; 49] {
        let mut key = [0u8; 49];
        key[0] = KEY_PREFIX_NODE;
        key[1..9].copy_from_slice(&self.version_id.to_be_bytes());
        key[9..49].copy_from_slice(node_id);
        key
    }

    /// 构造叶子存储 key
    ///
    /// Key 格式：`[0x01][version_id: 8B][node_id: 40B]` = 49 bytes
    fn make_leaf_key(&self, node_id: &NodeId) -> [u8; 49] {
        let mut key = [0u8; 49];
        key[0] = KEY_PREFIX_LEAF;
        key[1..9].copy_from_slice(&self.version_id.to_be_bytes());
        key[9..49].copy_from_slice(node_id);
        key
    }
}

#[cfg(feature = "kvdb-backend")]
impl NodeStore for KvNodeStore {
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>> {
        let key = self.make_node_key(id);
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
        let key = self.make_node_key(id);
        let bytes = node
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut tx = DBTransaction::new();
        tx.put(self.col, &key, &bytes);
        self.db
            .write(tx)
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    fn get_leaf(&self, id: &NodeId) -> Result<Option<LeafData>> {
        let key = self.make_leaf_key(id);
        match self.db.get(self.col, &key) {
            Ok(Some(bytes)) => {
                let leaf = LeafData::from_bytes(&bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(leaf))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    fn put_leaf(&mut self, id: &NodeId, leaf: &LeafData) -> Result<()> {
        let key = self.make_leaf_key(id);
        let bytes = leaf
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

    fn contains_node(&self, id: &NodeId) -> Result<bool> {
        let key = self.make_node_key(id);
        match self.db.get(self.col, &key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    fn contains_leaf(&self, id: &NodeId) -> Result<bool> {
        let key = self.make_leaf_key(id);
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
        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        node.sparse_partial_keys[2] = 2;
        node.sparse_partial_keys[3] = 3;
        node.children.push(ChildRef::Leaf([0x00u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x10u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x01u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x11u8; NODE_ID_SIZE]));
        node
    }

    fn create_test_leaf() -> LeafData {
        let mut key = [0u8; 32];
        key[0] = 0xAB;
        LeafData::new(key, b"test value".to_vec())
    }

    #[test]
    fn test_kv_store_put_and_get_node() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 存储节点
        store.put_node(&node_id, &node).unwrap();

        // 检查节点存在
        assert!(store.contains_node(&node_id).unwrap());

        // 获取节点
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_kv_store_put_and_get_leaf() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        let leaf = create_test_leaf();
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        // 存储叶子
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 检查叶子存在
        assert!(store.contains_leaf(&leaf_id).unwrap());

        // 获取叶子
        let retrieved = store.get_leaf(&leaf_id).unwrap().unwrap();
        assert_eq!(retrieved, leaf);
    }

    #[test]
    fn test_kv_store_get_nonexistent() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let store = KvNodeStore::new(db, 0, 1);
        let fake_id = [0u8; NODE_ID_SIZE];

        // 不存在的节点/叶子应该返回 None
        assert!(store.get_node(&fake_id).unwrap().is_none());
        assert!(store.get_leaf(&fake_id).unwrap().is_none());
        assert!(!store.contains_node(&fake_id).unwrap());
        assert!(!store.contains_leaf(&fake_id).unwrap());
    }

    #[test]
    fn test_kv_store_version_isolation() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(1));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

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
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

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
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
            0x1E, 0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B,
            0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
        ];

        let node_key = store.make_node_key(&node_id);
        let leaf_key = store.make_leaf_key(&node_id);

        // 验证 key 长度为 49 字节
        assert_eq!(node_key.len(), 49);
        assert_eq!(leaf_key.len(), 49);

        // 验证前缀
        assert_eq!(node_key[0], KEY_PREFIX_NODE);
        assert_eq!(leaf_key[0], KEY_PREFIX_LEAF);

        // 验证 version_id 在 [1..9]（big-endian）
        assert_eq!(
            &node_key[1..9],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );

        // 验证 node_id 在 [9..49]
        assert_eq!(&node_key[9..49], &node_id);
        assert_eq!(&leaf_key[9..49], &node_id);
    }

    #[test]
    fn test_kv_store_multiple_nodes() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);

        // 创建多个不同的节点
        let nodes: Vec<PersistentHOTNode> = (0..10)
            .map(|i| {
                let mut node = PersistentHOTNode::empty(2);
                node.extraction_masks = PersistentHOTNode::masks_from_bits(&[i as u16]);
                node.sparse_partial_keys[0] = 0;
                node.sparse_partial_keys[1] = 1;
                node.children.push(ChildRef::Leaf([i as u8; NODE_ID_SIZE]));
                node.children.push(ChildRef::Leaf([(i + 1) as u8; NODE_ID_SIZE]));
                node
            })
            .collect();

        // 存储所有节点
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
            store.put_node(&id, node).unwrap();
        }

        // 验证所有节点都能正确检索
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
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
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

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

    #[test]
    fn test_kv_store_node_leaf_isolation() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);

        let node = create_test_node();
        let leaf = create_test_leaf();

        let node_id = node.compute_node_id::<Blake3Hasher>(1);
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        store.put_node(&node_id, &node).unwrap();
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 各自能正确检索
        assert!(store.get_node(&node_id).unwrap().is_some());
        assert!(store.get_leaf(&leaf_id).unwrap().is_some());

        // 交叉查询应该返回 None
        assert!(store.get_node(&leaf_id).unwrap().is_none());
        assert!(store.get_leaf(&node_id).unwrap().is_none());
    }
}
