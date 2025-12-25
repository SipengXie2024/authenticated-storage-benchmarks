//! 基于 kvdb 的节点存储实现

#![cfg(feature = "kvdb-backend")]

use std::sync::Arc;

use kvdb::{DBTransaction, KeyValueDB};

use super::error::{Result, StoreError};
use super::traits::NodeStore;
use crate::node::{LeafData, NodeId, PersistentHOTNode};

/// Key 前缀：内部节点
const KEY_PREFIX_NODE: u8 = 0x00;

/// Key 前缀：叶子数据
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
pub struct KvNodeStore {
    db: Arc<dyn KeyValueDB>,
    col: u32,
    version_id: u64,
}

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
        debug_assert!(
            node_id.is_internal(),
            "make_node_key requires NodeId::Internal"
        );
        let mut key = [0u8; 49];
        key[0] = KEY_PREFIX_NODE;
        key[1..9].copy_from_slice(&self.version_id.to_be_bytes());
        key[9..49].copy_from_slice(node_id.raw_bytes());
        key
    }

    /// 构造叶子存储 key
    ///
    /// Key 格式：`[0x01][version_id: 8B][node_id: 40B]` = 49 bytes
    fn make_leaf_key(&self, node_id: &NodeId) -> [u8; 49] {
        debug_assert!(
            node_id.is_leaf(),
            "make_leaf_key requires NodeId::Leaf"
        );
        let mut key = [0u8; 49];
        key[0] = KEY_PREFIX_LEAF;
        key[1..9].copy_from_slice(&self.version_id.to_be_bytes());
        key[9..49].copy_from_slice(node_id.raw_bytes());
        key
    }
}

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
