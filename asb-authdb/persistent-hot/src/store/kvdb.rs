//! 基于 kvdb 的节点存储实现

#![cfg(feature = "kvdb-backend")]

use std::sync::Arc;

use kvdb::{DBTransaction, KeyValueDB};

use super::error::{Result, StoreError};
use crate::node::{LeafData, NodeId, PersistentHOTNode};

/// 基于 kvdb 的节点存储
///
/// 使用双 column 分离存储：
/// - `col_node`: 存储中间节点 (Internal nodes)
/// - `col_leaf`: 存储叶子节点 (Leaf nodes)
///
/// Key 格式：直接使用 NodeId 的 40 字节（version 8B + content_hash 32B）
///
/// # 示例
///
/// ```ignore
/// use kvdb_memorydb;
/// use persistent_hot::KvNodeStore;
///
/// let db = Arc::new(kvdb_memorydb::create(2));  // 需要 2 个 column
/// let mut store = KvNodeStore::new(db, 0, 1, 1);  // col_node=0, col_leaf=1, version=1
/// ```
pub struct KvNodeStore {
    db: Arc<dyn KeyValueDB>,
    col_node: u32,
    col_leaf: u32,
    version_id: u64,
}

impl KvNodeStore {
    /// 创建新的 KvNodeStore
    ///
    /// # 参数
    /// - `db`: kvdb 后端（RocksDB、MDBX、内存等）
    /// - `col_node`: 存储中间节点的 column family
    /// - `col_leaf`: 存储叶子节点的 column family
    /// - `version_id`: 版本标识（仅用于 HOTTree 内部追踪）
    pub fn new(db: Arc<dyn KeyValueDB>, col_node: u32, col_leaf: u32, version_id: u64) -> Self {
        Self {
            db,
            col_node,
            col_leaf,
            version_id,
        }
    }

    /// 获取当前版本 ID
    pub fn version_id(&self) -> u64 {
        self.version_id
    }

    /// 设置版本 ID（用于版本切换）
    pub fn set_version_id(&mut self, version_id: u64) {
        self.version_id = version_id
    }
}

impl KvNodeStore {
    /// 获取内部节点
    pub fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>> {
        match self.db.get(self.col_node, id.raw_bytes()) {
            Ok(Some(bytes)) => {
                let node = PersistentHOTNode::from_bytes(&bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(node))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    /// 存储内部节点
    pub fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()> {
        let bytes = node
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut tx = DBTransaction::new();
        tx.put(self.col_node, id.raw_bytes(), &bytes);
        self.db
            .write(tx)
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    /// 获取叶子数据
    pub fn get_leaf(&self, id: &NodeId) -> Result<Option<LeafData>> {
        match self.db.get(self.col_leaf, id.raw_bytes()) {
            Ok(Some(bytes)) => {
                let leaf = LeafData::from_bytes(&bytes)
                    .map_err(|e| StoreError::DeserializationError(e.to_string()))?;
                Ok(Some(leaf))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    /// 存储叶子数据
    pub fn put_leaf(&mut self, id: &NodeId, leaf: &LeafData) -> Result<()> {
        let bytes = leaf
            .to_bytes()
            .map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let mut tx = DBTransaction::new();
        tx.put(self.col_leaf, id.raw_bytes(), &bytes);
        self.db
            .write(tx)
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    /// 刷新缓冲区到持久化存储
    pub fn flush(&mut self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| StoreError::StorageError(e.to_string()))
    }

    /// 检查内部节点是否存在
    pub fn contains_node(&self, id: &NodeId) -> Result<bool> {
        match self.db.get(self.col_node, id.raw_bytes()) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }

    /// 检查叶子是否存在
    pub fn contains_leaf(&self, id: &NodeId) -> Result<bool> {
        match self.db.get(self.col_leaf, id.raw_bytes()) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(StoreError::StorageError(e.to_string())),
        }
    }
}
