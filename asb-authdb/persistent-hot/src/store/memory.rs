//! 内存节点存储实现

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::error::{Result, StoreError};
use super::traits::NodeStore;
use crate::node::{LeafData, NodeId, PersistentHOTNode};

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
