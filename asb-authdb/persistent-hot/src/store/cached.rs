//! 带 Write-Back 缓存的节点存储
//!
//! 模仿 LVMT-DB 的 `DBAccess` 设计：
//! - get 操作：先查缓存，未命中则读取底层存储并缓存（标记为 Clean）
//! - put 操作：直接写入缓存（标记为 Dirty）
//! - flush 操作：将所有 Dirty 条目写入底层存储，然后清空缓存

use std::collections::HashMap;
use std::sync::RwLock;

use super::error::Result;
use super::traits::NodeStore;
use crate::node::{LeafData, NodeId, PersistentHOTNode};

/// 缓存条目状态
#[derive(Clone)]
enum CacheState<T: Clone> {
    /// 从存储读取的干净数据（不需要写回）
    Clean(T),
    /// 新写入的脏数据（待持久化）
    Dirty(T),
}

impl<T: Clone> CacheState<T> {
    /// 获取值的引用
    fn value(&self) -> &T {
        match self {
            CacheState::Clean(v) | CacheState::Dirty(v) => v,
        }
    }

    /// 检查是否为脏
    fn is_dirty(&self) -> bool {
        matches!(self, CacheState::Dirty(_))
    }
}

/// 缓存统计信息
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    /// 节点缓存命中次数
    pub node_hits: u64,
    /// 节点缓存未命中次数
    pub node_misses: u64,
    /// 叶子缓存命中次数
    pub leaf_hits: u64,
    /// 叶子缓存未命中次数
    pub leaf_misses: u64,
    /// flush 时写入的节点数
    pub nodes_flushed: u64,
    /// flush 时写入的叶子数
    pub leaves_flushed: u64,
}

impl CacheStats {
    /// 节点缓存命中率
    pub fn node_hit_rate(&self) -> f64 {
        let total = self.node_hits + self.node_misses;
        if total == 0 {
            0.0
        } else {
            self.node_hits as f64 / total as f64
        }
    }

    /// 叶子缓存命中率
    pub fn leaf_hit_rate(&self) -> f64 {
        let total = self.leaf_hits + self.leaf_misses;
        if total == 0 {
            0.0
        } else {
            self.leaf_hits as f64 / total as f64
        }
    }
}

/// 带 Write-Back 缓存的节点存储
///
/// 装饰器模式：包装任意 `NodeStore` 实现，添加缓存层。
///
/// # 使用示例
///
/// ```ignore
/// let kv_store = KvNodeStore::new(db, col, version);
/// let mut cached_store = CachedNodeStore::new(kv_store);
///
/// // 执行操作（写入缓存）
/// cached_store.put_node(&id, &node)?;
///
/// // 批量写入底层存储
/// cached_store.flush()?;
/// ```
///
/// # 缓存策略
///
/// - **Write-Back**: put 操作只写入缓存，flush 时批量写入底层
/// - **Clean/Dirty 状态**: 区分从存储读取的干净数据和新写入的脏数据
/// - **LVMT 风格清空**: flush 后清空所有缓存条目
pub struct CachedNodeStore<S: NodeStore> {
    /// 底层存储
    inner: S,
    /// 内部节点缓存
    node_cache: RwLock<HashMap<NodeId, CacheState<PersistentHOTNode>>>,
    /// 叶子缓存
    leaf_cache: RwLock<HashMap<NodeId, CacheState<LeafData>>>,
    /// 缓存统计
    stats: RwLock<CacheStats>,
}

impl<S: NodeStore> CachedNodeStore<S> {
    /// 创建带缓存的节点存储
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            node_cache: RwLock::new(HashMap::new()),
            leaf_cache: RwLock::new(HashMap::new()),
            stats: RwLock::new(CacheStats::default()),
        }
    }

    /// 获取缓存统计的副本
    pub fn stats(&self) -> CacheStats {
        self.stats.read().unwrap().clone()
    }

    /// 重置统计
    pub fn reset_stats(&self) {
        *self.stats.write().unwrap() = CacheStats::default();
    }

    /// 获取当前缓存的节点数
    pub fn cached_node_count(&self) -> usize {
        self.node_cache.read().unwrap().len()
    }

    /// 获取当前缓存的叶子数
    pub fn cached_leaf_count(&self) -> usize {
        self.leaf_cache.read().unwrap().len()
    }

    /// 获取底层存储的不可变引用
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// 获取底层存储的可变引用
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S: NodeStore> NodeStore for CachedNodeStore<S> {
    fn get_node(&self, id: &NodeId) -> Result<Option<PersistentHOTNode>> {
        // 1. 先查缓存
        {
            let cache = self.node_cache.read().unwrap();
            if let Some(state) = cache.get(id) {
                self.stats.write().unwrap().node_hits += 1;
                return Ok(Some(state.value().clone()));
            }
        }

        // 2. 缓存未命中，读取底层
        self.stats.write().unwrap().node_misses += 1;
        match self.inner.get_node(id)? {
            Some(node) => {
                // 缓存读取结果（干净状态）
                self.node_cache
                    .write()
                    .unwrap()
                    .insert(*id, CacheState::Clean(node.clone()));
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn put_node(&mut self, id: &NodeId, node: &PersistentHOTNode) -> Result<()> {
        // 直接放入缓存，标记为脏
        self.node_cache
            .write()
            .unwrap()
            .insert(*id, CacheState::Dirty(node.clone()));
        Ok(())
    }

    fn get_leaf(&self, id: &NodeId) -> Result<Option<LeafData>> {
        // 1. 先查缓存
        {
            let cache = self.leaf_cache.read().unwrap();
            if let Some(state) = cache.get(id) {
                self.stats.write().unwrap().leaf_hits += 1;
                return Ok(Some(state.value().clone()));
            }
        }

        // 2. 缓存未命中，读取底层
        self.stats.write().unwrap().leaf_misses += 1;
        match self.inner.get_leaf(id)? {
            Some(leaf) => {
                self.leaf_cache
                    .write()
                    .unwrap()
                    .insert(*id, CacheState::Clean(leaf.clone()));
                Ok(Some(leaf))
            }
            None => Ok(None),
        }
    }

    fn put_leaf(&mut self, id: &NodeId, leaf: &LeafData) -> Result<()> {
        self.leaf_cache
            .write()
            .unwrap()
            .insert(*id, CacheState::Dirty(leaf.clone()));
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // 1. 收集脏节点
        let dirty_nodes: Vec<_> = {
            let cache = self.node_cache.read().unwrap();
            cache
                .iter()
                .filter(|(_, state)| state.is_dirty())
                .map(|(id, state)| (*id, state.value().clone()))
                .collect()
        };

        // 2. 写入脏节点到底层存储
        for (id, node) in &dirty_nodes {
            self.inner.put_node(id, node)?;
        }

        // 3. 收集脏叶子
        let dirty_leaves: Vec<_> = {
            let cache = self.leaf_cache.read().unwrap();
            cache
                .iter()
                .filter(|(_, state)| state.is_dirty())
                .map(|(id, state)| (*id, state.value().clone()))
                .collect()
        };

        // 4. 写入脏叶子到底层存储
        for (id, leaf) in &dirty_leaves {
            self.inner.put_leaf(id, leaf)?;
        }

        // 5. 更新统计
        {
            let mut stats = self.stats.write().unwrap();
            stats.nodes_flushed += dirty_nodes.len() as u64;
            stats.leaves_flushed += dirty_leaves.len() as u64;
        }

        // 6. 清空缓存（LVMT 风格）
        self.node_cache.write().unwrap().clear();
        self.leaf_cache.write().unwrap().clear();

        // 7. 调用底层 flush
        self.inner.flush()
    }

    fn contains_node(&self, id: &NodeId) -> Result<bool> {
        // 先查缓存
        if self.node_cache.read().unwrap().contains_key(id) {
            return Ok(true);
        }
        // 再查底层
        self.inner.contains_node(id)
    }

    fn contains_leaf(&self, id: &NodeId) -> Result<bool> {
        if self.leaf_cache.read().unwrap().contains_key(id) {
            return Ok(true);
        }
        self.inner.contains_leaf(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryNodeStore;

    fn create_test_node(height: u8) -> PersistentHOTNode {
        PersistentHOTNode::empty(height)
    }

    fn create_test_leaf(key: [u8; 32], value: Vec<u8>) -> LeafData {
        LeafData { key, value }
    }

    fn create_test_node_id(prefix: u8) -> NodeId {
        let mut hash = [0u8; 40];
        hash[0] = prefix;
        NodeId::Internal(hash)
    }

    fn create_test_leaf_id(prefix: u8) -> NodeId {
        let mut hash = [0u8; 40];
        hash[0] = prefix;
        NodeId::Leaf(hash)
    }

    #[test]
    fn test_cache_hit_after_put() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let node = create_test_node(1);
        let id = create_test_node_id(1);

        // put 写入缓存
        store.put_node(&id, &node).unwrap();

        // get 应该命中缓存
        let retrieved = store.get_node(&id).unwrap();
        assert!(retrieved.is_some());

        let stats = store.stats();
        assert_eq!(stats.node_hits, 1);
        assert_eq!(stats.node_misses, 0);
    }

    #[test]
    fn test_cache_miss_reads_from_inner() {
        let mut inner = MemoryNodeStore::new();
        let node = create_test_node(2);
        let id = create_test_node_id(2);

        // 先在底层存储写入
        inner.put_node(&id, &node).unwrap();

        // 创建缓存存储
        let store = CachedNodeStore::new(inner);

        // 第一次 get：缓存未命中，读取底层
        let retrieved1 = store.get_node(&id).unwrap();
        assert!(retrieved1.is_some());

        let stats1 = store.stats();
        assert_eq!(stats1.node_hits, 0);
        assert_eq!(stats1.node_misses, 1);

        // 第二次 get：命中缓存
        let retrieved2 = store.get_node(&id).unwrap();
        assert!(retrieved2.is_some());

        let stats2 = store.stats();
        assert_eq!(stats2.node_hits, 1);
        assert_eq!(stats2.node_misses, 1);
    }

    #[test]
    fn test_flush_writes_dirty_only() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let node1 = create_test_node(1);
        let id1 = create_test_node_id(1);
        let node2 = create_test_node(2);
        let id2 = create_test_node_id(2);

        // 写入两个节点
        store.put_node(&id1, &node1).unwrap();
        store.put_node(&id2, &node2).unwrap();

        // flush
        store.flush().unwrap();

        let stats = store.stats();
        assert_eq!(stats.nodes_flushed, 2);

        // 验证底层存储包含数据
        assert!(store.inner().contains_node(&id1).unwrap());
        assert!(store.inner().contains_node(&id2).unwrap());
    }

    #[test]
    fn test_flush_clears_cache() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let node = create_test_node(3);
        let id = create_test_node_id(3);

        store.put_node(&id, &node).unwrap();
        assert_eq!(store.cached_node_count(), 1);

        store.flush().unwrap();
        assert_eq!(store.cached_node_count(), 0);
    }

    #[test]
    fn test_leaf_cache() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let key = [42u8; 32];
        let leaf = create_test_leaf(key, vec![1, 2, 3]);
        let id = create_test_leaf_id(1);

        // put 叶子
        store.put_leaf(&id, &leaf).unwrap();

        // get 命中缓存
        let retrieved = store.get_leaf(&id).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, vec![1, 2, 3]);

        let stats = store.stats();
        assert_eq!(stats.leaf_hits, 1);
        assert_eq!(stats.leaf_misses, 0);
    }

    #[test]
    fn test_contains_checks_cache() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let node = create_test_node(1);
        let id = create_test_node_id(1);

        // 未写入时不存在
        assert!(!store.contains_node(&id).unwrap());

        // 写入缓存后存在
        store.put_node(&id, &node).unwrap();
        assert!(store.contains_node(&id).unwrap());
    }

    #[test]
    fn test_stats_hit_rate() {
        let inner = MemoryNodeStore::new();
        let mut store = CachedNodeStore::new(inner);

        let node = create_test_node(1);
        let id = create_test_node_id(1);

        store.put_node(&id, &node).unwrap();

        // 3 次命中
        for _ in 0..3 {
            store.get_node(&id).unwrap();
        }

        // 1 次未命中（不存在的节点）
        let missing_id = create_test_node_id(99);
        store.get_node(&missing_id).unwrap();

        let stats = store.stats();
        assert_eq!(stats.node_hits, 3);
        assert_eq!(stats.node_misses, 1);
        assert!((stats.node_hit_rate() - 0.75).abs() < 0.001);
    }
}
