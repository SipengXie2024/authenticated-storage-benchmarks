//! HOTTree: Height Optimized Trie 的树级操作
//!
//! 提供 tree-level 的 lookup/insert/delete 操作，
//! 基于 `PersistentHOTNode` 节点和 `NodeStore` 存储抽象。

use std::marker::PhantomData;

use crate::hash::{Blake3Hasher, Hasher};
use crate::node::{
    find_first_differing_bit, BiNode, ChildRef, LeafData, NodeId, PersistentHOTNode, SearchResult,
};
use crate::store::{NodeStore, Result, StoreError};

/// Height Optimized Trie
///
/// # 类型参数
///
/// - `S`: 节点存储实现，必须实现 `NodeStore` trait
/// - `H`: 哈希算法，默认 Blake3
///
/// # 版本管理
///
/// `version` 不存储在结构中，而是作为 insert 参数传入，
pub struct HOTTree<S: NodeStore, H: Hasher = Blake3Hasher> {
    store: S,
    root_id: Option<NodeId>,
    _marker: PhantomData<H>,
}

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 创建空树
    pub fn new(store: S) -> Self {
        Self {
            store,
            root_id: None,
            _marker: PhantomData,
        }
    }

    /// 创建带有根节点的树
    pub fn with_root(store: S, root_id: NodeId) -> Self {
        Self {
            store,
            root_id: Some(root_id),
            _marker: PhantomData,
        }
    }

    /// 获取根节点 ID
    #[inline]
    pub fn root_id(&self) -> Option<&NodeId> {
        self.root_id.as_ref()
    }

    /// 获取存储引用
    #[inline]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// 获取可变存储引用
    #[inline]
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// 查找指定版本下 key 对应的值
    ///
    /// # 参数
    ///
    /// - `key`: 32 字节的 key
    ///
    /// # 返回
    ///
    /// - `Ok(Some(value))`: 找到匹配的 key，返回 value
    /// - `Ok(None)`: key 不存在或假阳性（partial key 匹配但完整 key 不匹配）
    /// - `Err(_)`: 存储错误
    pub fn lookup(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let root_id = match &self.root_id {
            Some(id) => id,
            None => return Ok(None),
        };
        self.lookup_internal(root_id, key)
    }

    /// 内部递归查找
    fn lookup_internal(&self, node_id: &NodeId, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let node = self
            .store
            .get_node(node_id)?
            .ok_or(StoreError::NotFound)?;

        match node.search(key) {
            SearchResult::Found { index } => {
                match &node.children[index] {
                    ChildRef::Internal(child_id) => {
                        // 递归搜索子节点
                        self.lookup_internal(child_id, key)
                    }
                    ChildRef::Leaf(leaf_id) => {
                        // 获取叶子数据，验证 key 完全匹配
                        let leaf = self
                            .store
                            .get_leaf(leaf_id)?
                            .ok_or(StoreError::NotFound)?;
                        if &leaf.key == key {
                            Ok(Some(leaf.value.clone()))
                        } else {
                            Ok(None) // Key 不匹配（假阳性）
                        }
                    }
                }
            }
            SearchResult::NotFound { .. } => Ok(None),
        }
    }

    /// 检查树是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root_id.is_none()
    }

    // ========================================================================
    // Insert 操作
    // ========================================================================

    /// 插入 key-value 对
    ///
    /// # 参数
    ///
    /// - `key`: 32 字节的 key
    /// - `value`: 任意长度的 value
    /// - `version`: 版本号（用于生成 NodeId）
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 插入成功
    /// - `Err(_)`: 存储错误
    pub fn insert(&mut self, key: &[u8; 32], value: Vec<u8>, version: u64) -> Result<()> {
        // 创建并存储叶子
        let leaf = LeafData {
            key: *key,
            value,
        };
        let leaf_id = leaf.compute_node_id::<H>(version);
        self.store.put_leaf(&leaf_id, &leaf)?;

        match &self.root_id {
            None => {
                // 空树：创建单叶子节点作为根
                let node = PersistentHOTNode::single_leaf(leaf_id);
                let node_id = node.compute_node_id::<H>(version);
                self.store.put_node(&node_id, &node)?;
                self.root_id = Some(node_id);
                Ok(())
            }
            Some(root_id) => {
                // 非空树：递归插入
                let new_root_id = self.insert_internal(root_id.clone(), key, leaf_id, version)?;
                self.root_id = Some(new_root_id);
                Ok(())
            }
        }
    }

    /// 递归插入到节点
    ///
    /// 返回新的节点 ID（可能是修改后的当前节点，或新创建的父节点）
    fn insert_internal(
        &mut self,
        node_id: NodeId,
        key: &[u8; 32],
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        let node = self
            .store
            .get_node(&node_id)?
            .ok_or(StoreError::NotFound)?;

        match node.search(key) {
            SearchResult::Found { index } => {
                // 找到匹配的 entry
                match &node.children[index] {
                    ChildRef::Internal(child_id) => {
                        // 递归插入到子节点
                        let new_child_id =
                            self.insert_internal(child_id.clone(), key, leaf_id, version)?;

                        // 更新当前节点的 child 引用
                        let mut new_node = node.clone();
                        new_node.children[index] = ChildRef::Internal(new_child_id);
                        let new_node_id = new_node.compute_node_id::<H>(version);
                        self.store.put_node(&new_node_id, &new_node)?;
                        Ok(new_node_id)
                    }
                    ChildRef::Leaf(existing_leaf_id) => {
                        // 叶子节点：需要处理碰撞
                        let existing_leaf = self
                            .store
                            .get_leaf(existing_leaf_id)?
                            .ok_or(StoreError::NotFound)?;

                        if &existing_leaf.key == key {
                            // 相同 key：替换值（更新 leaf）
                            let mut new_node = node.clone();
                            new_node.children[index] = ChildRef::Leaf(leaf_id);
                            let new_node_id = new_node.compute_node_id::<H>(version);
                            self.store.put_node(&new_node_id, &new_node)?;
                            Ok(new_node_id)
                        } else {
                            // 不同 key：Leaf Node Pushdown
                            self.leaf_pushdown(
                                &node,
                                index,
                                &existing_leaf.key,
                                existing_leaf_id.clone(),
                                key,
                                leaf_id,
                                version,
                            )
                        }
                    }
                }
            }
            SearchResult::NotFound { dense_key } => {
                // 没有匹配的 entry：需要添加新 entry
                self.add_entry_to_node(&node, key, dense_key, leaf_id, version)
            }
        }
    }

    /// Leaf Node Pushdown: 创建新节点容纳两个叶子
    fn leaf_pushdown(
        &mut self,
        parent_node: &PersistentHOTNode,
        affected_index: usize,
        existing_key: &[u8; 32],
        existing_leaf_id: NodeId,
        new_key: &[u8; 32],
        new_leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 创建包含两个叶子的新节点（two_leaves 内部会计算 diff_bit）
        let new_child = PersistentHOTNode::two_leaves(
            existing_key,
            existing_leaf_id,
            new_key,
            new_leaf_id,
        );
        let new_child_id = new_child.compute_node_id::<H>(version);
        self.store.put_node(&new_child_id, &new_child)?;

        // 更新父节点：将叶子替换为内部节点
        let mut new_parent = parent_node.clone();
        new_parent.children[affected_index] = ChildRef::Internal(new_child_id);

        // 更新父节点高度：h(parent) = max(h(children)) + 1
        // 新子节点 height = 1，如果父节点原来只有叶子（height=1），需要更新为 2
        new_parent.height = std::cmp::max(parent_node.height, new_child.height + 1);

        let new_parent_id = new_parent.compute_node_id::<H>(version);
        self.store.put_node(&new_parent_id, &new_parent)?;

        Ok(new_parent_id)
    }

    /// 向节点添加新 entry
    fn add_entry_to_node(
        &mut self,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        dense_key: u32,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 检查节点是否已满
        if node.len() >= 32 {
            // 节点溢出：需要 Split
            return self.handle_overflow(node, key, dense_key, leaf_id, version);
        }

        // 找到 affected entry（用于确定 diff bit）
        // 使用 sparse matching 找到最后一个匹配的 entry
        let affected_index = self.find_affected_entry(node, dense_key)
            .expect("HOT invariant violated: no matching entry found for sparse matching");
        let affected_child = &node.children[affected_index];

        // 获取 affected entry 的 key
        let affected_key = self.get_entry_key(affected_child)?;

        // 找到 diff bit
        let diff_bit = find_first_differing_bit(&affected_key, key)
            .expect("Keys must be different");
        let new_bit_value = crate::node::extract_bit(key, diff_bit);

        // 使用 with_new_entry 创建新节点
        let new_node = node.with_new_entry(
            diff_bit,
            new_bit_value,
            affected_index,
            ChildRef::Leaf(leaf_id),
        );

        let new_node_id = new_node.compute_node_id::<H>(version);
        self.store.put_node(&new_node_id, &new_node)?;
        Ok(new_node_id)
    }

    /// 处理节点溢出（Split）
    ///
    /// 优化：先在目标子节点完成插入，再持久化，避免多余的写入
    fn handle_overflow(
        &mut self,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        _dense_key: u32,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // Split 节点
        let (disc_bit, left_node, right_node) = node.split();

        // 确定新 entry 应该插入哪个子节点
        let new_bit_value = crate::node::extract_bit(key, disc_bit);

        // 分离目标节点和另一侧节点
        let (target_node, other_node) = if new_bit_value {
            (right_node, left_node)
        } else {
            (left_node, right_node)
        };

        // 先持久化另一侧节点（它不会被修改）
        let other_id = other_node.compute_node_id::<H>(version);
        let other_height = other_node.height;
        self.store.put_node(&other_id, &other_node)?;

        // 在目标子节点完成插入（只持久化一次最终版本）
        let target_id = target_node.compute_node_id::<H>(version);
        let new_target_id = self.insert_into_split_child(&target_node, target_id, key, leaf_id, version)?;

        // 获取更新后的目标子节点高度
        let new_target_node = self.store.get_node(&new_target_id)?
            .expect("Just inserted node should exist");
        let new_target_height = new_target_node.height;

        // 创建包含两个子节点的父节点
        let (final_left_id, final_right_id) = if new_bit_value {
            (other_id.clone(), new_target_id)
        } else {
            (new_target_id, other_id.clone())
        };

        // BiNode.height 应该是两个子节点高度的最大值
        let max_child_height = std::cmp::max(new_target_height, other_height);

        let bi_node = BiNode {
            discriminative_bit: disc_bit,
            left: final_left_id,
            right: final_right_id,
            height: max_child_height,
        };

        let parent_node = bi_node.to_two_entry_node();
        let parent_id = parent_node.compute_node_id::<H>(version);
        self.store.put_node(&parent_id, &parent_node)?;

        Ok(parent_id)
    }

    /// 插入到 split 后的子节点
    fn insert_into_split_child(
        &mut self,
        node: &PersistentHOTNode,
        node_id: NodeId,
        key: &[u8; 32],
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 如果子节点为空或只有一个 entry，特殊处理
        if node.len() == 0 {
            // 创建单叶子节点
            let new_node = PersistentHOTNode::single_leaf(leaf_id);
            let new_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_id, &new_node)?;
            return Ok(new_id);
        }

        if node.len() == 1 {
            // 只有一个 entry，需要获取其 key 来计算 diff bit
            let existing_key = self.get_entry_key(&node.children[0])?;
            if &existing_key == key {
                // 相同 key，替换
                let mut new_node = node.clone();
                new_node.children[0] = ChildRef::Leaf(leaf_id);
                let new_id = new_node.compute_node_id::<H>(version);
                self.store.put_node(&new_id, &new_node)?;
                return Ok(new_id);
            }

            // 不同 key，创建两叶子节点
            // 获取现有 entry 的 leaf_id
            let existing_leaf_id = match &node.children[0] {
                ChildRef::Leaf(id) => id.clone(),
                ChildRef::Internal(_) => {
                    // 如果是内部节点，需要特殊处理
                    // 这里简化处理，直接使用 insert_internal
                    return self.insert_internal(node_id, key, leaf_id, version);
                }
            };

            let new_node = PersistentHOTNode::two_leaves(
                &existing_key,
                existing_leaf_id,
                key,
                leaf_id,
            );
            let new_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_id, &new_node)?;
            return Ok(new_id);
        }

        // 正常情况：使用 insert_internal
        self.insert_internal(node_id, key, leaf_id, version)
    }

    /// 找到 affected entry 索引
    ///
    /// 使用 sparse matching 找到最后一个 (dense & sparse) == sparse 的 entry。
    /// 按照 HOT 设计，应该总是能找到匹配（至少 sparse=0 总是匹配）。
    /// 返回 None 表示数据结构不一致。
    fn find_affected_entry(&self, node: &PersistentHOTNode, dense_key: u32) -> Option<usize> {
        // 使用 sparse matching 找到最后一个 (dense & sparse) == sparse 的 entry
        for i in (0..node.len()).rev() {
            let sparse = node.sparse_partial_keys[i];
            if (dense_key & sparse) == sparse {
                return Some(i);
            }
        }
        None // 数据结构不一致
    }

    /// 获取 entry 对应的 key
    fn get_entry_key(&self, child: &ChildRef) -> Result<[u8; 32]> {
        match child {
            ChildRef::Leaf(leaf_id) => {
                let leaf = self
                    .store
                    .get_leaf(leaf_id)?
                    .ok_or(StoreError::NotFound)?;
                Ok(leaf.key)
            }
            ChildRef::Internal(node_id) => {
                // 对于内部节点，递归获取第一个叶子的 key
                let node = self
                    .store
                    .get_node(node_id)?
                    .ok_or(StoreError::NotFound)?;
                if node.len() > 0 {
                    self.get_entry_key(&node.children[0])
                } else {
                    Err(StoreError::NotFound)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Blake3Hasher;
    use crate::node::{LeafData, PersistentHOTNode};
    use crate::store::MemoryNodeStore;

    /// 辅助函数：创建测试用的 key
    fn make_key(seed: u8) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0] = seed;
        key
    }

    #[test]
    fn test_empty_tree_lookup() {
        let store = MemoryNodeStore::new();
        let tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        let key = make_key(1);
        let result = tree.lookup(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_single_leaf_lookup() {
        let mut store = MemoryNodeStore::new();

        // 创建叶子数据
        let key = make_key(42);
        let value = b"hello world".to_vec();
        let leaf = LeafData {
            key,
            value: value.clone(),
        };
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 创建只有一个叶子的节点
        let node = PersistentHOTNode::single_leaf(leaf_id.clone());
        let node_id = node.compute_node_id::<Blake3Hasher>(1);
        store.put_node(&node_id, &node).unwrap();

        // 创建树
        let tree: HOTTree<_, Blake3Hasher> = HOTTree::with_root(store, node_id);

        // 查找存在的 key
        let result = tree.lookup(&key).unwrap();
        assert_eq!(result, Some(value));

        // 查找不存在的 key
        let other_key = make_key(99);
        let result = tree.lookup(&other_key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_tree_accessors() {
        let store = MemoryNodeStore::new();
        let tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        assert!(tree.is_empty());
        assert!(tree.root_id().is_none());
    }

    // ========================================================================
    // Insert 测试
    // ========================================================================

    #[test]
    fn test_insert_into_empty_tree() {
        let store = MemoryNodeStore::new();
        let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        let key = make_key(1);
        let value = b"value1".to_vec();

        tree.insert(&key, value.clone(), 1).unwrap();

        assert!(!tree.is_empty());
        let result = tree.lookup(&key).unwrap();
        assert_eq!(result, Some(value));
    }

    #[test]
    fn test_insert_two_keys() {
        let store = MemoryNodeStore::new();
        let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        let key1 = make_key(1);
        let value1 = b"value1".to_vec();
        let key2 = make_key(2);
        let value2 = b"value2".to_vec();

        tree.insert(&key1, value1.clone(), 1).unwrap();
        tree.insert(&key2, value2.clone(), 1).unwrap();

        assert_eq!(tree.lookup(&key1).unwrap(), Some(value1));
        assert_eq!(tree.lookup(&key2).unwrap(), Some(value2));
    }

    #[test]
    fn test_insert_update_existing() {
        let store = MemoryNodeStore::new();
        let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        let key = make_key(1);
        let value1 = b"value1".to_vec();
        let value2 = b"updated".to_vec();

        tree.insert(&key, value1, 1).unwrap();
        tree.insert(&key, value2.clone(), 2).unwrap();

        let result = tree.lookup(&key).unwrap();
        assert_eq!(result, Some(value2));
    }

    #[test]
    fn test_insert_multiple_keys() {
        let store = MemoryNodeStore::new();
        let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

        // 插入 10 个 keys
        for i in 0..10u8 {
            let key = make_key(i);
            let value = format!("value{}", i).into_bytes();
            tree.insert(&key, value, 1).unwrap();
        }

        // 验证所有 keys 都能找到
        for i in 0..10u8 {
            let key = make_key(i);
            let result = tree.lookup(&key).unwrap();
            assert!(result.is_some(), "Key {} not found", i);
            assert_eq!(result.unwrap(), format!("value{}", i).into_bytes());
        }

        // 验证不存在的 key
        let missing_key = make_key(100);
        assert!(tree.lookup(&missing_key).unwrap().is_none());
    }
}
