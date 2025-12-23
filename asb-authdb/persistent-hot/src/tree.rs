//! HOTTree: Height Optimized Trie 的树级操作
//!
//! 提供 tree-level 的 lookup/insert/delete 操作，
//! 基于 `PersistentHOTNode` 节点和 `NodeStore` 存储抽象。

use std::marker::PhantomData;

use crate::hash::{Blake3Hasher, Hasher};
use crate::node::{ChildRef, NodeId, SearchResult};
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
}
