//! 辅助函数

use crate::hash::Hasher;
use crate::node::{extract_bit, find_first_differing_bit, NodeId, PersistentHOTNode, SearchResult, SplitChild};
use crate::store::{NodeStore, Result, StoreError};

use super::core::{HOTTree, InsertStackEntry};

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 获取 child 的高度（Leaf=0，Internal 读取存储）
    pub(super) fn get_child_height(&self, child_id: &NodeId) -> Result<u8> {
        if let Some(height) = child_id.height_if_leaf() {
            return Ok(height);
        }

        let node = self
            .store
            .get_node(child_id)?
            .ok_or(StoreError::NotFound)?;
        Ok(node.height)
    }

    /// 将 split 子节点物化为 NodeId（必要时持久化新节点）
    pub(super) fn materialize_split_child(
        &mut self,
        child: SplitChild,
        version: u64,
    ) -> Result<NodeId> {
        match child {
            SplitChild::Existing(id) => Ok(id),
            SplitChild::Node(node) => {
                let id = node.compute_node_id::<H>(version);
                self.store.put_node(&id, &node)?;
                Ok(id)
            }
        }
    }

    /// 将 split 子节点物化为 NodeId，并返回高度
    pub(super) fn materialize_split_child_with_height(
        &mut self,
        child: SplitChild,
        version: u64,
    ) -> Result<(NodeId, u8)> {
        match child {
            SplitChild::Existing(id) => {
                let height = self.get_child_height(&id)?;
                Ok((id, height))
            }
            SplitChild::Node(node) => {
                let id = node.compute_node_id::<H>(version);
                let height = node.height;
                self.store.put_node(&id, &node)?;
                Ok((id, height))
            }
        }
    }

    /// 向上传播指针更新
    ///
    /// 从栈中依次取出父节点，更新其 child 引用
    pub(super) fn propagate_pointer_updates(
        &mut self,
        mut stack: Vec<InsertStackEntry>,
        mut new_child_id: NodeId,
        version: u64,
    ) -> Result<()> {
        while let Some(entry) = stack.pop() {
            // 更新父节点的 child 引用
            let mut new_node = entry.node.clone();
            new_node.children[entry.child_index] = new_child_id;

            // 读取新子节点获取高度（用于维护 height 不变量）
            if let Ok(Some(child)) = self.store.get_node(&new_child_id) {
                new_node.height = std::cmp::max(new_node.height, child.height + 1);
            }

            let new_node_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_node_id, &new_node)?;
            new_child_id = new_node_id;
        }

        // 更新根节点
        self.root_id = Some(new_child_id);
        Ok(())
    }

    /// 找到 affected entry 索引
    ///
    /// 使用 sparse matching 找到最后一个 (dense & sparse) == sparse 的 entry。
    /// 按照 HOT 设计，应该总是能找到匹配（至少 sparse=0 总是匹配）。
    /// 返回 None 表示数据结构不一致。
    pub(super) fn find_affected_entry(
        &self,
        node: &PersistentHOTNode,
        dense_key: u32,
    ) -> Option<usize> {
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
    pub(super) fn get_entry_key(&self, child: &NodeId) -> Result<[u8; 32]> {
        match child {
            NodeId::Leaf(_) => {
                let leaf = self
                    .store
                    .get_leaf(child)?
                    .ok_or(StoreError::NotFound)?;
                Ok(leaf.key)
            }
            NodeId::Internal(_) => {
                // 对于内部节点，递归获取第一个叶子的 key
                let node = self
                    .store
                    .get_node(child)?
                    .ok_or(StoreError::NotFound)?;
                if node.len() > 0 {
                    self.get_entry_key(&node.children[0])
                } else {
                    Err(StoreError::NotFound)
                }
            }
        }
    }

    /// 为 split child 计算真实的 discriminative bit
    ///
    /// C++ 对齐：BiNode 必须使用真实的 diff bit，不能用占位符。
    /// 对应 C++ BiNode::createFromExistingAndNewEntry 总是用真实 key 计算。
    pub(super) fn compute_disc_bit_for_split_child(
        &self,
        split_child: &SplitChild,
        key: &[u8; 32],
    ) -> Result<(u16, bool)> {
        match split_child {
            SplitChild::Existing(id) => {
                let existing_key = self.get_entry_key(id)?;
                let diff = find_first_differing_bit(&existing_key, key)
                    .expect("Keys must be different");
                Ok((diff, extract_bit(key, diff)))
            }
            SplitChild::Node(node) => {
                // 在节点中搜索并找到 affected entry
                let affected_key = match node.search(key) {
                    SearchResult::Found { index } => {
                        self.get_entry_key(&node.children[index])?
                    }
                    SearchResult::NotFound { dense_key } => {
                        let idx = self.find_affected_entry(node, dense_key)
                            .expect("HOT invariant violated");
                        self.get_entry_key(&node.children[idx])?
                    }
                };
                let diff = find_first_differing_bit(&affected_key, key)
                    .expect("Keys must be different");
                Ok((diff, extract_bit(key, diff)))
            }
        }
    }
}
