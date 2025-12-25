//! 辅助函数

use crate::hash::Hasher;
use crate::node::{ChildRef, NodeId, PersistentHOTNode};
use crate::store::{NodeStore, Result, StoreError};

use super::core::{HOTTree, InsertStackEntry};

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
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
            new_node.children[entry.child_index] = ChildRef::Internal(new_child_id.clone());

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
    pub(super) fn get_entry_key(&self, child: &ChildRef) -> Result<[u8; 32]> {
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
