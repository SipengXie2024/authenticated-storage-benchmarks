//! 查找操作

use crate::hash::Hasher;
use crate::node::{NodeId, SearchResult};
use crate::store::{NodeStore, Result, StoreError};

use super::core::HOTTree;

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
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

        let search_result = node.search(key);

        match search_result {
            SearchResult::Found { index } => {
                let child = &node.children[index];
                match child {
                    NodeId::Internal(_) => {
                        // 递归搜索子节点
                        self.lookup_internal(child, key)
                    }
                    NodeId::Leaf(_) => {
                        // 获取叶子数据，验证 key 完全匹配
                        let leaf = self
                            .store
                            .get_leaf(child)?
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
}
