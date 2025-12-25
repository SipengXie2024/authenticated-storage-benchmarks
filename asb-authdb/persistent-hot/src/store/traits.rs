//! NodeStore trait 定义

use super::error::Result;
use crate::node::{LeafData, NodeId, PersistentHOTNode};

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
