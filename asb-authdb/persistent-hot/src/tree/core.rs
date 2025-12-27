//! HOTTree 核心结构体

use std::marker::PhantomData;

use crate::hash::{Blake3Hasher, Hasher};
use crate::node::{NodeId, PersistentHOTNode};
use crate::store::{CachedNodeStore, NodeStore};

// ============================================================================
// Insert Stack
// ============================================================================

/// 插入栈条目
///
/// 用于追踪从根到当前节点的路径，支持 Parent Pull Up 操作。
/// 在 overflow 处理时，需要沿路径向上传播更新。
#[derive(Debug, Clone)]
pub(super) struct InsertStackEntry {
    /// 当前节点的 ID（用于调试和潜在的扩展）
    #[allow(dead_code)]
    pub node_id: NodeId,
    /// 选中的 child 索引（用于 overflow 时更新父节点）
    pub child_index: usize,
    /// 缓存的节点数据（避免重复读取）
    pub node: PersistentHOTNode,
}

// ============================================================================
// HOT Tree
// ============================================================================

/// Height Optimized Trie
///
/// # 类型参数
///
/// - `S`: 底层存储实现，必须实现 `NodeStore` trait
/// - `H`: 哈希算法，默认 Blake3
///
/// # 缓存层
///
/// 树操作强制经过 `CachedNodeStore<S>` 缓存层，用户传入的底层存储会被自动包装。
///
/// # 版本管理
///
/// `version` 不存储在结构中，而是作为 insert 参数传入。
pub struct HOTTree<S: NodeStore, H: Hasher = Blake3Hasher> {
    pub(super) store: CachedNodeStore<S>,
    pub(super) root_id: Option<NodeId>,
    pub(super) _marker: PhantomData<H>,
}

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 创建空树
    ///
    /// 传入的底层存储会被自动包装为 `CachedNodeStore`。
    pub fn new(store: S) -> Self {
        Self {
            store: CachedNodeStore::new(store),
            root_id: None,
            _marker: PhantomData,
        }
    }

    /// 创建带有根节点的树
    ///
    /// 传入的底层存储会被自动包装为 `CachedNodeStore`。
    pub fn with_root(store: S, root_id: NodeId) -> Self {
        Self {
            store: CachedNodeStore::new(store),
            root_id: Some(root_id),
            _marker: PhantomData,
        }
    }

    /// 获取根节点 ID
    #[inline]
    pub fn root_id(&self) -> Option<&NodeId> {
        self.root_id.as_ref()
    }

    /// 获取缓存存储引用
    #[inline]
    pub fn store(&self) -> &CachedNodeStore<S> {
        &self.store
    }

    /// 获取可变缓存存储引用
    #[inline]
    pub fn store_mut(&mut self) -> &mut CachedNodeStore<S> {
        &mut self.store
    }

    /// 检查树是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root_id.is_none()
    }

    // ========== 底层存储访问 ==========

    /// 获取底层存储引用（绕过缓存层）
    #[inline]
    pub fn inner_store(&self) -> &S {
        self.store.inner()
    }

    /// 获取底层存储可变引用（绕过缓存层）
    #[inline]
    pub fn inner_store_mut(&mut self) -> &mut S {
        self.store.inner_mut()
    }

    // ========== 缓存操作便捷方法 ==========

    /// 获取缓存统计信息
    #[inline]
    pub fn cache_stats(&self) -> crate::store::CacheStats {
        self.store.stats()
    }

    /// 刷新缓存到底层存储
    ///
    /// 将所有脏数据写入底层存储并清空缓存。
    #[inline]
    pub fn flush_cache(&mut self) -> crate::store::Result<()> {
        self.store.flush()
    }
}
