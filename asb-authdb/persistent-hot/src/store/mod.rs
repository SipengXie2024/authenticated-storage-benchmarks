//! 节点存储层
//!
//! 提供 HOT 树的持久化存储：
//! - `KvNodeStore`: 基于 kvdb trait 的持久化存储
//! - `CachedNodeStore`: 带 Write-Back 缓存的存储包装器

mod cached;
mod error;

#[cfg(feature = "kvdb-backend")]
mod kvdb;

// Re-export 公开 API
pub use cached::{CacheStats, CachedNodeStore};
pub use error::{Result, StoreError};

#[cfg(feature = "kvdb-backend")]
pub use self::kvdb::KvNodeStore;
