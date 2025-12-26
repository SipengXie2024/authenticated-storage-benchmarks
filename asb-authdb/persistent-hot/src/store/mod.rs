//! 节点存储抽象层
//!
//! 提供 `NodeStore` trait 用于节点和叶子的持久化存储和检索。
//! 实现：
//! - `MemoryNodeStore`: 内存存储，用于测试
//! - `KvNodeStore`: 基于 kvdb trait 的持久化存储（需要 `kvdb-backend` feature）
//! - `CachedNodeStore`: 带 Write-Back 缓存的存储装饰器

mod cached;
mod error;
mod memory;
mod traits;

#[cfg(feature = "kvdb-backend")]
mod kvdb;

#[cfg(test)]
mod tests;

// Re-export 公开 API
pub use cached::{CacheStats, CachedNodeStore};
pub use error::{Result, StoreError};
pub use memory::MemoryNodeStore;
pub use traits::NodeStore;

#[cfg(feature = "kvdb-backend")]
pub use kvdb::KvNodeStore;
