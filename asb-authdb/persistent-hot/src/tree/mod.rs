//! HOTTree 模块
//!
//! 提供 tree-level 的 lookup/insert/delete 操作，
//! 基于 `PersistentHOTNode` 节点和 `NodeStore` 存储抽象。

mod core;
mod helpers;
mod insert;
mod lookup;
mod overflow;

// Re-export 公开 API
pub use self::core::HOTTree;
