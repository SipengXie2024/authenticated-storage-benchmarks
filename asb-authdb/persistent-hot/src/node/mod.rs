//! HOT 节点模块
//!
//! 包含 PersistentHOTNode 及其相关类型和操作。

mod bitmask;
mod core;
mod insert;
mod search;
mod split;
mod types;
mod utils;

#[cfg(test)]
mod tests;

// Re-export 公开 API
pub use self::core::PersistentHOTNode;
pub use types::{
    bincode_config, make_node_id, node_id_hash, node_id_version, BiNode, ChildRef,
    InsertInformation, LeafData, NodeId, SearchResult, NODE_ID_SIZE,
};
pub use utils::{extract_bit, find_first_differing_bit};
