//! 节点核心类型定义
//!
//! 包含 NodeId、SearchResult、LeafData、InsertInformation、BiNode

use bincode::Options;
use serde::{Deserialize, Serialize};

use super::utils::extract_bit;
use crate::hash::Hasher;

// ============================================================================
// NodeId
// ============================================================================

/// NodeId 裸字节大小：8 字节 version + 32 字节 content hash
pub const NODE_ID_SIZE: usize = 40;

/// 节点标识符（区分 Leaf/Internal）
///
/// 格式：`[version: 8 bytes big-endian][content_hash: 32 bytes]`
///
/// Version 的作用：
/// - Epoch 追踪：标识数据属于哪个 commit epoch
/// - 历史查询：支持查询特定版本的状态
/// - 垃圾回收：根据 version 判断数据是否可回收
/// - 冲突检测：同一 content hash 不同 version 是不同数据
///
/// 高度语义（对齐 C++）：
/// - `NodeId::Leaf`: 高度 = 0
/// - `NodeId::Internal`: 高度 ≥ 1
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeId {
    /// 叶子节点（关联 LeafData，高度 = 0）
    Leaf([u8; NODE_ID_SIZE]),
    /// 内部节点（关联 PersistentHOTNode，高度 ≥ 1）
    Internal([u8; NODE_ID_SIZE]),
}

impl NodeId {
    /// 创建 Leaf NodeId
    #[inline]
    pub fn leaf(version: u64, content_hash: &[u8; 32]) -> Self {
        NodeId::Leaf(make_raw_id(version, content_hash))
    }

    /// 创建 Internal NodeId
    #[inline]
    pub fn internal(version: u64, content_hash: &[u8; 32]) -> Self {
        NodeId::Internal(make_raw_id(version, content_hash))
    }

    /// 检查是否为叶子节点
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeId::Leaf(_))
    }

    /// 检查是否为内部节点
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, NodeId::Internal(_))
    }

    /// 获取裸字节引用
    #[inline]
    pub fn raw_bytes(&self) -> &[u8; NODE_ID_SIZE] {
        match self {
            NodeId::Leaf(id) | NodeId::Internal(id) => id,
        }
    }

    /// 获取 version
    #[inline]
    pub fn version(&self) -> u64 {
        u64::from_be_bytes(self.raw_bytes()[0..8].try_into().unwrap())
    }

    /// 获取 content hash
    #[inline]
    pub fn content_hash(&self) -> [u8; 32] {
        self.raw_bytes()[8..40].try_into().unwrap()
    }

    /// 获取高度（Leaf = 0，Internal 需要查询 store）
    #[inline]
    pub fn height_if_leaf(&self) -> Option<u8> {
        match self {
            NodeId::Leaf(_) => Some(0),
            NodeId::Internal(_) => None,
        }
    }
}

// 手动实现 Serialize/Deserialize（1 byte discriminant + 40 bytes）
impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(NODE_ID_SIZE + 1)?;
        match self {
            NodeId::Internal(id) => {
                tuple.serialize_element(&0u8)?;
                for byte in id.iter() {
                    tuple.serialize_element(byte)?;
                }
            }
            NodeId::Leaf(id) => {
                tuple.serialize_element(&1u8)?;
                for byte in id.iter() {
                    tuple.serialize_element(byte)?;
                }
            }
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeIdVisitor;

        impl<'de> serde::de::Visitor<'de> for NodeIdVisitor {
            type Value = NodeId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a NodeId (discriminant + 40 bytes)")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let discriminant: u8 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;

                let mut raw_id = [0u8; NODE_ID_SIZE];
                for i in 0..NODE_ID_SIZE {
                    raw_id[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i + 1, &self))?;
                }

                match discriminant {
                    0 => Ok(NodeId::Internal(raw_id)),
                    1 => Ok(NodeId::Leaf(raw_id)),
                    _ => Err(serde::de::Error::custom(format!(
                        "Invalid NodeId discriminant: {}",
                        discriminant
                    ))),
                }
            }
        }

        deserializer.deserialize_tuple(NODE_ID_SIZE + 1, NodeIdVisitor)
    }
}

/// 从 version 和 content hash 构造裸 ID 字节
#[inline]
pub fn make_raw_id(version: u64, content_hash: &[u8; 32]) -> [u8; NODE_ID_SIZE] {
    let mut id = [0u8; NODE_ID_SIZE];
    id[0..8].copy_from_slice(&version.to_be_bytes());
    id[8..40].copy_from_slice(content_hash);
    id
}

// ============================================================================
// SearchResult
// ============================================================================

/// 节点搜索结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchResult {
    /// 找到匹配的 entry
    Found {
        /// 匹配的索引（直接对应 children[index]）
        index: usize,
    },
    /// 未找到匹配
    NotFound {
        /// 搜索的 dense partial key（避免重复计算）
        dense_key: u32,
    },
}

impl SearchResult {
    /// 获取找到的索引
    #[inline]
    pub fn found_index(&self) -> Option<usize> {
        match self {
            SearchResult::Found { index } => Some(*index),
            SearchResult::NotFound { .. } => None,
        }
    }

    /// 检查是否找到
    #[inline]
    pub fn is_found(&self) -> bool {
        matches!(self, SearchResult::Found { .. })
    }
}

// ============================================================================
// LeafData
// ============================================================================

/// 叶子数据（单独存储）
///
/// 与内部节点分开存储，支持大 value，节点大小可预测。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeafData {
    /// U256 key（固定 32 字节）
    pub key: [u8; 32],
    /// Value（可变长度）
    pub value: Vec<u8>,
}

impl LeafData {
    /// 创建新叶子
    pub fn new(key: [u8; 32], value: Vec<u8>) -> Self {
        Self { key, value }
    }

    /// 计算 NodeId（返回 NodeId::Leaf）
    pub fn compute_node_id<H: Hasher>(&self, version: u64) -> NodeId {
        let bytes = self.to_bytes().expect("LeafData serialization should never fail");
        let hash = H::hash(&bytes);
        NodeId::leaf(version, &hash)
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode_config().serialize(self)
    }

    /// 从字节反序列化
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode_config().deserialize(bytes)
    }
}

// ============================================================================
// InsertInformation
// ============================================================================

/// 插入信息（对应 C++ `InsertInformation`）
///
/// 包含执行插入操作所需的信息：
/// - 受影响子树中共享的 prefix partial key
/// - 受影响子树的范围（first index 和 entry count）
/// - 新 key 的 discriminative bit 信息
///
/// # 关键用途
///
/// 用于判断 `isSingleEntry`：如果 `number_entries_in_affected_subtree == 1`，
/// 则表示新 key 只影响一个 entry，可以进行 Leaf Pushdown 或递归插入。
/// 否则，应该在当前节点执行 Normal Insert。
#[derive(Debug, Clone)]
pub struct InsertInformation {
    /// 受影响子树内所有 entries 共享的 prefix bits 的 partial key
    pub subtree_prefix_partial_key: u32,
    /// 受影响子树中第一个 entry 的索引
    pub first_index_in_affected_subtree: usize,
    /// 受影响子树中 entries 的总数
    pub number_entries_in_affected_subtree: usize,
    /// 新 key 的 discriminative bit 位置
    pub discriminative_bit: u16,
    /// 新 key 在 discriminative bit 处的值
    pub new_bit_value: bool,
    /// 受影响子树的 bitmask（第 i 位为 1 表示 entry i 属于 affected subtree）
    pub affected_subtree_mask: u32,
}

impl InsertInformation {
    /// 检查受影响子树是否只有一个 entry
    #[inline]
    pub fn is_single_entry(&self) -> bool {
        self.number_entries_in_affected_subtree == 1
    }
}

// ============================================================================
// BiNode
// ============================================================================

/// Split 操作的结果
///
/// 表示将一个满节点分裂为两个子节点的结果。
/// BiNode 持有已存储子节点的 NodeId（Copy-on-Write 模式）。
///
/// # 字段
///
/// - `discriminative_bit`: 分裂点的 bit 位置（MSB）
/// - `left`: 左子树 NodeId（该 bit = 0）
/// - `right`: 右子树 NodeId（该 bit = 1）
/// - `height`: 子树的高度（继承自原节点）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BiNode {
    /// 分裂 bit 的绝对位置（0-255）
    pub discriminative_bit: u16,
    /// 左子树（bit = 0），已存储
    pub left: NodeId,
    /// 右子树（bit = 1），已存储
    pub right: NodeId,
    /// 子树高度
    pub height: u8,
}

impl BiNode {
    /// 从两个已有值创建 BiNode
    ///
    /// 根据 key 中 discriminative_bit 的值决定左右位置
    pub fn from_existing_and_new(
        discriminative_bit: u16,
        existing_key: &[u8; 32],
        existing_id: NodeId,
        new_id: NodeId,
        height: u8,
    ) -> Self {
        let existing_bit = extract_bit(existing_key, discriminative_bit);
        if existing_bit {
            // existing 的 bit = 1，放右边
            BiNode {
                discriminative_bit,
                left: new_id,
                right: existing_id,
                height,
            }
        } else {
            // existing 的 bit = 0，放左边
            BiNode {
                discriminative_bit,
                left: existing_id,
                right: new_id,
                height,
            }
        }
    }

    /// 创建包含两个 entry 的节点
    ///
    /// 根据 BiNode 信息创建一个新的 PersistentHOTNode。
    /// 用于 Intermediate Node Creation 场景。
    ///
    /// # Height
    ///
    /// 返回节点的 height = bi_node.height
    /// BiNode.height 已经表示"实体化后节点的高度"（子节点高度 + 1）
    pub fn to_two_entry_node(&self) -> super::core::PersistentHOTNode {
        let mut node = super::core::PersistentHOTNode::empty(self.height);
        node.extraction_masks = super::core::PersistentHOTNode::masks_from_bits(&[self.discriminative_bit]);
        // left (bit=0) 放前面，sparse_key = 0
        // right (bit=1) 放后面，sparse_key = 1
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        // left/right 已经是 NodeId 类型
        node.children = vec![self.left, self.right];
        node
    }
}

// ============================================================================
// bincode 配置（内部使用）
// ============================================================================

/// 创建确定性 bincode 配置
pub fn bincode_config() -> impl bincode::Options {
    bincode::options()
        .with_little_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}
