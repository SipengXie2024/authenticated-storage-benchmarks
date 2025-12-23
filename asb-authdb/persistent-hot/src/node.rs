//! HOT 节点核心数据结构
//!
//! 本模块定义了持久化 HOT 的核心类型：
//! - `NodeId`: 40 字节节点标识符（8 字节 version + 32 字节 content hash）
//! - `LeafData`: 叶子数据（单独存储）
//! - `ChildRef`: 子节点引用（统一使用 NodeId）
//! - `PersistentHOTNode`: HOT 复合节点的持久化表示
//! - `SearchResult`: 节点内搜索结果
//!
//! # 设计决策
//!
//! 1. **40 字节 NodeId**: 包含 version 用于历史查询和垃圾回收
//! 2. **叶子数据单独存储**: 节点大小可预测，支持大 value
//! 3. **固定数组布局**: SIMD 友好，缓存效率高
//! 4. **Content-Addressed**: 节点 ID = 节点内容的哈希

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::bits::{pext32, pext64};
use crate::hash::Hasher;
use crate::simd::{simd_search, SimdSearchResult};

// ============================================================================
// NodeId
// ============================================================================

/// NodeId 大小：8 字节 version + 32 字节 content hash
pub const NODE_ID_SIZE: usize = 40;

/// 节点标识符：版本 + 内容哈希
///
/// 格式：`[version: 8 bytes big-endian][content_hash: 32 bytes]`
///
/// Version 的作用：
/// - Epoch 追踪：标识数据属于哪个 commit epoch
/// - 历史查询：支持查询特定版本的状态
/// - 垃圾回收：根据 version 判断数据是否可回收
/// - 冲突检测：同一 content hash 不同 version 是不同数据
pub type NodeId = [u8; NODE_ID_SIZE];

/// 从 version 和 content hash 构造 NodeId
#[inline]
pub fn make_node_id(version: u64, content_hash: &[u8; 32]) -> NodeId {
    let mut id = [0u8; NODE_ID_SIZE];
    id[0..8].copy_from_slice(&version.to_be_bytes());
    id[8..40].copy_from_slice(content_hash);
    id
}

/// 从 NodeId 提取 version
#[inline]
pub fn node_id_version(id: &NodeId) -> u64 {
    u64::from_be_bytes(id[0..8].try_into().unwrap())
}

/// 从 NodeId 提取 content hash
#[inline]
pub fn node_id_hash(id: &NodeId) -> [u8; 32] {
    id[8..40].try_into().unwrap()
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

    /// 计算 NodeId
    pub fn compute_node_id<H: Hasher>(&self, version: u64) -> NodeId {
        let bytes = self.to_bytes().expect("LeafData serialization should never fail");
        let hash = H::hash(&bytes);
        make_node_id(version, &hash)
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
// ChildRef
// ============================================================================

/// 子节点引用
///
/// 保留 Internal/Leaf 区分（类型安全，调试友好），
/// 但都使用 NodeId 引用，叶子数据单独存储。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildRef {
    /// 内部节点引用
    Internal(NodeId),
    /// 叶子节点引用（指向单独存储的 LeafData）
    Leaf(NodeId),
}

// 手动实现 Serialize/Deserialize（serde 默认不支持 [u8; 40]）
impl Serialize for ChildRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        // 格式：(discriminant: u8, node_id: [u8; 40])
        let mut tuple = serializer.serialize_tuple(NODE_ID_SIZE + 1)?;
        match self {
            ChildRef::Internal(id) => {
                tuple.serialize_element(&0u8)?;
                for byte in id.iter() {
                    tuple.serialize_element(byte)?;
                }
            }
            ChildRef::Leaf(id) => {
                tuple.serialize_element(&1u8)?;
                for byte in id.iter() {
                    tuple.serialize_element(byte)?;
                }
            }
        }
        tuple.end()
    }
}

impl<'de> Deserialize<'de> for ChildRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ChildRefVisitor;

        impl<'de> serde::de::Visitor<'de> for ChildRefVisitor {
            type Value = ChildRef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a ChildRef (discriminant + 40 byte NodeId)")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let discriminant: u8 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;

                let mut node_id = [0u8; NODE_ID_SIZE];
                for i in 0..NODE_ID_SIZE {
                    node_id[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i + 1, &self))?;
                }

                match discriminant {
                    0 => Ok(ChildRef::Internal(node_id)),
                    1 => Ok(ChildRef::Leaf(node_id)),
                    _ => Err(serde::de::Error::custom(format!(
                        "Invalid ChildRef discriminant: {}",
                        discriminant
                    ))),
                }
            }
        }

        deserializer.deserialize_tuple(NODE_ID_SIZE + 1, ChildRefVisitor)
    }
}

impl ChildRef {
    /// 检查是否为叶子节点
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, ChildRef::Leaf(_))
    }

    /// 检查是否为内部节点
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, ChildRef::Internal(_))
    }

    /// 获取 NodeId 引用
    #[inline]
    pub fn node_id(&self) -> &NodeId {
        match self {
            ChildRef::Internal(id) | ChildRef::Leaf(id) => id,
        }
    }

    /// 获取子节点的高度（叶子节点固定为 1）
    pub fn height_if_leaf(&self) -> Option<u8> {
        match self {
            ChildRef::Leaf(_) => Some(1),
            ChildRef::Internal(_) => None,
        }
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

    /// 创建包含两个叶子的节点
    ///
    /// 根据 BiNode 信息创建一个新的 PersistentHOTNode
    pub fn to_two_entry_node(&self) -> PersistentHOTNode {
        let mut node = PersistentHOTNode::empty(self.height + 1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[self.discriminative_bit]);
        // left (bit=0) 放前面，sparse_key = 0
        // right (bit=1) 放后面，sparse_key = 1
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        node.children = vec![
            ChildRef::Internal(self.left.clone()),
            ChildRef::Internal(self.right.clone()),
        ];
        node
    }
}

// ============================================================================
// PersistentHOTNode
// ============================================================================

/// HOT 节点的持久化表示
///
/// 混合布局策略（v4 设计）：
/// - `sparse_partial_keys: [u32; 32]` — 固定大小，SIMD 友好
/// - `children: Vec<ChildRef>` — 紧凑存储，节省空间
/// - `len()` 从 `children.len()` 推断，无需额外字段
/// - 索引直接对应：`keys[i] ↔ children[i]`
///
/// # 核心约束
///
/// - Maximum Span: 32（u32 partial key 的位宽）
/// - Maximum Fanout: 32（SIMD 友好，4 × AVX2）
///
/// # 不变量
///
/// 1. `len() <= 32`
/// 2. `span() <= 32`
/// 3. `height > 0`
/// 4. `sparse_partial_keys[0..len()]` 有效，按值升序
/// 5. `children[i]` 对应 `sparse_partial_keys[i]`（直接索引）
/// 6. `sparse_partial_keys[len()..32]` 是垃圾数据，不可信任
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentHOTNode {
    /// 节点在树中的高度
    ///
    /// 定义：h(n) = max(h(children)) + 1
    /// 叶子节点的 height = 1
    pub height: u8,

    /// Extraction masks，用于 PEXT 提取 dense partial key
    ///
    /// 覆盖 U256 的全部 256 bits：
    /// - masks[0]: bits 0-63
    /// - masks[1]: bits 64-127
    /// - masks[2]: bits 128-191
    /// - masks[3]: bits 192-255
    pub extraction_masks: [u64; 4],

    /// Sparse partial keys（固定 32 槽位，SIMD 友好）
    ///
    /// 只有 [0..len()] 有效，按值升序排列。
    /// [len()..32] 是未初始化区域（垃圾数据），由 valid_mask() 过滤。
    pub sparse_partial_keys: [u32; 32],

    /// Children（紧凑存储）
    ///
    /// `children.len()` = 有效 entries 数量。
    /// `children[i]` 对应 `sparse_partial_keys[i]`（直接索引）。
    pub children: Vec<ChildRef>,
}

/// 创建确定性 bincode 配置
fn bincode_config() -> impl bincode::Options {
    bincode::options()
        .with_little_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

impl PersistentHOTNode {
    // ========================================================================
    // 基本访问器
    // ========================================================================

    /// 有效 entries 数量（从 children.len() 推断）
    #[inline]
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// 是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// 是否已满
    #[inline]
    pub fn is_full(&self) -> bool {
        self.children.len() >= 32
    }

    /// 用于 SIMD 过滤的 valid mask（动态计算）
    ///
    /// 返回连续的低位 1，用于过滤 sparse_partial_keys 尾部垃圾数据
    #[inline]
    pub fn valid_mask(&self) -> u32 {
        let len = self.children.len();
        if len >= 32 {
            u32::MAX
        } else {
            (1u32 << len) - 1
        }
    }

    /// 获取 child（直接索引）
    #[inline]
    pub fn get_child(&self, index: usize) -> &ChildRef {
        debug_assert!(index < self.len());
        &self.children[index]
    }

    /// 获取 child（可变引用）
    #[inline]
    pub fn get_child_mut(&mut self, index: usize) -> &mut ChildRef {
        debug_assert!(index < self.len());
        &mut self.children[index]
    }

    /// Discriminative bits 数量（span）
    #[inline]
    pub fn span(&self) -> u32 {
        self.extraction_masks.iter().map(|m| m.count_ones()).sum()
    }

    // ========================================================================
    // Bitmask 风格操作（对齐 C++ HOT 实现）
    // ========================================================================

    /// 返回最小的 discriminative bit index（用于 Split 分区）
    ///
    /// 对应 C++ 的 `mMostSignificantDiscriminativeBitIndex`。
    /// 注意：C++ 称之为 "most significant" 是因为它在 trie 中最先被检查，
    /// 而不是因为它在数值上最大。
    ///
    /// # 返回
    /// - `Some(bit_index)`: 最小的 key bit index
    /// - `None`: 节点没有 discriminative bits
    #[inline]
    pub fn first_discriminative_bit(&self) -> Option<u16> {
        for (chunk, &mask) in self.extraction_masks.iter().enumerate() {
            if mask != 0 {
                // mask 中最高的 u64 bit 对应最小的 key bit
                // 因为 key bit N → u64 bit (63 - N%64)
                let u64_msb = 63 - mask.leading_zeros() as u16;
                let key_bit_in_chunk = 63 - u64_msb;
                return Some((chunk as u16) * 64 + key_bit_in_chunk);
            }
        }
        None
    }

    /// 返回所有有效 bits 的 mask（连续的低位 1）
    ///
    /// 对应 C++ 的 `getAllMaskBits()`。
    /// 用于 PDEP/PEXT 的 conversion mask 计算。
    ///
    /// # 示例
    /// - span = 3 → 返回 0b111
    /// - span = 5 → 返回 0b11111
    #[inline]
    pub fn get_all_mask_bits(&self) -> u32 {
        let span = self.span();
        if span >= 32 {
            u32::MAX
        } else if span == 0 {
            0
        } else {
            (1u32 << span) - 1
        }
    }

    /// 获取某个 key bit 在 sparse key 中对应的 mask
    ///
    /// 对应 C++ 的 `getMaskFor(DiscriminativeBit)`。
    /// 返回只有一个 bit 为 1 的 mask，表示该 key bit 在 sparse key 中的位置。
    ///
    /// # 参数
    /// - `bit`: key bit index (0-255)
    ///
    /// # 返回
    /// - 如果该 bit 是 discriminative bit，返回对应的 mask
    /// - 如果该 bit 不是 discriminative bit，返回 0
    ///
    /// # 实现
    /// 构造只有目标 bit 的虚拟 key chunk，然后用 PEXT 提取。
    #[inline]
    pub fn get_mask_for_bit(&self, bit: u16) -> u32 {
        let chunk = (bit / 64) as usize;
        let bit_in_chunk = bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk; // MSB-first 转换

        if chunk >= 4 {
            return 0;
        }

        let mask = self.extraction_masks[chunk];
        let single_bit = 1u64 << u64_bit_pos;

        // 检查该 bit 是否在 mask 中
        if (mask & single_bit) == 0 {
            return 0;
        }

        // 使用 PEXT 计算该 bit 在 sparse key 中的位置
        // 先计算之前所有 chunks 贡献的 bits 数量
        let offset: u32 = self.extraction_masks[..chunk]
            .iter()
            .map(|m| m.count_ones())
            .sum();

        // 在当前 chunk 中，该 bit 之前（更低 u64 bit position）有多少个 1
        let lower_mask = single_bit - 1; // 比 single_bit 更低的所有位
        let bits_before = (mask & lower_mask).count_ones();

        1u32 << (offset + bits_before)
    }

    /// 获取 Split 分区用的 root mask
    ///
    /// 返回最小 discriminative bit 对应的 sparse key mask。
    ///
    /// **注意**：由于 PEXT 按 chunk 顺序处理，最小 key bit 不一定对应
    /// sparse key 的最高位。必须通过 `get_mask_for_bit` 计算实际位置。
    #[inline]
    pub fn get_root_mask(&self) -> u32 {
        match self.first_discriminative_bit() {
            Some(bit) => self.get_mask_for_bit(bit),
            None => 0,
        }
    }

    /// 找到 sparse key 应该插入的位置（保持升序）
    ///
    /// 使用 SIMD 优化（AVX2 可用时）
    pub fn find_insert_position(&self, sparse_key: u32) -> usize {
        crate::simd::simd_find_insert_position(
            &self.sparse_partial_keys,
            sparse_key,
            self.len() as u8,
        )
    }

    // ========================================================================
    // 构造函数
    // ========================================================================

    /// 创建空节点
    pub fn empty(height: u8) -> Self {
        Self {
            height,
            extraction_masks: [0; 4],
            sparse_partial_keys: [0; 32],
            children: Vec::new(),
        }
    }

    /// 创建单叶子节点
    ///
    /// 需要传入已存储的叶子的 NodeId
    pub fn single_leaf(leaf_id: NodeId) -> Self {
        Self {
            height: 1,
            extraction_masks: [0; 4], // 无 discriminative bits
            sparse_partial_keys: [0; 32], // sparse key = 0
            children: vec![ChildRef::Leaf(leaf_id)],
        }
    }

    /// 创建两叶子节点
    ///
    /// 需要传入两个已存储的叶子的 NodeId 和它们的 key（用于计算 diff bit）
    pub fn two_leaves(
        key1: &[u8; 32],
        leaf_id1: NodeId,
        key2: &[u8; 32],
        leaf_id2: NodeId,
    ) -> Self {
        let diff_bit = find_first_differing_bit(key1, key2).expect("keys must be different");

        let bit1 = extract_bit(key1, diff_bit);

        // 确保 bit=0 的在前，保持排序
        let (id_first, id_second) = if !bit1 {
            (leaf_id1, leaf_id2)
        } else {
            (leaf_id2, leaf_id1)
        };

        let mut sparse_partial_keys = [0u32; 32];
        sparse_partial_keys[0] = 0; // bit = 0
        sparse_partial_keys[1] = 1; // bit = 1

        Self {
            height: 1, // 只包含叶子指针的节点 height = 1
            extraction_masks: Self::masks_from_bits(&[diff_bit]),
            sparse_partial_keys,
            children: vec![ChildRef::Leaf(id_first), ChildRef::Leaf(id_second)],
        }
    }

    // ========================================================================
    // Mask 转换
    // ========================================================================

    /// 从 extraction_masks 反推 discriminative bits
    ///
    /// 使用 MSB-first 约定：bit 0 是 key[0] 的 MSB
    pub fn discriminative_bits(&self) -> Vec<u16> {
        let mut bits = Vec::with_capacity(32);
        for (chunk, &mask) in self.extraction_masks.iter().enumerate() {
            let base = (chunk * 64) as u16;
            let mut m = mask;
            while m != 0 {
                // u64 bit position (0 = LSB, 63 = MSB)
                let u64_pos = m.trailing_zeros() as u16;
                // 转换为 key bit position (0 = MSB of byte 0)
                let key_pos = 63 - u64_pos;
                bits.push(base + key_pos);
                m &= m - 1;
            }
        }
        // 按 key bit position 排序
        bits.sort();
        bits
    }

    /// 从 discriminative_bits 构造 extraction_masks
    ///
    /// 使用 MSB-first 约定：bit 0 是 key[0] 的 MSB
    /// 与 from_be_bytes 加载的 u64 配合使用
    pub fn masks_from_bits(bits: &[u16]) -> [u64; 4] {
        let mut masks = [0u64; 4];
        for &bit in bits {
            let chunk = (bit / 64) as usize;
            let pos_in_chunk = bit % 64;
            // 转换：key bit N → u64 bit (63 - N)
            // 因为 from_be_bytes 使 key[0] 成为 u64 的 MSB
            masks[chunk] |= 1u64 << (63 - pos_in_chunk);
        }
        masks
    }

    // ========================================================================
    // Insert 操作（Copy-on-Write）
    // ========================================================================

    /// Normal Insert: 添加新 entry，返回新节点
    ///
    /// 遵循 Copy-on-Write 原则：不修改 self，返回新节点。
    ///
    /// # 参数
    /// - `new_bit`: 新的 discriminative bit 位置（0-255）
    /// - `new_bit_value`: 新 key 在该 bit 位置的值（true=1, false=0）
    /// - `affected_index`: 受影响的 entry index（与新 key 共享前缀）
    /// - `child`: 新的 ChildRef（叶子或内部节点）
    ///
    /// # Panics
    /// - 如果节点已满（debug 模式）
    pub fn with_new_entry(
        &self,
        new_bit: u16,
        new_bit_value: bool,
        affected_index: usize,
        child: ChildRef,
    ) -> Self {
        debug_assert!(!self.is_full(), "Cannot add entry to full node");
        debug_assert!(affected_index < self.len(), "affected_index out of bounds");

        // 创建副本
        let mut new_node = self.clone();

        // Step 1: 检查是否需要添加新的 discriminative bit
        let bit_chunk = (new_bit / 64) as usize;
        let bit_in_chunk = new_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk; // MSB-first 转换
        let bit_mask = 1u64 << u64_bit_pos;
        let is_new_bit = (new_node.extraction_masks[bit_chunk] & bit_mask) == 0;

        // Step 2: 如果是新 bit，更新 extraction_masks 并重编码 sparse keys
        let new_bit_mask: u32 = if is_new_bit {
            // 先添加到 extraction_masks（这样 get_mask_for_bit 才能工作）
            new_node.extraction_masks[bit_chunk] |= bit_mask;

            // 获取新 bit 在 sparse key 中的 mask
            let new_bit_mask = new_node.get_mask_for_bit(new_bit);

            // 基于 mask 计算 PDEP deposit mask（替代 compute_deposit_mask）
            // deposit_mask 的作用：在 new_bit_mask 位置留一个 0，其余保持
            let old_all_bits = self.get_all_mask_bits();
            let low_mask = new_bit_mask - 1; // new_bit_mask 之前的位
            let high_mask = old_all_bits & !low_mask; // new_bit_mask 及之后的位
            let deposit_mask = (high_mask << 1) | low_mask;

            // 使用 PDEP 重编码所有现有 sparse keys
            for i in 0..new_node.len() {
                new_node.sparse_partial_keys[i] =
                    crate::bits::pdep32(new_node.sparse_partial_keys[i], deposit_mask);
            }

            new_bit_mask
        } else {
            // bit 已存在，直接获取其 mask
            new_node.get_mask_for_bit(new_bit)
        };

        // Step 3: 计算新 entry 的 sparse partial key
        let affected_sparse = new_node.sparse_partial_keys[affected_index];
        let new_sparse_key = if new_bit_value {
            affected_sparse | new_bit_mask
        } else {
            affected_sparse & !new_bit_mask
        };

        // Step 4: 如果是新 bit，更新 affected entry 的 sparse key
        if is_new_bit && !new_bit_value {
            // 新 key 的 bit=0，则 affected entry 的 bit=1
            new_node.sparse_partial_keys[affected_index] |= new_bit_mask;
        }
        // 注：如果 new_bit_value=true，affected entry 保持 bit=0（PDEP 已填充 0）

        // Step 5: 找到插入位置（保持 sparse keys 升序）
        let insert_pos = new_node.find_insert_position(new_sparse_key);

        // Step 6: 插入新 entry
        // 6a. 移动 sparse_partial_keys（固定数组，手动移动）
        let old_len = new_node.len();
        for i in (insert_pos..old_len).rev() {
            new_node.sparse_partial_keys[i + 1] = new_node.sparse_partial_keys[i];
        }
        new_node.sparse_partial_keys[insert_pos] = new_sparse_key;

        // 6b. 插入 child（Vec::insert 自动处理）
        new_node.children.insert(insert_pos, child);

        new_node
    }

    // ========================================================================
    // Dense Key 提取（4×PEXT）
    // ========================================================================

    /// 从 U256 key 提取 dense partial key
    ///
    /// 使用 4 次 PEXT 操作，每次处理 64 bits
    #[inline]
    pub fn extract_dense_partial_key(&self, key: &[u8; 32]) -> u32 {
        let mut dense_key = 0u32;
        let mut bit_offset = 0u32;

        for (i, &mask) in self.extraction_masks.iter().enumerate() {
            if mask == 0 {
                continue;
            }

            // 加载对应的 8 字节（big-endian）
            let start = i * 8;
            let key_chunk = u64::from_be_bytes(key[start..start + 8].try_into().unwrap());

            // PEXT 提取这部分的 bits
            let extracted = pext64(key_chunk, mask);
            let bits_count = mask.count_ones();

            // 合并到 dense_key
            dense_key |= (extracted as u32) << bit_offset;
            bit_offset += bits_count;
        }

        dense_key
    }

    // ========================================================================
    // 搜索
    // ========================================================================

    /// 搜索匹配的 entry
    ///
    /// 使用 sparse partial key 匹配逻辑：`(dense & sparse) == sparse`
    pub fn search(&self, key: &[u8; 32]) -> SearchResult {
        let dense_key = self.extract_dense_partial_key(key);
        self.search_with_dense_key(dense_key)
    }

    /// 使用已计算的 dense key 搜索（SIMD 优化）
    #[inline]
    pub fn search_with_dense_key(&self, dense_key: u32) -> SearchResult {
        match simd_search(&self.sparse_partial_keys, dense_key, self.len() as u8) {
            SimdSearchResult::Found(index) => SearchResult::Found { index },
            SimdSearchResult::NotFound => SearchResult::NotFound { dense_key },
        }
    }

    /// 搜索并返回 child
    pub fn search_child(&self, key: &[u8; 32]) -> Option<&ChildRef> {
        match self.search(key) {
            SearchResult::Found { index } => Some(&self.children[index]),
            SearchResult::NotFound { .. } => None,
        }
    }

    // ========================================================================
    // Split 操作
    // ========================================================================

    /// 获取 root bit = 1 的 entries 掩码
    ///
    /// 返回 u32 位掩码，每一位表示对应 entry 的 root bit 是否为 1
    #[cfg(test)]
    fn get_mask_for_larger_entries(&self) -> u32 {
        let root_mask = self.get_root_mask();
        if root_mask == 0 {
            return 0;
        }

        let mut result = 0u32;
        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & root_mask) != 0 {
                result |= 1 << i;
            }
        }
        result
    }

    /// 分裂节点
    ///
    /// 按 first_discriminative_bit 将节点分成两组：
    /// - left: root bit = 0 的 entries
    /// - right: root bit = 1 的 entries
    ///
    /// 返回 (discriminative_bit, left_node, right_node)
    ///
    /// # Panics
    ///
    /// 如果节点 span = 0（无法分裂）
    pub fn split(&self) -> (u16, PersistentHOTNode, PersistentHOTNode) {
        let disc_bit = self
            .first_discriminative_bit()
            .expect("Cannot split node with span=0");
        let root_mask = self.get_root_mask();

        // 收集两组 entries
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();

        for i in 0..self.len() {
            if (self.sparse_partial_keys[i] & root_mask) == 0 {
                left_indices.push(i);
            } else {
                right_indices.push(i);
            }
        }

        // 创建压缩后的子节点
        let left_node = self.compress_entries(&left_indices, disc_bit);
        let right_node = self.compress_entries(&right_indices, disc_bit);

        (disc_bit, left_node, right_node)
    }

    /// 压缩指定 entries 到新节点
    ///
    /// 移除 root_bit 对应的 discriminative bit，重新编码 sparse keys
    ///
    /// # Panics
    ///
    /// 在 debug 模式下，如果 `indices` 为空会 panic。
    /// Split 后两侧必须非空是 HOT 的不变量。
    fn compress_entries(&self, indices: &[usize], removed_bit: u16) -> PersistentHOTNode {
        debug_assert!(
            !indices.is_empty(),
            "HOT invariant violated: split should produce non-empty partitions"
        );
        if indices.is_empty() {
            return PersistentHOTNode::empty(self.height);
        }

        // 单个 entry：与 C++ 一致的 height 语义
        // C++ compressEntries 单 entry 时直接返回原 ChildPointer（不创建新节点）
        // C++ getHeight(): isLeaf() ? 0 : getNode()->mHeight
        if indices.len() == 1 {
            let idx = indices[0];
            let child = &self.children[idx];

            // Leaf 的 "height" = 0（C++ 语义），包装节点 height = 0 + 1 = 1
            // Internal 保守使用 self.height（无法访问 store 查询实际值）
            let height = match child {
                ChildRef::Leaf(_) => 1,
                ChildRef::Internal(_) => self.height,
            };

            let mut node = PersistentHOTNode::empty(height);
            node.children.push(child.clone());
            return node;
        }

        // 计算新的 extraction_masks（移除 removed_bit）
        let chunk = (removed_bit / 64) as usize;
        let bit_in_chunk = removed_bit % 64;
        let u64_bit_pos = 63 - bit_in_chunk;
        let bit_to_remove = 1u64 << u64_bit_pos;

        let mut new_masks = self.extraction_masks;
        new_masks[chunk] &= !bit_to_remove;

        // 计算 compression mask（用于 PEXT 重编码 sparse keys）
        // compression_mask = 移除 root_mask 对应位后的所有位
        let root_sparse_mask = self.get_mask_for_bit(removed_bit);
        let all_bits = self.get_all_mask_bits();
        let compression_mask = all_bits & !root_sparse_mask;

        // 计算新节点 height（与 C++ 一致）
        // - 如果所有选中的 children 都是 Leaf，height = 1
        // - 否则保守使用 self.height（无法访问 store 查询 Internal 节点的实际 height）
        let all_leaves = indices.iter().all(|&idx| {
            matches!(self.children[idx], ChildRef::Leaf(_))
        });
        let height = if all_leaves { 1 } else { self.height };

        // 构建新节点
        let mut new_node = PersistentHOTNode {
            height,
            extraction_masks: new_masks,
            sparse_partial_keys: [0; 32],
            children: Vec::with_capacity(indices.len()),
        };

        for (new_idx, &old_idx) in indices.iter().enumerate() {
            // PEXT 重编码 sparse key
            let old_sparse = self.sparse_partial_keys[old_idx];
            let new_sparse = pext32(old_sparse, compression_mask);
            new_node.sparse_partial_keys[new_idx] = new_sparse;
            new_node.children.push(self.children[old_idx].clone());
        }

        new_node
    }

    // ========================================================================
    // 序列化
    // ========================================================================

    /// 计算节点的 NodeId（content-addressed）
    pub fn compute_node_id<H: Hasher>(&self, version: u64) -> NodeId {
        let bytes = self.to_bytes().expect("Serialization should never fail");
        let hash = H::hash(&bytes);
        make_node_id(version, &hash)
    }

    /// 序列化为字节（用于存储）
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode_config().serialize(self)
    }

    /// 从字节反序列化
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode_config().deserialize(bytes)
    }

    // ========================================================================
    // 验证
    // ========================================================================

    /// 验证节点结构一致性
    pub fn validate(&self) -> Result<(), String> {
        // 1. len 范围检查
        let len = self.len();
        if len > 32 {
            return Err(format!("len {} exceeds maximum 32", len));
        }

        // 2. span 不超过 32
        let span = self.span();
        if span > 32 {
            return Err(format!("span {} exceeds maximum 32", span));
        }

        // 3. height 合理性
        if self.height == 0 {
            return Err("height cannot be 0".to_string());
        }

        Ok(())
    }
}

// ============================================================================
// 位操作辅助函数
// ============================================================================

/// 从字节数组中提取指定位置的 bit
///
/// # 位编码
/// 使用 MSB-first 编码（与 C++ HOT 一致）：
/// - bit 0 = 第 0 字节的 bit 7（最高位）
/// - bit 7 = 第 0 字节的 bit 0（最低位）
/// - bit 8 = 第 1 字节的 bit 7
/// - ...
///
/// # 返回
/// - `true`: bit 为 1
/// - `false`: bit 为 0（或超出 key 范围）
#[inline]
pub fn extract_bit(key: &[u8], bit_pos: u16) -> bool {
    let byte_idx = (bit_pos / 8) as usize;
    let bit_idx = 7 - (bit_pos % 8); // MSB-first

    if byte_idx >= key.len() {
        return false; // 超出范围视为 0
    }

    (key[byte_idx] >> bit_idx) & 1 == 1
}

/// 找到两个 key 的第一个不同 bit 位置
///
/// 对应 C++ 中的 `DiscriminativeBit` 计算。
///
/// # 返回
/// - `Some(bit_pos)`: 第一个不同的 bit 位置
/// - `None`: 两个 key 完全相同
pub fn find_first_differing_bit(key1: &[u8], key2: &[u8]) -> Option<u16> {
    let min_len = key1.len().min(key2.len());

    for i in 0..min_len {
        if key1[i] != key2[i] {
            let xor = key1[i] ^ key2[i];
            let bit_in_byte = xor.leading_zeros() as u16;
            return Some((i as u16) * 8 + bit_in_byte);
        }
    }

    // 检查长度不同的情况
    if key1.len() != key2.len() {
        let longer = if key1.len() > key2.len() { key1 } else { key2 };
        for i in min_len..longer.len() {
            if longer[i] != 0 {
                let bit_in_byte = longer[i].leading_zeros() as u16;
                return Some((i as u16) * 8 + bit_in_byte);
            }
        }
    }

    None
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::{Blake3Hasher, Keccak256Hasher};

    #[test]
    fn test_node_id() {
        let version = 42u64;
        let hash = [0xABu8; 32];
        let id = make_node_id(version, &hash);

        assert_eq!(node_id_version(&id), version);
        assert_eq!(node_id_hash(&id), hash);
        assert_eq!(id.len(), NODE_ID_SIZE);
    }

    #[test]
    fn test_valid_mask() {
        // len = 0
        let node = PersistentHOTNode::empty(1);
        assert_eq!(node.valid_mask(), 0b0);

        // len = 1
        let node = PersistentHOTNode::single_leaf([0u8; NODE_ID_SIZE]);
        assert_eq!(node.valid_mask(), 0b1);

        // len = 2
        let mut key1 = [0u8; 32];
        let mut key2 = [0u8; 32];
        key1[0] = 0b0000_0000;
        key2[0] = 0b0000_0001;
        let node = PersistentHOTNode::two_leaves(&key1, [1u8; NODE_ID_SIZE], &key2, [2u8; NODE_ID_SIZE]);
        assert_eq!(node.valid_mask(), 0b11);
    }

    #[test]
    fn test_masks_conversion() {
        // MSB-first 约定：bit N → u64 bit (63 - N % 64)
        let bits = vec![3, 7, 65, 130];
        let masks = PersistentHOTNode::masks_from_bits(&bits);

        // bit 3 → u64 bit 60, bit 7 → u64 bit 56
        assert_eq!(masks[0], (1u64 << 60) | (1u64 << 56));
        // bit 65 = 64 + 1 → u64 bit 62
        assert_eq!(masks[1], 1u64 << 62);
        // bit 130 = 128 + 2 → u64 bit 61
        assert_eq!(masks[2], 1u64 << 61);
        assert_eq!(masks[3], 0);

        let node = PersistentHOTNode {
            extraction_masks: masks,
            height: 1,
            sparse_partial_keys: [0; 32],
            children: Vec::new(),
        };
        assert_eq!(node.discriminative_bits(), bits);
        assert_eq!(node.span(), 4);
    }

    #[test]
    fn test_extract_bit() {
        // key = [0b10110100, 0b01001011]
        let key = [0b10110100u8, 0b01001011u8];

        assert!(extract_bit(&key, 0)); // MSB of byte 0 = 1
        assert!(!extract_bit(&key, 1)); // = 0
        assert!(extract_bit(&key, 2)); // = 1
        assert!(!extract_bit(&key, 7)); // LSB of byte 0 = 0
        assert!(!extract_bit(&key, 8)); // MSB of byte 1 = 0
        assert!(extract_bit(&key, 9)); // = 1
    }

    #[test]
    fn test_find_first_differing_bit() {
        assert_eq!(find_first_differing_bit(&[0x12], &[0x12]), None);

        let key1 = [0b10000000u8];
        let key2 = [0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(0));

        let key1 = [0b00000001u8];
        let key2 = [0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(7));

        let key1 = [0x00, 0b10000000u8];
        let key2 = [0x00, 0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(8));
    }

    #[test]
    fn test_search_result() {
        let found = SearchResult::Found { index: 5 };
        assert!(found.is_found());
        assert_eq!(found.found_index(), Some(5));

        let not_found = SearchResult::NotFound { dense_key: 42 };
        assert!(!not_found.is_found());
        assert_eq!(not_found.found_index(), None);
    }

    #[test]
    fn test_leaf_data() {
        let key = [0xABu8; 32];
        let value = b"test value".to_vec();
        let leaf = LeafData::new(key, value.clone());

        assert_eq!(leaf.key, key);
        assert_eq!(leaf.value, value);

        // 序列化往返测试
        let bytes = leaf.to_bytes().unwrap();
        let decoded = LeafData::from_bytes(&bytes).unwrap();
        assert_eq!(leaf, decoded);
    }

    #[test]
    fn test_child_ref() {
        let id = [0u8; NODE_ID_SIZE];

        let leaf = ChildRef::Leaf(id);
        assert!(leaf.is_leaf());
        assert!(!leaf.is_internal());
        assert_eq!(leaf.node_id(), &id);
        assert_eq!(leaf.height_if_leaf(), Some(1));

        let internal = ChildRef::Internal(id);
        assert!(!internal.is_leaf());
        assert!(internal.is_internal());
        assert_eq!(internal.node_id(), &id);
        assert_eq!(internal.height_if_leaf(), None);
    }

    #[test]
    fn test_node_serialization_determinism() {
        let mut node = PersistentHOTNode::empty(3);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[0, 3, 7, 15]);
        node.sparse_partial_keys[0] = 0b0000;
        node.sparse_partial_keys[1] = 0b1010;
        node.children.push(ChildRef::Leaf([0xAAu8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Internal([0xBBu8; NODE_ID_SIZE]));

        // 序列化两次应该得到相同字节
        let bytes1 = node.to_bytes().unwrap();
        let bytes2 = node.to_bytes().unwrap();
        assert_eq!(bytes1, bytes2, "Serialization should be deterministic");

        // 反序列化应该恢复原始数据
        let decoded = PersistentHOTNode::from_bytes(&bytes1).unwrap();
        assert_eq!(node, decoded, "Round-trip should preserve data");
    }

    #[test]
    fn test_compute_node_id_determinism() {
        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[5]);
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        node.children.push(ChildRef::Leaf([0x11u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([0x22u8; NODE_ID_SIZE]));

        let version = 100u64;

        // 相同节点计算 ID 两次应该相同
        let id1 = node.compute_node_id::<Blake3Hasher>(version);
        let id2 = node.compute_node_id::<Blake3Hasher>(version);
        assert_eq!(id1, id2, "NodeId should be deterministic");

        // 验证 version 被包含在 ID 中
        assert_eq!(node_id_version(&id1), version);

        // 不同 version 产生不同 ID
        let id_v200 = node.compute_node_id::<Blake3Hasher>(200);
        assert_ne!(id1, id_v200, "Different versions should produce different IDs");

        // 不同哈希函数应产生不同 ID
        let blake3_id = node.compute_node_id::<Blake3Hasher>(version);
        let keccak_id = node.compute_node_id::<Keccak256Hasher>(version);
        assert_ne!(
            blake3_id, keccak_id,
            "Different hashers should produce different IDs"
        );
    }

    #[test]
    fn test_validate_valid_node() {
        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0;
        node.sparse_partial_keys[1] = 1;
        node.children.push(ChildRef::Leaf([0u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([1u8; NODE_ID_SIZE]));

        assert!(node.validate().is_ok());
    }

    #[test]
    fn test_validate_too_many_children() {
        let mut node = PersistentHOTNode::empty(2);
        // 添加 33 个 children 超过限制
        for i in 0..33 {
            node.children.push(ChildRef::Leaf([i as u8; NODE_ID_SIZE]));
        }

        assert!(node.validate().is_err());
    }

    #[test]
    fn test_validate_height_zero() {
        let mut node = PersistentHOTNode::empty(1);
        node.height = 0;

        assert!(node.validate().is_err());
    }

    #[test]
    fn test_two_leaves() {
        let mut key1 = [0u8; 32];
        let mut key2 = [0u8; 32];
        key1[0] = 0b0000_0000; // bit 7 = 0
        key2[0] = 0b0000_0001; // bit 7 = 1

        // 创建叶子数据
        let leaf1 = LeafData::new(key1, b"value1".to_vec());
        let leaf2 = LeafData::new(key2, b"value2".to_vec());
        let id1 = leaf1.compute_node_id::<Blake3Hasher>(0);
        let id2 = leaf2.compute_node_id::<Blake3Hasher>(0);

        let node = PersistentHOTNode::two_leaves(&key1, id1, &key2, id2);

        assert_eq!(node.len(), 2);
        assert_eq!(node.height, 1); // 只包含叶子指针的节点 height = 1
        assert_eq!(node.span(), 1);
        assert_eq!(node.sparse_partial_keys[0], 0);
        assert_eq!(node.sparse_partial_keys[1], 1);
        assert!(node.validate().is_ok());
    }

    #[test]
    fn test_search() {
        // 创建一个简单的两叶子节点
        // discriminative_bit = 3
        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3]);
        node.sparse_partial_keys[0] = 0; // bit 3 = 0
        node.sparse_partial_keys[1] = 1; // bit 3 = 1
        node.children.push(ChildRef::Leaf([0u8; NODE_ID_SIZE]));
        node.children.push(ChildRef::Leaf([1u8; NODE_ID_SIZE]));

        // 搜索 bit 3 = 0 的 key
        let mut search_key = [0u8; 32];
        search_key[0] = 0b0000_0000; // bit 3 = 0
        assert_eq!(node.search(&search_key).found_index(), Some(0));

        // 搜索 bit 3 = 1 的 key
        search_key[0] = 0b0001_0000; // bit 3 = 1
        assert_eq!(node.search(&search_key).found_index(), Some(1));
    }

    #[test]
    fn test_search_sparse_matching() {
        // 测试 sparse 匹配逻辑：(dense & sparse) == sparse
        //
        // 模拟一个节点：
        // discriminative bits: [0, 4]（bit0 在 dense key 的低位，bit4 在高位）
        //
        //         bit0
        //        /    \
        //     0 /      \ 1
        //      /        \
        //   bit4      [Leaf A] sparse=0b01
        //  /    \
        // 0      1
        // [B]   [C]
        // 0b00  0b10
        //
        // HOT 要求 entries 按 key/trie 遍历顺序排列：B, C, A
        // 这样 sparse 匹配时取最后一个匹配才是正确的

        let mut node = PersistentHOTNode::empty(2);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[0, 4]);
        // 按 trie 遍历顺序：左子树先于右子树
        node.sparse_partial_keys[0] = 0b00; // B: bit0=0, bit4=0
        node.sparse_partial_keys[1] = 0b10; // C: bit0=0, bit4=1
        node.sparse_partial_keys[2] = 0b01; // A: bit0=1, bit4=don't care
        node.children.push(ChildRef::Leaf([0u8; NODE_ID_SIZE])); // B
        node.children.push(ChildRef::Leaf([1u8; NODE_ID_SIZE])); // C
        node.children.push(ChildRef::Leaf([2u8; NODE_ID_SIZE])); // A

        // dense=0b01 (bit0=1, bit4=0) → 匹配 A（最后一个匹配）
        assert_eq!(node.search_with_dense_key(0b01).found_index(), Some(2));

        // dense=0b11 (bit0=1, bit4=1) → 匹配 A（bit4 是 don't care，取最后匹配）
        assert_eq!(node.search_with_dense_key(0b11).found_index(), Some(2));

        // dense=0b00 → 只匹配 B
        assert_eq!(node.search_with_dense_key(0b00).found_index(), Some(0));

        // dense=0b10 → 匹配 B 和 C，选最后一个 C
        assert_eq!(node.search_with_dense_key(0b10).found_index(), Some(1));
    }

    #[test]
    fn test_extract_dense_partial_key() {
        // 测试 4×PEXT
        let bits = vec![7, 65]; // bit 7 在 chunk 0，bit 65 在 chunk 1
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&bits);

        let mut key = [0u8; 32];
        // bit 7 在 byte 0 的 LSB 位置
        key[0] = 0b0000_0001; // bit 7 = 1
        // bit 65 在 byte 8 的 bit 6 位置 (65 = 64 + 1, 在 chunk 1 的 bit 1)
        key[8] = 0b0100_0000; // bit 65 = 1

        let dense = node.extract_dense_partial_key(&key);
        assert_eq!(dense, 0b11); // 两个 bit 都是 1
    }

    // ========================================================================
    // Bitmask 风格函数测试
    // ========================================================================

    #[test]
    fn test_first_discriminative_bit() {
        // 空节点
        let node = PersistentHOTNode::empty(1);
        assert_eq!(node.first_discriminative_bit(), None);

        // 单 bit: key bit 3
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3]);
        assert_eq!(node.first_discriminative_bit(), Some(3));

        // 多个 bits: [3, 7, 100]，应返回最小的 3
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7, 100]);
        assert_eq!(node.first_discriminative_bit(), Some(3));

        // 跨 chunk: [65, 130]，应返回 65
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[65, 130]);
        assert_eq!(node.first_discriminative_bit(), Some(65));

        // 只在 chunk 2: [128, 130]
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[128, 130]);
        assert_eq!(node.first_discriminative_bit(), Some(128));
    }

    #[test]
    fn test_get_all_mask_bits() {
        // span = 0
        let node = PersistentHOTNode::empty(1);
        assert_eq!(node.get_all_mask_bits(), 0);

        // span = 1
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[5]);
        assert_eq!(node.get_all_mask_bits(), 0b1);

        // span = 3
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7, 100]);
        assert_eq!(node.get_all_mask_bits(), 0b111);

        // span = 5
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[0, 1, 2, 3, 4]);
        assert_eq!(node.get_all_mask_bits(), 0b11111);
    }

    #[test]
    fn test_get_mask_for_bit() {
        // 单 bit: key bit 3 → sparse key bit 0
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3]);
        assert_eq!(node.get_mask_for_bit(3), 0b1);
        assert_eq!(node.get_mask_for_bit(5), 0); // 不是 discriminative bit

        // 两个 bits 在同一 chunk: [3, 7]
        // PEXT 从 u64 LSB 开始提取，较大 key bit → 较低 u64 bit → 先提取
        // key bit 7 → u64 bit 56 → sparse key bit 0
        // key bit 3 → u64 bit 60 → sparse key bit 1
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        assert_eq!(node.get_mask_for_bit(7), 0b01); // bit 0
        assert_eq!(node.get_mask_for_bit(3), 0b10); // bit 1
        assert_eq!(node.get_mask_for_bit(5), 0);    // 不存在

        // 三个 bits 跨 chunk: [3, 7, 100]
        // 同 chunk 内：较大 key bit → 较低 sparse bit
        // 跨 chunk：较小 chunk → 较低 sparse bits
        //
        // chunk 0 (bits 3, 7): 7 → sparse bit 0, 3 → sparse bit 1
        // chunk 1 (bit 100): 100 → sparse bit 2
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7, 100]);
        assert_eq!(node.get_mask_for_bit(7), 0b001);   // sparse bit 0
        assert_eq!(node.get_mask_for_bit(3), 0b010);   // sparse bit 1
        assert_eq!(node.get_mask_for_bit(100), 0b100); // sparse bit 2
    }

    #[test]
    fn test_get_root_mask() {
        // span = 0
        let node = PersistentHOTNode::empty(1);
        assert_eq!(node.get_root_mask(), 0);

        // span = 1
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[5]);
        assert_eq!(node.get_root_mask(), 0b1);

        // 同 chunk 两个 bits: [3, 7]
        // first_discriminative_bit = 3 → get_mask_for_bit(3) = 0b10
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        assert_eq!(node.first_discriminative_bit(), Some(3));
        assert_eq!(node.get_root_mask(), 0b10); // NOT 1 << (2-1) = 2

        // 跨 chunk: [3, 7, 100]
        // first_discriminative_bit = 3 → get_mask_for_bit(3) = 0b010
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7, 100]);
        assert_eq!(node.first_discriminative_bit(), Some(3));
        assert_eq!(node.get_root_mask(), 0b010); // NOT 1 << (3-1) = 4

        // 验证 root_mask == get_mask_for_bit(first_discriminative_bit)
        let first_bit = node.first_discriminative_bit().unwrap();
        assert_eq!(node.get_root_mask(), node.get_mask_for_bit(first_bit));

        // 不同 chunk，无共享 byte: [3, 100]
        // first_discriminative_bit = 3 → get_mask_for_bit(3) = 0b01
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 100]);
        assert_eq!(node.first_discriminative_bit(), Some(3));
        assert_eq!(node.get_root_mask(), 0b01); // sparse bit 0
    }

    #[test]
    fn test_bitmask_consistency_with_pext() {
        // 验证 get_mask_for_bit 与实际 PEXT 结果一致
        let bits = vec![3, 7, 100];
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&bits);

        // 构造只有 key bit 3 = 1 的 key
        let mut key = [0u8; 32];
        key[0] = 0b0001_0000; // bit 3 = 1

        let dense = node.extract_dense_partial_key(&key);
        let mask_for_bit3 = node.get_mask_for_bit(3);

        // dense 应该只有 bit 3 对应的位为 1
        assert_eq!(dense, mask_for_bit3);

        // 构造只有 key bit 100 = 1 的 key
        let mut key = [0u8; 32];
        key[12] = 0b0000_1000; // bit 100 = byte 12, bit 4 in byte

        let dense = node.extract_dense_partial_key(&key);
        let mask_for_bit100 = node.get_mask_for_bit(100);

        assert_eq!(dense, mask_for_bit100);
    }

    // ========================================================================
    // Split 测试
    // ========================================================================

    #[test]
    fn test_split_basic() {
        // 创建包含 4 个 entries 的节点
        // discriminative bits: [3, 7]
        // sparse keys: 0b00, 0b01, 0b10, 0b11
        // first_discriminative_bit = 3 → root_mask = 0b10
        //
        // 分裂后：
        // - left (root bit = 0): entries 0, 1 (sparse keys 0b00, 0b01)
        // - right (root bit = 1): entries 2, 3 (sparse keys 0b10, 0b11)
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0b00;
        node.sparse_partial_keys[1] = 0b01;
        node.sparse_partial_keys[2] = 0b10;
        node.sparse_partial_keys[3] = 0b11;
        node.children = vec![
            ChildRef::Leaf(make_node_id(1, &[1; 32])),
            ChildRef::Leaf(make_node_id(1, &[2; 32])),
            ChildRef::Leaf(make_node_id(1, &[3; 32])),
            ChildRef::Leaf(make_node_id(1, &[4; 32])),
        ];

        let (disc_bit, left, right) = node.split();

        // 验证分裂 bit
        assert_eq!(disc_bit, 3); // first_discriminative_bit

        // 验证 left 节点
        assert_eq!(left.len(), 2);
        assert_eq!(left.span(), 1); // 只剩 bit 7
        assert_eq!(left.sparse_partial_keys[0], 0b0); // 压缩后的 0b00
        assert_eq!(left.sparse_partial_keys[1], 0b1); // 压缩后的 0b01
        assert_eq!(left.children[0], ChildRef::Leaf(make_node_id(1, &[1; 32])));
        assert_eq!(left.children[1], ChildRef::Leaf(make_node_id(1, &[2; 32])));

        // 验证 right 节点
        assert_eq!(right.len(), 2);
        assert_eq!(right.span(), 1); // 只剩 bit 7
        assert_eq!(right.sparse_partial_keys[0], 0b0); // 压缩后的 0b10 → 0b0
        assert_eq!(right.sparse_partial_keys[1], 0b1); // 压缩后的 0b11 → 0b1
        assert_eq!(right.children[0], ChildRef::Leaf(make_node_id(1, &[3; 32])));
        assert_eq!(right.children[1], ChildRef::Leaf(make_node_id(1, &[4; 32])));
    }

    #[test]
    fn test_split_unbalanced() {
        // 创建不均匀分布的节点
        // sparse keys: 0b00, 0b01, 0b10
        // root_mask = 0b10 → left 有 2 个，right 有 1 个
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0b00;
        node.sparse_partial_keys[1] = 0b01;
        node.sparse_partial_keys[2] = 0b10;
        node.children = vec![
            ChildRef::Leaf(make_node_id(1, &[1; 32])),
            ChildRef::Leaf(make_node_id(1, &[2; 32])),
            ChildRef::Leaf(make_node_id(1, &[3; 32])),
        ];

        let (disc_bit, left, right) = node.split();

        assert_eq!(disc_bit, 3);
        assert_eq!(left.len(), 2);
        assert_eq!(right.len(), 1);

        // right 只有一个 entry，无 discriminative bits
        assert_eq!(right.span(), 0);
    }

    #[test]
    fn test_get_mask_for_larger_entries() {
        let mut node = PersistentHOTNode::empty(1);
        node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
        node.sparse_partial_keys[0] = 0b00;
        node.sparse_partial_keys[1] = 0b01;
        node.sparse_partial_keys[2] = 0b10;
        node.sparse_partial_keys[3] = 0b11;
        node.children = vec![
            ChildRef::Leaf(make_node_id(1, &[1; 32])),
            ChildRef::Leaf(make_node_id(1, &[2; 32])),
            ChildRef::Leaf(make_node_id(1, &[3; 32])),
            ChildRef::Leaf(make_node_id(1, &[4; 32])),
        ];

        // root_mask = 0b10
        // entries 2 和 3 的 root bit = 1
        let mask = node.get_mask_for_larger_entries();
        assert_eq!(mask, 0b1100); // bit 2 和 3 为 1
    }
}
