//! PersistentHOTNode 核心结构体

use bincode::Options;
use serde::{Deserialize, Serialize};

use super::types::{bincode_config, make_node_id, ChildRef, NodeId};
use super::utils::{extract_bit, find_first_differing_bit};
use crate::hash::Hasher;

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
