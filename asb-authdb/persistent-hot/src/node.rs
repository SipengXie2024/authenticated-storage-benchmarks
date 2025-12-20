//! HOT 节点核心数据结构
//!
//! 本模块定义了持久化 HOT 的核心类型：
//! - `PersistentHOTNode`: HOT 复合节点的持久化表示
//! - `ChildRef`: 子节点引用（内部节点 ID 或叶子数据）
//! - `NodeId`: 节点标识符（32字节哈希）
//!
//! # 与 C++ HOT 的对应关系
//!
//! | C++ 原版 | Rust 持久化版 | 说明 |
//! |----------|---------------|------|
//! | `BiNode` | 隐含在 `discriminative_bits` | 每个 bit 位置对应一个逻辑 BiNode |
//! | `SparsePartialKeys` | `sparse_partial_keys: Vec<u32>` | 稀疏部分键数组 |
//! | `ChildPointer[]` | `children: Vec<ChildRef>` | 子节点引用数组 |
//! | Node height | `height: u16` | 节点高度 |
//!
//! # 设计决策
//!
//! 1. **放弃 SIMD 优化**：持久化场景下 I/O 时间主导，SIMD 节省的 ~1ns 可忽略
//! 2. **使用 `Vec` 而非固定数组**：简化序列化，保持灵活性
//! 3. **排序存储**：`sparse_partial_keys` 按值排序，确保序列化确定性
//! 4. **Content-Addressed**：节点 ID = 节点内容的哈希

use bincode::Options;
use serde::{Deserialize, Serialize};

use crate::hash::Hasher;

/// 节点标识符：32字节哈希值
///
/// 在 content-addressed 存储中，NodeId 是节点序列化内容的哈希。
/// 相同内容的节点具有相同的 NodeId，实现自动去重。
///
/// 后续 Merkle 化时，NodeId 将作为节点的 commitment。
pub type NodeId = [u8; 32];

/// HOT 节点的持久化表示
///
/// 对应论文中的 Compound Node（复合节点），最多包含 k=32 个 entries。
///
/// # 核心概念
///
/// ## Discriminative Bits
/// `discriminative_bits` 存储需要检查的 bit 位置（绝对索引）。
/// 例如 `[3, 7, 12]` 表示检查 key 的第 3、7、12 位。
/// 这决定了节点的 "span"（跨度）。
///
/// ## Sparse Partial Keys
/// 每个 child 对应一个 sparse partial key。
/// Sparse 的含义：只有路径上的 bit 位置有效，其他位为 0。
/// 这与 C++ 版本的 `SparsePartialKeys` 数组对应。
///
/// ## 搜索逻辑
/// 给定 search key，提取其 dense partial key（所有 discriminative bits 的值），
/// 然后找到满足 `(dense & sparse) == sparse` 的 entry。
///
/// # 不变量
///
/// 1. `sparse_partial_keys.len() == children.len()`
/// 2. `children.len() <= 32`（HOT 的 fanout 限制）
/// 3. `discriminative_bits` 已排序且无重复
/// 4. `sparse_partial_keys` 已排序（确保序列化确定性）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentHOTNode {
    /// 节点高度
    ///
    /// 定义：h(n) = max(h(children)) + 1
    /// 叶子节点的 height = 1
    ///
    /// 高度用于判断插入策略：
    /// - Parent Pull Up: h(n) + 1 == h(parent)
    /// - Intermediate Node Creation: h(n) + 1 < h(parent)
    pub height: u16,

    /// 判别位索引列表（绝对 bit 位置，从 0 开始）
    ///
    /// 例如：key = [0x12, 0x34, ...]
    /// - bit 0-7 在第 0 字节 (0x12)
    /// - bit 8-15 在第 1 字节 (0x34)
    /// - bit 3 = 0 (0x12 的第 4 位，MSB first)
    ///
    /// 最多 31 个（对应 32 个 children，需要 31 个二分节点）
    pub discriminative_bits: Vec<u16>,

    /// 稀疏部分键数组
    ///
    /// 每个元素是对应 child 的 sparse partial key。
    /// u32 足够存储最多 31 个 discriminative bits。
    ///
    /// **必须按值排序**以确保序列化确定性。
    pub sparse_partial_keys: Vec<u32>,

    /// 子节点引用数组
    ///
    /// 与 `sparse_partial_keys` 一一对应。
    /// 顺序必须与 `sparse_partial_keys` 的排序保持一致。
    pub children: Vec<ChildRef>,
}

/// 子节点引用：内部节点 ID 或叶子数据
///
/// # 设计说明
///
/// - `Internal(NodeId)`：指向另一个 HOT 节点
/// - `Leaf { key, value }`：直接存储键值对
///
/// 选择存储完整 key（而非 suffix）是为了简化实现：
/// - 查找时可直接验证 key 完全匹配
/// - 无需沿路径收集前缀
/// - 对于短 key，存储开销可接受
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChildRef {
    /// 内部节点引用（存储节点 ID）
    Internal(NodeId),

    /// 叶子节点（直接存储完整键值对）
    Leaf { key: Vec<u8>, value: Vec<u8> },
}

impl ChildRef {
    /// 检查是否为叶子节点
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, ChildRef::Leaf { .. })
    }

    /// 检查是否为内部节点
    #[inline]
    pub fn is_internal(&self) -> bool {
        matches!(self, ChildRef::Internal(_))
    }

    /// 尝试获取叶子数据
    pub fn as_leaf(&self) -> Option<(&[u8], &[u8])> {
        match self {
            ChildRef::Leaf { key, value } => Some((key, value)),
            ChildRef::Internal(_) => None,
        }
    }

    /// 尝试获取内部节点 ID
    pub fn as_internal(&self) -> Option<&NodeId> {
        match self {
            ChildRef::Internal(id) => Some(id),
            ChildRef::Leaf { .. } => None,
        }
    }

    /// 获取子节点的高度
    ///
    /// - 叶子节点：高度 = 1
    /// - 内部节点：需要从存储中读取（此处返回 None）
    pub fn height_if_leaf(&self) -> Option<u16> {
        match self {
            ChildRef::Leaf { .. } => Some(1),
            ChildRef::Internal(_) => None,
        }
    }
}

/// 创建确定性 bincode 配置
///
/// 保证：
/// 1. 固定字节序（little-endian）
/// 2. 固定整数编码（fixint，不使用变长编码）
/// 3. 允许尾部字节（兼容性）
///
/// 这确保相同的节点内容总是产生相同的字节序列。
fn bincode_config() -> impl bincode::Options {
    bincode::options()
        .with_little_endian()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

impl PersistentHOTNode {
    /// 创建空节点（仅用于测试）
    pub fn empty() -> Self {
        Self {
            height: 1,
            discriminative_bits: Vec::new(),
            sparse_partial_keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// 创建只包含单个叶子的节点
    pub fn single_leaf(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            height: 2, // 包含叶子的节点高度为 2
            discriminative_bits: Vec::new(),
            sparse_partial_keys: vec![0], // 单个 entry 的 sparse key 为 0
            children: vec![ChildRef::Leaf { key, value }],
        }
    }

    /// 创建包含两个叶子的节点
    ///
    /// # 参数
    /// - `discriminative_bit`: 区分两个叶子的 bit 位置
    /// - `leaf1`, `leaf2`: 两个叶子（按 sparse key 排序后存储）
    ///
    /// # 返回
    /// 新创建的节点
    pub fn two_leaves(
        discriminative_bit: u16,
        key1: Vec<u8>,
        value1: Vec<u8>,
        key2: Vec<u8>,
        value2: Vec<u8>,
    ) -> Self {
        // 提取 discriminative bit 的值
        let bit1 = extract_bit(&key1, discriminative_bit);
        let bit2 = extract_bit(&key2, discriminative_bit);

        // 根据 bit 值决定顺序（bit=0 在前，bit=1 在后）
        let (sparse_keys, children) = if bit1 <= bit2 {
            (
                vec![0, 1], // sparse keys: 0 for left, 1 for right
                vec![
                    ChildRef::Leaf {
                        key: key1,
                        value: value1,
                    },
                    ChildRef::Leaf {
                        key: key2,
                        value: value2,
                    },
                ],
            )
        } else {
            (
                vec![0, 1],
                vec![
                    ChildRef::Leaf {
                        key: key2,
                        value: value2,
                    },
                    ChildRef::Leaf {
                        key: key1,
                        value: value1,
                    },
                ],
            )
        };

        Self {
            height: 2, // 两个叶子的父节点高度为 2
            discriminative_bits: vec![discriminative_bit],
            sparse_partial_keys: sparse_keys,
            children,
        }
    }

    /// 计算节点的 NodeId（content-addressed）
    ///
    /// # 流程
    /// 1. 使用 bincode 确定性序列化
    /// 2. 对序列化字节计算哈希
    /// 3. 返回 32 字节哈希值作为 NodeId
    ///
    /// # 类型参数
    /// - `H`: 实现 `Hasher` trait 的哈希算法（Blake3 或 Keccak256）
    pub fn compute_node_id<H: Hasher>(&self) -> NodeId {
        let bytes = self.to_bytes().expect("Serialization should never fail");
        H::hash(&bytes)
    }

    /// 序列化为字节（用于存储）
    pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode_config().serialize(self)
    }

    /// 从字节反序列化
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode_config().deserialize(bytes)
    }

    /// 验证节点内部一致性
    ///
    /// 检查所有不变量是否满足。
    pub fn validate(&self) -> Result<(), String> {
        // 1. 长度一致性
        if self.sparse_partial_keys.len() != self.children.len() {
            return Err(format!(
                "Length mismatch: {} sparse_partial_keys vs {} children",
                self.sparse_partial_keys.len(),
                self.children.len()
            ));
        }

        // 2. Fanout 限制
        if self.children.len() > 32 {
            return Err(format!(
                "Too many children: {} (max 32)",
                self.children.len()
            ));
        }

        // 3. Discriminative bits 排序
        if !self
            .discriminative_bits
            .windows(2)
            .all(|w| w[0] < w[1])
        {
            return Err("discriminative_bits not strictly sorted".to_string());
        }

        // 4. Sparse partial keys 排序（确保序列化确定性）
        if !self
            .sparse_partial_keys
            .windows(2)
            .all(|w| w[0] <= w[1])
        {
            return Err("sparse_partial_keys not sorted".to_string());
        }

        // 5. Height 合理性
        if self.height == 0 {
            return Err("Height cannot be 0".to_string());
        }

        Ok(())
    }

    /// 检查节点是否已满（需要分裂）
    #[inline]
    pub fn is_full(&self) -> bool {
        self.children.len() >= 32
    }

    /// 获取子节点数量
    #[inline]
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// 检查是否为空节点
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// 从 key 中提取 dense partial key
    ///
    /// Dense partial key 包含所有 discriminative bits 的值，
    /// 用于在节点中搜索匹配的 child。
    ///
    /// # 位提取规则
    /// bit 位置使用 MSB-first 编码：
    /// - bit 0 = 第 0 字节的最高位
    /// - bit 7 = 第 0 字节的最低位
    /// - bit 8 = 第 1 字节的最高位
    /// - ...
    pub fn extract_dense_partial_key(&self, key: &[u8]) -> u32 {
        let mut partial_key = 0u32;
        for (i, &bit_pos) in self.discriminative_bits.iter().enumerate() {
            if extract_bit(key, bit_pos) {
                partial_key |= 1 << i;
            }
        }
        partial_key
    }

    /// 在节点中搜索匹配的 child
    ///
    /// 使用 sparse partial key 匹配逻辑：`(dense & sparse) == sparse`
    ///
    /// # 返回
    /// - `Some(index)`: 找到匹配的 child 索引
    /// - `None`: 无匹配（理论上不应发生，除非数据损坏）
    pub fn search(&self, key: &[u8]) -> Option<usize> {
        let dense_key = self.extract_dense_partial_key(key);

        for (i, &sparse_key) in self.sparse_partial_keys.iter().enumerate() {
            if (dense_key & sparse_key) == sparse_key {
                return Some(i);
            }
        }

        None
    }
}

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
            // 找到第一个不同的字节
            let xor = key1[i] ^ key2[i];
            // 找到该字节中第一个不同的 bit（从高位开始）
            let bit_in_byte = xor.leading_zeros() as u16 - 24; // u8 的 leading_zeros 从 32 开始
            return Some((i as u16) * 8 + (7 - bit_in_byte));
        }
    }

    // 检查长度不同的情况
    if key1.len() != key2.len() {
        // 较短 key 后面的位视为 0，较长 key 的第一个非零位就是 differing bit
        let longer = if key1.len() > key2.len() { key1 } else { key2 };
        for i in min_len..longer.len() {
            if longer[i] != 0 {
                let bit_in_byte = longer[i].leading_zeros() as u16 - 24;
                return Some((i as u16) * 8 + (7 - bit_in_byte));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::{Blake3Hasher, Keccak256Hasher};

    #[test]
    fn test_extract_bit() {
        // key = [0b10110100, 0b01001011]
        //        ^^^^^^^^    ^^^^^^^^
        //        bits 0-7    bits 8-15
        let key = [0b10110100u8, 0b01001011u8];

        // Bit 0 = MSB of byte 0 = 1
        assert!(extract_bit(&key, 0));
        // Bit 1 = 0
        assert!(!extract_bit(&key, 1));
        // Bit 2 = 1
        assert!(extract_bit(&key, 2));
        // Bit 7 = LSB of byte 0 = 0
        assert!(!extract_bit(&key, 7));
        // Bit 8 = MSB of byte 1 = 0
        assert!(!extract_bit(&key, 8));
        // Bit 9 = 1
        assert!(extract_bit(&key, 9));
    }

    #[test]
    fn test_find_first_differing_bit() {
        // 相同 key
        assert_eq!(find_first_differing_bit(&[0x12], &[0x12]), None);

        // 第一个 bit 不同
        let key1 = [0b10000000u8];
        let key2 = [0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(0));

        // 第 7 个 bit 不同
        let key1 = [0b00000001u8];
        let key2 = [0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(7));

        // 第二个字节中的 bit 不同
        let key1 = [0x00, 0b10000000u8];
        let key2 = [0x00, 0b00000000u8];
        assert_eq!(find_first_differing_bit(&key1, &key2), Some(8));
    }

    #[test]
    fn test_node_serialization_determinism() {
        let node = PersistentHOTNode {
            height: 3,
            discriminative_bits: vec![0, 3, 7, 15],
            sparse_partial_keys: vec![0b0000, 0b1010],
            children: vec![
                ChildRef::Leaf {
                    key: vec![0x01, 0x02],
                    value: vec![0xAA, 0xBB],
                },
                ChildRef::Internal([0u8; 32]),
            ],
        };

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
        let node = PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![5],
            sparse_partial_keys: vec![0, 1],
            children: vec![
                ChildRef::Leaf {
                    key: b"test1".to_vec(),
                    value: b"value1".to_vec(),
                },
                ChildRef::Leaf {
                    key: b"test2".to_vec(),
                    value: b"value2".to_vec(),
                },
            ],
        };

        // 相同节点计算 ID 两次应该相同
        let id1 = node.compute_node_id::<Blake3Hasher>();
        let id2 = node.compute_node_id::<Blake3Hasher>();
        assert_eq!(id1, id2, "NodeId should be deterministic");

        // 不同哈希函数应产生不同 ID
        let blake3_id = node.compute_node_id::<Blake3Hasher>();
        let keccak_id = node.compute_node_id::<Keccak256Hasher>();
        assert_ne!(blake3_id, keccak_id, "Different hashers should produce different IDs");
    }

    #[test]
    fn test_validate_valid_node() {
        let node = PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![3, 7],
            sparse_partial_keys: vec![0, 1],
            children: vec![
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
            ],
        };
        assert!(node.validate().is_ok());
    }

    #[test]
    fn test_validate_length_mismatch() {
        let node = PersistentHOTNode {
            height: 1,
            discriminative_bits: vec![],
            sparse_partial_keys: vec![0],
            children: vec![
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
            ],
        };
        assert!(node.validate().is_err());
    }

    #[test]
    fn test_validate_too_many_children() {
        let node = PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![],
            sparse_partial_keys: vec![0; 33],
            children: vec![
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                };
                33
            ],
        };
        assert!(node.validate().is_err());
    }

    #[test]
    fn test_validate_unsorted_discriminative_bits() {
        let node = PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![7, 3], // 未排序
            sparse_partial_keys: vec![0, 1],
            children: vec![
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
                ChildRef::Leaf {
                    key: vec![],
                    value: vec![],
                },
            ],
        };
        assert!(node.validate().is_err());
    }

    #[test]
    fn test_search() {
        // 创建一个简单的两叶子节点
        // discriminative_bit = 3
        // key1 = 0b0000_0000 (bit 3 = 0) -> sparse_key = 0
        // key2 = 0b0001_0000 (bit 3 = 1) -> sparse_key = 1
        let node = PersistentHOTNode {
            height: 2,
            discriminative_bits: vec![3],
            sparse_partial_keys: vec![0, 1],
            children: vec![
                ChildRef::Leaf {
                    key: vec![0b0000_0000],
                    value: b"value0".to_vec(),
                },
                ChildRef::Leaf {
                    key: vec![0b0001_0000],
                    value: b"value1".to_vec(),
                },
            ],
        };

        // 搜索 bit 3 = 0 的 key
        let search_key = [0b0000_0000u8];
        assert_eq!(node.search(&search_key), Some(0));

        // 搜索 bit 3 = 1 的 key
        let search_key = [0b0001_0000u8];
        assert_eq!(node.search(&search_key), Some(1));
    }

    #[test]
    fn test_child_ref_methods() {
        let leaf = ChildRef::Leaf {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };
        assert!(leaf.is_leaf());
        assert!(!leaf.is_internal());
        assert!(leaf.as_leaf().is_some());
        assert!(leaf.as_internal().is_none());
        assert_eq!(leaf.height_if_leaf(), Some(1));

        let internal = ChildRef::Internal([0u8; 32]);
        assert!(!internal.is_leaf());
        assert!(internal.is_internal());
        assert!(internal.as_leaf().is_none());
        assert!(internal.as_internal().is_some());
        assert_eq!(internal.height_if_leaf(), None);
    }

    #[test]
    fn test_two_leaves_constructor() {
        let node = PersistentHOTNode::two_leaves(
            3, // discriminative bit
            vec![0b0000_0000],
            b"value0".to_vec(),
            vec![0b0001_0000],
            b"value1".to_vec(),
        );

        assert_eq!(node.height, 2);
        assert_eq!(node.discriminative_bits, vec![3]);
        assert_eq!(node.children.len(), 2);
        assert!(node.validate().is_ok());
    }
}
