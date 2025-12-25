//! node 模块测试

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
    assert_eq!(leaf.height_if_leaf(), Some(0)); // C++ 语义：leaf height = 0

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
    // discriminative bits: [0, 4]
    //
    // PEXT 位序说明（参考 CLAUDE.md Section 7.3）：
    // - key bit 0 → u64 bit 63（较高位）
    // - key bit 4 → u64 bit 59（较低位）
    // PEXT 从低位到高位提取，所以：
    // - key bit 4 → sparse bit 0
    // - key bit 0 → sparse bit 1
    //
    //         bit0
    //        /    \
    //     0 /      \ 1
    //      /        \
    //   bit4      [Leaf A] sparse=0b10（只有 bit0=1）
    //  /    \
    // 0      1
    // [B]   [C]
    // 0b00  0b01（bit4=1 → sparse bit 0 = 1）
    //
    // HOT 要求 entries 按 key/trie 遍历顺序排列：B, C, A

    let mut node = PersistentHOTNode::empty(2);
    node.extraction_masks = PersistentHOTNode::masks_from_bits(&[0, 4]);
    // 按 trie 遍历顺序：左子树先于右子树
    // sparse key 中：bit 0 = key bit 4, bit 1 = key bit 0
    node.sparse_partial_keys[0] = 0b00; // B: bit0=0, bit4=0
    node.sparse_partial_keys[1] = 0b01; // C: bit0=0, bit4=1 → sparse bit 0 = 1
    node.sparse_partial_keys[2] = 0b10; // A: bit0=1 → sparse bit 1 = 1
    node.children.push(ChildRef::Leaf([0u8; NODE_ID_SIZE])); // B
    node.children.push(ChildRef::Leaf([1u8; NODE_ID_SIZE])); // C
    node.children.push(ChildRef::Leaf([2u8; NODE_ID_SIZE])); // A

    // dense=0b10 (sparse key 语义：bit0=key_bit4=0, bit1=key_bit0=1) → 匹配 A
    assert_eq!(node.search_with_dense_key(0b10).found_index(), Some(2));

    // dense=0b11 (bit0=1, bit1=1) → 匹配 A（A 的 sparse=0b10，(0b11 & 0b10)==0b10）
    assert_eq!(node.search_with_dense_key(0b11).found_index(), Some(2));

    // dense=0b00 → 只匹配 B
    assert_eq!(node.search_with_dense_key(0b00).found_index(), Some(0));

    // dense=0b01 (bit0=1, bit1=0) → 匹配 B 和 C，选最后一个 C
    assert_eq!(node.search_with_dense_key(0b01).found_index(), Some(1));
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
    assert_eq!(node.get_mask_for_bit(5), 0); // 不存在

    // 三个 bits 跨 chunk: [3, 7, 100]
    // 同 chunk 内：较大 key bit → 较低 sparse bit
    // 跨 chunk：较小 chunk → 较低 sparse bits
    //
    // chunk 0 (bits 3, 7): 7 → sparse bit 0, 3 → sparse bit 1
    // chunk 1 (bit 100): 100 → sparse bit 2
    let mut node = PersistentHOTNode::empty(1);
    node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7, 100]);
    assert_eq!(node.get_mask_for_bit(7), 0b001); // sparse bit 0
    assert_eq!(node.get_mask_for_bit(3), 0b010); // sparse bit 1
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
