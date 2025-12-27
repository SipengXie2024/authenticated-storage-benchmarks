//! 边界条件测试
//!
//! 对应 C++ HOTSingleThreadedTest.cpp 中的边界测试

use persistent_hot::hash::Blake3Hasher;
use persistent_hot::store::MemoryNodeStore;
use persistent_hot::tree::HOTTree;

#[path = "../common/mod.rs"]
mod common;

/// 辅助函数：创建测试树
fn create_test_tree() -> HOTTree<MemoryNodeStore, Blake3Hasher> {
    let store = MemoryNodeStore::new();
    HOTTree::new(store)
}

/// 测试：最小和最大键
///
/// 对应 C++ testMinMaxKeys
#[test]
fn test_min_max_keys() {
    let mut tree = create_test_tree();

    let min_key = [0u8; 32];
    let max_key = [0xFFu8; 32];

    tree.insert(&min_key, b"min".to_vec()).unwrap();
    tree.insert(&max_key, b"max".to_vec()).unwrap();

    assert!(tree.lookup(&min_key).unwrap().is_some());
    assert!(tree.lookup(&max_key).unwrap().is_some());
}

/// 测试：相邻键
///
/// 对应 C++ testAdjacentKeys
#[test]
fn test_adjacent_keys() {
    let mut tree = create_test_tree();

    // 创建多个只差一个 bit 的键
    for i in 0..32u8 {
        let mut key = [0u8; 32];
        key[0] = i;
        tree.insert(&key, format!("value_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..32u8 {
        let mut key = [0u8; 32];
        key[0] = i;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：单 bit 差异
///
/// 对应 C++ testSingleBitDifference
#[test]
fn test_single_bit_difference() {
    let mut tree = create_test_tree();

    // 基准键
    let base_key = [0u8; 32];
    tree.insert(&base_key, b"base".to_vec()).unwrap();

    // 每个字节的每个 bit 位不同的键
    for byte_idx in 0..32 {
        for bit_idx in 0..8 {
            let mut key = [0u8; 32];
            key[byte_idx] = 1 << bit_idx;
            tree.insert(&key, format!("bit_{}_{}", byte_idx, bit_idx).into_bytes())
                .unwrap();
        }
    }

    // 验证基准键
    assert!(tree.lookup(&base_key).unwrap().is_some());

    // 验证所有变体键
    for byte_idx in 0..32 {
        for bit_idx in 0..8 {
            let mut key = [0u8; 32];
            key[byte_idx] = 1 << bit_idx;
            assert!(
                tree.lookup(&key).unwrap().is_some(),
                "Failed for byte {} bit {}",
                byte_idx,
                bit_idx
            );
        }
    }
}

/// 测试：前缀相同的键
#[test]
fn test_common_prefix_keys() {
    let mut tree = create_test_tree();

    // 所有键共享相同的前 16 字节
    let prefix = [0xABu8; 16];

    for i in 0..100 {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(&prefix);
        key[16] = (i >> 8) as u8;
        key[17] = (i & 0xFF) as u8;
        tree.insert(&key, format!("prefix_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..100 {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(&prefix);
        key[16] = (i >> 8) as u8;
        key[17] = (i & 0xFF) as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：稀疏键分布
///
/// 对应 C++ testSparseKeys
#[test]
fn test_sparse_keys() {
    let mut tree = create_test_tree();

    // 只有少数几个非零字节
    for i in 0..100 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        key[31] = (255 - i) as u8;
        tree.insert(&key, format!("sparse_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..100 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        key[31] = (255 - i) as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：高度集中的键
#[test]
fn test_clustered_keys() {
    let mut tree = create_test_tree();

    // 所有键都在很小的范围内
    for i in 0..256 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        tree.insert(&key, format!("clustered_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..256 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：全部相同的键值（除了最后一个字节）
#[test]
fn test_keys_differ_in_last_byte() {
    let mut tree = create_test_tree();

    for i in 0..256 {
        let mut key = [0xFFu8; 32];
        key[31] = i as u8;
        tree.insert(&key, format!("last_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..256 {
        let mut key = [0xFFu8; 32];
        key[31] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：节点分裂边界
///
/// 插入恰好触发节点分裂的数量
#[test]
fn test_split_boundary() {
    let mut tree = create_test_tree();

    // HOT 节点最多 32 个 entries，插入 33 个应该触发分裂
    for i in 0..33 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        tree.insert(&key, format!("split_{}", i).into_bytes())
            .unwrap();
    }

    // 验证所有值仍然可查
    for i in 0..33 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：多层分裂
///
/// 插入足够多的数据触发多层分裂
#[test]
fn test_multi_level_split() {
    let mut tree = create_test_tree();

    // 插入 1000+ 个值，应该产生多层树结构
    for i in 0..1024 {
        let mut key = [0u8; 32];
        key[0] = ((i >> 8) & 0xFF) as u8;
        key[1] = (i & 0xFF) as u8;
        tree.insert(&key, format!("multi_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..1024 {
        let mut key = [0u8; 32];
        key[0] = ((i >> 8) & 0xFF) as u8;
        key[1] = (i & 0xFF) as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}
