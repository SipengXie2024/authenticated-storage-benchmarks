//! 边界情况测试
//!
//! 测试各种边界情况和异常场景

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

/// 测试：重复插入相同键
#[test]
fn test_duplicate_key_insert() {
    let mut tree = create_test_tree();

    let key = [42u8; 32];

    // 第一次插入
    let result1 = tree.insert(&key, b"first".to_vec());
    assert!(result1.is_ok());

    // 第二次插入相同键（同一个 epoch 内）
    let result2 = tree.insert(&key, b"second".to_vec());
    // 行为取决于实现：可能成功（更新）或返回错误
    // 这里只验证不会 panic
    let _ = result2;
}

/// 测试：全零键
#[test]
fn test_all_zero_key() {
    let mut tree = create_test_tree();

    let key = [0u8; 32];
    tree.insert(&key, b"zero".to_vec()).unwrap();
    assert!(tree.lookup(&key).unwrap().is_some());
}

/// 测试：全 FF 键
#[test]
fn test_all_ff_key() {
    let mut tree = create_test_tree();

    let key = [0xFFu8; 32];
    tree.insert(&key, b"max".to_vec()).unwrap();
    assert!(tree.lookup(&key).unwrap().is_some());
}

/// 测试：交替 pattern 键
#[test]
fn test_alternating_pattern_keys() {
    let mut tree = create_test_tree();

    // 0x55 = 0b01010101
    let key1 = [0x55u8; 32];
    // 0xAA = 0b10101010
    let key2 = [0xAAu8; 32];

    tree.insert(&key1, b"pattern1".to_vec()).unwrap();
    tree.insert(&key2, b"pattern2".to_vec()).unwrap();

    assert!(tree.lookup(&key1).unwrap().is_some());
    assert!(tree.lookup(&key2).unwrap().is_some());
}

/// 测试：空值
#[test]
fn test_empty_value() {
    let mut tree = create_test_tree();

    let key = [1u8; 32];
    tree.insert(&key, b"".to_vec()).unwrap();
    assert!(tree.lookup(&key).unwrap().is_some());
}

/// 测试：大值
#[test]
fn test_large_value() {
    let mut tree = create_test_tree();

    let key = [1u8; 32];
    let large_value = vec![0xABu8; 10000];
    tree.insert(&key, large_value).unwrap();
    assert!(tree.lookup(&key).unwrap().is_some());
}

/// 测试：连续插入和查询
#[test]
fn test_interleaved_insert_lookup() {
    let mut tree = create_test_tree();

    for i in 0..100 {
        let mut key = [0u8; 32];
        key[0] = i as u8;

        // 插入
        tree.insert(&key, format!("value_{}", i).into_bytes())
            .unwrap();

        // 立即查询
        assert!(tree.lookup(&key).unwrap().is_some());

        // 查询之前的键
        for j in 0..=i {
            let mut prev_key = [0u8; 32];
            prev_key[0] = j as u8;
            assert!(tree.lookup(&prev_key).unwrap().is_some());
        }
    }
}

/// 测试：高 entropy 键
#[test]
fn test_high_entropy_keys() {
    let mut tree = create_test_tree();

    // 每个字节都不同
    for i in 0..100 {
        let mut key = [0u8; 32];
        for j in 0..32 {
            key[j] = ((i * 17 + j * 31) % 256) as u8;
        }
        tree.insert(&key, format!("entropy_{}", i).into_bytes())
            .unwrap();
    }

    // 验证
    for i in 0..100 {
        let mut key = [0u8; 32];
        for j in 0..32 {
            key[j] = ((i * 17 + j * 31) % 256) as u8;
        }
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：跨 epoch 插入
///
/// 原测试使用不同 version 参数，现改为使用 commit() 推进 epoch
#[test]
fn test_cross_epoch_inserts() {
    let mut tree = create_test_tree();

    // 每 10 个插入后 commit 一次，共 10 个 epoch
    for epoch in 0..10 {
        for i in 0..10 {
            let mut key = [0u8; 32];
            key[0] = (epoch * 10 + i) as u8;
            tree.insert(&key, format!("epoch_{}_item_{}", epoch, i).into_bytes())
                .unwrap();
        }
        tree.commit(epoch as u64);
    }

    // 验证所有 100 个键
    for i in 0..100 {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：极端稀疏分布
#[test]
fn test_extremely_sparse_distribution() {
    let mut tree = create_test_tree();

    // 只在特定位置有值
    let positions = [0, 31, 64, 127, 192, 255];

    for &pos in &positions {
        let mut key = [0u8; 32];
        key[pos / 8] = 1 << (7 - pos % 8);
        tree.insert(&key, format!("pos_{}", pos).into_bytes())
            .unwrap();
    }

    // 验证
    for &pos in &positions {
        let mut key = [0u8; 32];
        key[pos / 8] = 1 << (7 - pos % 8);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：最深路径
///
/// 创建需要遍历多层才能到达的键
#[test]
fn test_deep_tree_path() {
    let mut tree = create_test_tree();

    // 创建一系列键，每个键只在一个特定位置不同
    // 这会创建一个很深的树结构
    for depth in 0..256 {
        let mut key = [0xFFu8; 32];
        key[depth / 8] &= !(1 << (7 - depth % 8));
        tree.insert(&key, format!("depth_{}", depth).into_bytes())
            .unwrap();
    }

    // 验证
    for depth in 0..256 {
        let mut key = [0xFFu8; 32];
        key[depth / 8] &= !(1 << (7 - depth % 8));
        assert!(
            tree.lookup(&key).unwrap().is_some(),
            "Failed at depth {}",
            depth
        );
    }
}

/// 测试：批量插入后查询不存在的键
#[test]
fn test_lookup_nonexistent_after_bulk_insert() {
    let mut tree = create_test_tree();

    // 插入偶数键
    for i in (0..200).step_by(2) {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        tree.insert(&key, b"even".to_vec()).unwrap();
    }

    // 查询奇数键（不存在）
    for i in (1..200).step_by(2) {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_none());
    }

    // 确认偶数键仍存在
    for i in (0..200).step_by(2) {
        let mut key = [0u8; 32];
        key[0] = i as u8;
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：相同前缀不同长度（通过不同后缀模拟）
#[test]
fn test_prefix_variants() {
    let mut tree = create_test_tree();

    let base = [0xABu8; 16];

    // 创建多个共享相同前缀但后缀不同的键
    for suffix_len in 0..16 {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(&base);
        for i in 0..suffix_len {
            key[16 + i] = ((suffix_len + i) * 17) as u8;
        }
        tree.insert(
            &key,
            format!("suffix_len_{}", suffix_len).into_bytes(),
        )
        .unwrap();
    }

    // 验证
    for suffix_len in 0..16 {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(&base);
        for i in 0..suffix_len {
            key[16 + i] = ((suffix_len + i) * 17) as u8;
        }
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}
