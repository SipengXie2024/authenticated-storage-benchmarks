//! 随机插入测试
//!
//! 对应 C++ HOTSingleThreadedTest.cpp 中的随机插入测试

use std::sync::Arc;
use persistent_hot::hash::Blake3Hasher;
use persistent_hot::tree::HOTTree;

#[path = "../common/mod.rs"]
mod common;

use common::sample_data::get_random_keys;

/// 辅助函数：创建测试树
fn create_test_tree() -> HOTTree<Blake3Hasher> {
    let db = Arc::new(kvdb_memorydb::create(2)); // 2 columns: node and leaf
    HOTTree::new(db, 0, 1)
}

/// 测试：随机插入 100 个值
///
/// 对应 C++ testRandomInsert100
#[test]
fn test_random_insert_100() {
    let mut tree = create_test_tree();

    let keys = get_random_keys(100, 12345);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("random_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Random insert {} should succeed", i);
    }

    // 验证所有值
    for key in &keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}

/// 测试：随机插入 1000 个值
///
/// 对应 C++ testRandomInsert1000
#[test]
fn test_random_insert_1000() {
    let mut tree = create_test_tree();

    let keys = get_random_keys(1000, 54321);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("random_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Random insert {} should succeed", i);
    }

    // 验证所有值
    for key in &keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}

/// 测试：随机插入 10000 个值
///
/// 对应 C++ testRandomInsert10000
#[test]
fn test_random_insert_10000() {
    let mut tree = create_test_tree();

    let keys = get_random_keys(10000, 98765);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("random_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Random insert {} should succeed", i);
    }

    // 随机抽样验证
    for i in (0..10000).step_by(100) {
        assert!(tree.lookup(&keys[i]).unwrap().is_some());
    }
}

/// 测试：不同 seed 的随机数据
///
/// 验证多种随机分布都能正确处理
#[test]
fn test_multiple_random_seeds() {
    for seed in [1, 42, 100, 9999, 123456] {
        let mut tree = create_test_tree();
        let keys = get_random_keys(500, seed);

        for (i, key) in keys.iter().enumerate() {
            let value = format!("seed{}_{}", seed, i).into_bytes();
            tree.insert(key, value).unwrap();
        }

        for key in &keys {
            assert!(
                tree.lookup(key).unwrap().is_some(),
                "Failed for seed {}",
                seed
            );
        }
    }
}

/// 测试：随机查询不存在的键
#[test]
fn test_random_lookup_nonexistent() {
    let mut tree = create_test_tree();

    // 插入一批键
    let inserted_keys = get_random_keys(100, 11111);
    for (i, key) in inserted_keys.iter().enumerate() {
        tree.insert(key, format!("v{}", i).into_bytes()).unwrap();
    }

    // 查询另一批随机键（很可能不存在）
    let query_keys = get_random_keys(100, 22222);
    for key in &query_keys {
        let result = tree.lookup(key);
        assert!(result.is_ok());
        // 不做 is_some 断言，因为可能碰巧存在
    }
}

/// 测试：混合随机和顺序插入
#[test]
fn test_mixed_random_sequential() {
    let mut tree = create_test_tree();

    let random_keys = get_random_keys(500, 33333);
    let sequential_keys: Vec<[u8; 32]> = (0..500)
        .map(|i| {
            let mut key = [0u8; 32];
            key[0] = ((i >> 8) & 0xFF) as u8;
            key[1] = (i & 0xFF) as u8;
            key
        })
        .collect();

    // 交替插入
    for i in 0..500 {
        tree.insert(&random_keys[i], b"random".to_vec()).unwrap();
        tree.insert(&sequential_keys[i], b"sequential".to_vec()).unwrap();
    }

    // 验证
    for key in &random_keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
    for key in &sequential_keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}
