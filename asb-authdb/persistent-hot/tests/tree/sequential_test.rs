//! 顺序插入测试
//!
//! 对应 C++ HOTSingleThreadedTest.cpp 中的顺序插入测试

use persistent_hot::hash::Blake3Hasher;
use persistent_hot::store::MemoryNodeStore;
use persistent_hot::tree::HOTTree;

#[path = "../common/mod.rs"]
mod common;

use common::sample_data::get_sequential_keys;

/// 辅助函数：创建测试树
fn create_test_tree() -> HOTTree<MemoryNodeStore, Blake3Hasher> {
    let store = MemoryNodeStore::new();
    HOTTree::new(store)
}

/// 测试：顺序插入 100 个值
///
/// 对应 C++ testSequentialInsert100
#[test]
fn test_sequential_insert_100() {
    let mut tree = create_test_tree();

    let keys = get_sequential_keys(100);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("value_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Insert {} should succeed", i);
    }

    // 验证所有值
    for (i, key) in keys.iter().enumerate() {
        let result = tree.lookup(key);
        assert!(result.is_ok(), "Lookup {} should succeed", i);
        assert!(result.unwrap().is_some(), "Value {} should exist", i);
    }
}

/// 测试：顺序插入 1000 个值
///
/// 对应 C++ testSequentialInsert1000
#[test]
fn test_sequential_insert_1000() {
    let mut tree = create_test_tree();

    let keys = get_sequential_keys(1000);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("value_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Insert {} should succeed", i);
    }

    // 验证所有值
    for key in &keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}

/// 测试：顺序插入 10000 个值
///
/// 对应 C++ testSequentialInsert10000
#[test]
fn test_sequential_insert_10000() {
    let mut tree = create_test_tree();

    let keys = get_sequential_keys(10000);

    for (i, key) in keys.iter().enumerate() {
        let value = format!("value_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Insert {} should succeed", i);
    }

    // 随机抽样验证
    for i in (0..10000).step_by(100) {
        assert!(tree.lookup(&keys[i]).unwrap().is_some());
    }
}

/// 测试：逆序插入
///
/// 对应 C++ testReverseInsert
#[test]
fn test_reverse_insert() {
    let mut tree = create_test_tree();

    let keys = get_sequential_keys(1000);

    // 逆序插入
    for (i, key) in keys.iter().rev().enumerate() {
        let value = format!("reverse_{}", i).into_bytes();
        let result = tree.insert(key, value);
        assert!(result.is_ok(), "Reverse insert {} should succeed", i);
    }

    // 验证
    for key in &keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}

/// 测试：交替插入
///
/// 对应 C++ testAlternatingInsert
#[test]
fn test_alternating_insert() {
    let mut tree = create_test_tree();

    let keys = get_sequential_keys(1000);

    // 交替插入：先偶数，后奇数
    for i in (0..1000).step_by(2) {
        let value = format!("even_{}", i).into_bytes();
        tree.insert(&keys[i], value).unwrap();
    }

    for i in (1..1000).step_by(2) {
        let value = format!("odd_{}", i).into_bytes();
        tree.insert(&keys[i], value).unwrap();
    }

    // 验证
    for key in &keys {
        assert!(tree.lookup(key).unwrap().is_some());
    }
}

/// 测试：空树查询
#[test]
fn test_empty_tree_lookup() {
    let tree = create_test_tree();

    let key = [0u8; 32];
    let result = tree.lookup(&key);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// 测试：单元素树
#[test]
fn test_single_element_tree() {
    let mut tree = create_test_tree();

    let key = [42u8; 32];
    tree.insert(&key, b"single".to_vec()).unwrap();

    assert!(tree.lookup(&key).unwrap().is_some());

    // 不存在的键
    let other_key = [0u8; 32];
    assert!(tree.lookup(&other_key).unwrap().is_none());
}
