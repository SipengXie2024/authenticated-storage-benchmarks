//! tree 模块测试

use crate::hash::Blake3Hasher;
use crate::node::{LeafData, PersistentHOTNode};
use crate::store::{MemoryNodeStore, NodeStore};

use super::core::HOTTree;

/// 辅助函数：创建测试用的 key
fn make_key(seed: u8) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[0] = seed;
    key
}

#[test]
fn test_empty_tree_lookup() {
    let store = MemoryNodeStore::new();
    let tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    let key = make_key(1);
    let result = tree.lookup(&key).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_single_leaf_lookup() {
    let mut store = MemoryNodeStore::new();

    // 创建叶子数据
    let key = make_key(42);
    let value = b"hello world".to_vec();
    let leaf = LeafData {
        key,
        value: value.clone(),
    };
    let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);
    store.put_leaf(&leaf_id, &leaf).unwrap();

    // 创建只有一个叶子的节点
    let node = PersistentHOTNode::single_leaf(leaf_id.clone());
    let node_id = node.compute_node_id::<Blake3Hasher>(1);
    store.put_node(&node_id, &node).unwrap();

    // 创建树
    let tree: HOTTree<_, Blake3Hasher> = HOTTree::with_root(store, node_id);

    // 查找存在的 key
    let result = tree.lookup(&key).unwrap();
    assert_eq!(result, Some(value));

    // 查找不存在的 key
    let other_key = make_key(99);
    let result = tree.lookup(&other_key).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_tree_accessors() {
    let store = MemoryNodeStore::new();
    let tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    assert!(tree.is_empty());
    assert!(tree.root_id().is_none());
}

// ========================================================================
// Insert 测试
// ========================================================================

#[test]
fn test_insert_into_empty_tree() {
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    let key = make_key(1);
    let value = b"value1".to_vec();

    tree.insert(&key, value.clone(), 1).unwrap();

    assert!(!tree.is_empty());
    let result = tree.lookup(&key).unwrap();
    assert_eq!(result, Some(value));
}

#[test]
fn test_insert_two_keys() {
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    let key1 = make_key(1);
    let value1 = b"value1".to_vec();
    let key2 = make_key(2);
    let value2 = b"value2".to_vec();

    tree.insert(&key1, value1.clone(), 1).unwrap();
    tree.insert(&key2, value2.clone(), 1).unwrap();

    assert_eq!(tree.lookup(&key1).unwrap(), Some(value1));
    assert_eq!(tree.lookup(&key2).unwrap(), Some(value2));
}

#[test]
fn test_insert_update_existing() {
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    let key = make_key(1);
    let value1 = b"value1".to_vec();
    let value2 = b"updated".to_vec();

    tree.insert(&key, value1, 1).unwrap();
    tree.insert(&key, value2.clone(), 2).unwrap();

    let result = tree.lookup(&key).unwrap();
    assert_eq!(result, Some(value2));
}

#[test]
fn test_insert_multiple_keys() {
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    // 插入 10 个 keys
    for i in 0..10u8 {
        let key = make_key(i);
        let value = format!("value{}", i).into_bytes();
        tree.insert(&key, value, 1).unwrap();
    }

    // 验证所有 keys 都能找到
    for i in 0..10u8 {
        let key = make_key(i);
        let result = tree.lookup(&key).unwrap();
        assert!(result.is_some(), "Key {} not found", i);
        assert_eq!(result.unwrap(), format!("value{}", i).into_bytes());
    }

    // 验证不存在的 key
    let missing_key = make_key(100);
    assert!(tree.lookup(&missing_key).unwrap().is_none());
}

// ========================================================================
// Overflow 测试（触发 Split 和 Parent Pull Up / Intermediate Node Creation）
// ========================================================================

/// 辅助函数：创建更分散的 key（避免都在第一个字节区分）
fn make_dispersed_key(seed: u8) -> [u8; 32] {
    let mut key = [0u8; 32];
    // 使用简单的线性同余生成器来分散 bits
    let mut v = seed as u32;
    for byte in key.iter_mut() {
        v = v.wrapping_mul(1103515245).wrapping_add(12345);
        *byte = (v >> 16) as u8;
    }
    key
}

#[test]
fn test_insert_triggers_overflow() {
    // 插入超过 32 个 key 来触发 overflow
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    // 插入 40 个 keys，这应该触发至少一次 split
    for i in 0..40u8 {
        let key = make_dispersed_key(i);
        let value = format!("value{}", i).into_bytes();
        tree.insert(&key, value, 1).unwrap();
    }

    // 验证所有 keys 都能找到
    for i in 0..40u8 {
        let key = make_dispersed_key(i);
        let result = tree.lookup(&key).unwrap();
        assert!(result.is_some(), "Key {} not found after overflow", i);
        assert_eq!(
            result.unwrap(),
            format!("value{}", i).into_bytes(),
            "Value mismatch for key {}",
            i
        );
    }
}

#[test]
fn test_insert_many_keys_large_scale() {
    // 插入 100 个 keys 来更彻底地测试 overflow 处理
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    for i in 0..100u8 {
        let key = make_dispersed_key(i);
        let value = format!("value{}", i).into_bytes();
        tree.insert(&key, value, 1).unwrap();
    }

    // 验证所有 keys
    for i in 0..100u8 {
        let key = make_dispersed_key(i);
        let result = tree.lookup(&key).unwrap();
        assert!(result.is_some(), "Key {} not found", i);
    }
}

#[test]
fn test_insert_update_after_overflow() {
    // 先触发 overflow，然后更新已存在的 key
    let store = MemoryNodeStore::new();
    let mut tree: HOTTree<_, Blake3Hasher> = HOTTree::new(store);

    // 插入 50 个 keys
    for i in 0..50u8 {
        let key = make_dispersed_key(i);
        let value = format!("original{}", i).into_bytes();
        tree.insert(&key, value, 1).unwrap();
    }

    // 更新其中一些 keys
    for i in (0..50u8).step_by(5) {
        let key = make_dispersed_key(i);
        let value = format!("updated{}", i).into_bytes();
        tree.insert(&key, value, 2).unwrap();
    }

    // 验证更新
    for i in 0..50u8 {
        let key = make_dispersed_key(i);
        let result = tree.lookup(&key).unwrap();
        assert!(result.is_some(), "Key {} not found", i);
        let expected = if i % 5 == 0 {
            format!("updated{}", i).into_bytes()
        } else {
            format!("original{}", i).into_bytes()
        };
        assert_eq!(result.unwrap(), expected);
    }
}
