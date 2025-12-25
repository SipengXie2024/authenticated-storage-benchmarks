//! store 模块测试

use super::*;
use crate::hash::Blake3Hasher;
use crate::node::{LeafData, NodeId, PersistentHOTNode, NODE_ID_SIZE};

fn create_test_node() -> PersistentHOTNode {
    let mut node = PersistentHOTNode::empty(2);
    node.extraction_masks = PersistentHOTNode::masks_from_bits(&[3, 7]);
    node.sparse_partial_keys[0] = 0;
    node.sparse_partial_keys[1] = 1;
    node.sparse_partial_keys[2] = 2;
    node.sparse_partial_keys[3] = 3;
    node.children.push(NodeId::Leaf([0x00u8; NODE_ID_SIZE]));
    node.children.push(NodeId::Leaf([0x10u8; NODE_ID_SIZE]));
    node.children.push(NodeId::Leaf([0x01u8; NODE_ID_SIZE]));
    node.children.push(NodeId::Leaf([0x11u8; NODE_ID_SIZE]));
    node
}

fn create_test_leaf() -> LeafData {
    let mut key = [0u8; 32];
    key[0] = 0xAB;
    LeafData::new(key, b"test value".to_vec())
}

#[test]
fn test_memory_store_put_and_get_node() {
    let mut store = MemoryNodeStore::new();
    let node = create_test_node();
    let node_id = node.compute_node_id::<Blake3Hasher>(1);

    // 存储节点
    store.put_node(&node_id, &node).unwrap();

    // 检查节点存在
    assert!(store.contains_node(&node_id).unwrap());

    // 获取节点
    let retrieved = store.get_node(&node_id).unwrap().unwrap();
    assert_eq!(retrieved, node);
}

#[test]
fn test_memory_store_put_and_get_leaf() {
    let mut store = MemoryNodeStore::new();
    let leaf = create_test_leaf();
    let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

    // 存储叶子
    store.put_leaf(&leaf_id, &leaf).unwrap();

    // 检查叶子存在
    assert!(store.contains_leaf(&leaf_id).unwrap());

    // 获取叶子
    let retrieved = store.get_leaf(&leaf_id).unwrap().unwrap();
    assert_eq!(retrieved, leaf);
}

#[test]
fn test_memory_store_get_nonexistent() {
    let store = MemoryNodeStore::new();
    let fake_node_id = NodeId::Internal([0u8; NODE_ID_SIZE]);
    let fake_leaf_id = NodeId::Leaf([0u8; NODE_ID_SIZE]);

    // 不存在的节点/叶子应该返回 None
    assert!(store.get_node(&fake_node_id).unwrap().is_none());
    assert!(store.get_leaf(&fake_leaf_id).unwrap().is_none());
    assert!(!store.contains_node(&fake_node_id).unwrap());
    assert!(!store.contains_leaf(&fake_leaf_id).unwrap());
}

#[test]
fn test_memory_store_idempotent_put() {
    let mut store = MemoryNodeStore::new();
    let node = create_test_node();
    let node_id = node.compute_node_id::<Blake3Hasher>(1);

    // 多次写入相同节点
    store.put_node(&node_id, &node).unwrap();
    store.put_node(&node_id, &node).unwrap();
    store.put_node(&node_id, &node).unwrap();

    // 应该只存储一份
    assert_eq!(store.node_count(), 1);

    // 内容应该一致
    let retrieved = store.get_node(&node_id).unwrap().unwrap();
    assert_eq!(retrieved, node);
}

#[test]
fn test_memory_store_multiple_nodes() {
    let mut store = MemoryNodeStore::new();

    // 创建多个不同的节点
    let nodes: Vec<PersistentHOTNode> = (0..10)
        .map(|i| {
            let mut node = PersistentHOTNode::empty(2);
            node.extraction_masks = PersistentHOTNode::masks_from_bits(&[i as u16]);
            node.sparse_partial_keys[0] = 0;
            node.sparse_partial_keys[1] = 1;
            node.children.push(NodeId::Leaf([i as u8; NODE_ID_SIZE]));
            node.children.push(NodeId::Leaf([(i + 1) as u8; NODE_ID_SIZE]));
            node
        })
        .collect();

    // 存储所有节点
    for node in &nodes {
        let id = node.compute_node_id::<Blake3Hasher>(1);
        store.put_node(&id, node).unwrap();
    }

    assert_eq!(store.node_count(), 10);

    // 验证所有节点都能正确检索
    for node in &nodes {
        let id = node.compute_node_id::<Blake3Hasher>(1);
        let retrieved = store.get_node(&id).unwrap().unwrap();
        assert_eq!(&retrieved, node);
    }
}

#[test]
fn test_memory_store_clear() {
    let mut store = MemoryNodeStore::new();
    let node = create_test_node();
    let node_id = node.compute_node_id::<Blake3Hasher>(1);
    let leaf = create_test_leaf();
    let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

    store.put_node(&node_id, &node).unwrap();
    store.put_leaf(&leaf_id, &leaf).unwrap();
    assert!(!store.is_empty());

    store.clear();
    assert!(store.is_empty());
    assert!(store.get_node(&node_id).unwrap().is_none());
    assert!(store.get_leaf(&leaf_id).unwrap().is_none());
}

#[test]
fn test_memory_store_clone_shares_data() {
    let mut store1 = MemoryNodeStore::new();
    let store2 = store1.clone();

    let node = create_test_node();
    let node_id = node.compute_node_id::<Blake3Hasher>(1);

    // 在 store1 中写入
    store1.put_node(&node_id, &node).unwrap();

    // store2 应该也能看到
    assert!(store2.contains_node(&node_id).unwrap());
    let retrieved = store2.get_node(&node_id).unwrap().unwrap();
    assert_eq!(retrieved, node);
}

#[test]
fn test_memory_store_flush() {
    let mut store = MemoryNodeStore::new();
    // flush 对内存存储应该是 no-op
    assert!(store.flush().is_ok());
}

#[test]
fn test_memory_store_separate_node_and_leaf() {
    let mut store = MemoryNodeStore::new();

    // 创建一个节点和一个叶子，使用相同的 ID（模拟冲突场景）
    let node = create_test_node();
    let leaf = create_test_leaf();

    // 使用不同的 version 确保 ID 不同
    let node_id = node.compute_node_id::<Blake3Hasher>(1);
    let leaf_id = leaf.compute_node_id::<Blake3Hasher>(2);

    store.put_node(&node_id, &node).unwrap();
    store.put_leaf(&leaf_id, &leaf).unwrap();

    // 两者应该独立存储
    assert_eq!(store.node_count(), 1);
    assert_eq!(store.leaf_count(), 1);

    // 各自能正确检索
    assert!(store.get_node(&node_id).unwrap().is_some());
    assert!(store.get_leaf(&leaf_id).unwrap().is_some());
}

// ============================================================================
// KvNodeStore 测试
// ============================================================================

#[cfg(feature = "kvdb-backend")]
mod kv_tests {
    use super::*;
    use kvdb::KeyValueDB;
    use std::sync::Arc;

    #[test]
    fn test_kv_store_put_and_get_node() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 存储节点
        store.put_node(&node_id, &node).unwrap();

        // 检查节点存在
        assert!(store.contains_node(&node_id).unwrap());

        // 获取节点
        let retrieved = store.get_node(&node_id).unwrap().unwrap();
        assert_eq!(retrieved, node);
    }

    #[test]
    fn test_kv_store_put_and_get_leaf() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        let leaf = create_test_leaf();
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        // 存储叶子
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 检查叶子存在
        assert!(store.contains_leaf(&leaf_id).unwrap());

        // 获取叶子
        let retrieved = store.get_leaf(&leaf_id).unwrap().unwrap();
        assert_eq!(retrieved, leaf);
    }

    #[test]
    fn test_kv_store_get_nonexistent() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let store = KvNodeStore::new(db, 0, 1);
        let fake_node_id = NodeId::Internal([0u8; NODE_ID_SIZE]);
        let fake_leaf_id = NodeId::Leaf([0u8; NODE_ID_SIZE]);

        // 不存在的节点/叶子应该返回 None
        assert!(store.get_node(&fake_node_id).unwrap().is_none());
        assert!(store.get_leaf(&fake_leaf_id).unwrap().is_none());
        assert!(!store.contains_node(&fake_node_id).unwrap());
        assert!(!store.contains_leaf(&fake_leaf_id).unwrap());
    }

    #[test]
    fn test_kv_store_version_isolation() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(1));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 在版本 1 存储节点
        let mut store_v1 = KvNodeStore::new(Arc::clone(&db), 0, 1);
        store_v1.put_node(&node_id, &node).unwrap();

        // 版本 2 看不到版本 1 的数据
        let store_v2 = KvNodeStore::new(Arc::clone(&db), 0, 2);
        assert!(store_v2.get_node(&node_id).unwrap().is_none());

        // 版本 1 仍然可以看到数据
        assert!(store_v1.get_node(&node_id).unwrap().is_some());
    }

    #[test]
    fn test_kv_store_version_switch() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(1));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 在版本 1 存储节点
        let mut store = KvNodeStore::new(Arc::clone(&db), 0, 1);
        store.put_node(&node_id, &node).unwrap();

        // 切换到版本 2，看不到数据
        store.set_version_id(2);
        assert!(store.get_node(&node_id).unwrap().is_none());

        // 切回版本 1，可以看到数据
        store.set_version_id(1);
        assert!(store.get_node(&node_id).unwrap().is_some());
    }

    #[test]
    fn test_kv_store_multiple_nodes() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);

        // 创建多个不同的节点
        let nodes: Vec<PersistentHOTNode> = (0..10)
            .map(|i| {
                let mut node = PersistentHOTNode::empty(2);
                node.extraction_masks = PersistentHOTNode::masks_from_bits(&[i as u16]);
                node.sparse_partial_keys[0] = 0;
                node.sparse_partial_keys[1] = 1;
                node.children.push(NodeId::Leaf([i as u8; NODE_ID_SIZE]));
                node.children.push(NodeId::Leaf([(i + 1) as u8; NODE_ID_SIZE]));
                node
            })
            .collect();

        // 存储所有节点
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
            store.put_node(&id, node).unwrap();
        }

        // 验证所有节点都能正确检索
        for node in &nodes {
            let id = node.compute_node_id::<Blake3Hasher>(1);
            let retrieved = store.get_node(&id).unwrap().unwrap();
            assert_eq!(&retrieved, node);
        }
    }

    #[test]
    fn test_kv_store_flush() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);
        // flush 应该成功
        assert!(store.flush().is_ok());
    }

    #[test]
    fn test_kv_store_shared_db() {
        let db: Arc<dyn KeyValueDB> = Arc::new(kvdb_memorydb::create(2));
        let node = create_test_node();
        let node_id = node.compute_node_id::<Blake3Hasher>(1);

        // 两个 store 使用不同的 column
        let mut store_col0 = KvNodeStore::new(Arc::clone(&db), 0, 1);
        let store_col1 = KvNodeStore::new(Arc::clone(&db), 1, 1);

        // 在 column 0 存储节点
        store_col0.put_node(&node_id, &node).unwrap();

        // column 1 看不到 column 0 的数据
        assert!(store_col1.get_node(&node_id).unwrap().is_none());

        // column 0 可以看到数据
        assert!(store_col0.get_node(&node_id).unwrap().is_some());
    }

    #[test]
    fn test_kv_store_node_leaf_isolation() {
        let db = Arc::new(kvdb_memorydb::create(1));
        let mut store = KvNodeStore::new(db, 0, 1);

        let node = create_test_node();
        let leaf = create_test_leaf();

        let node_id = node.compute_node_id::<Blake3Hasher>(1);
        let leaf_id = leaf.compute_node_id::<Blake3Hasher>(1);

        store.put_node(&node_id, &node).unwrap();
        store.put_leaf(&leaf_id, &leaf).unwrap();

        // 各自能正确检索
        assert!(store.get_node(&node_id).unwrap().is_some());
        assert!(store.get_leaf(&leaf_id).unwrap().is_some());
    }
}
