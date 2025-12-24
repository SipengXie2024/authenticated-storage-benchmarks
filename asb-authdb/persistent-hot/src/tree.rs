//! HOTTree: Height Optimized Trie 的树级操作
//!
//! 提供 tree-level 的 lookup/insert/delete 操作，
//! 基于 `PersistentHOTNode` 节点和 `NodeStore` 存储抽象。

use std::marker::PhantomData;

use crate::hash::{Blake3Hasher, Hasher};
use crate::node::{
    extract_bit, find_first_differing_bit, BiNode, ChildRef, LeafData, NodeId,
    PersistentHOTNode, SearchResult,
};
use crate::store::{NodeStore, Result, StoreError};

// ============================================================================
// Insert Stack
// ============================================================================

/// 插入栈条目
///
/// 用于追踪从根到当前节点的路径，支持 Parent Pull Up 操作。
/// 在 overflow 处理时，需要沿路径向上传播更新。
#[derive(Debug, Clone)]
struct InsertStackEntry {
    /// 当前节点的 ID（用于调试和潜在的扩展）
    #[allow(dead_code)]
    node_id: NodeId,
    /// 选中的 child 索引（用于 overflow 时更新父节点）
    child_index: usize,
    /// 缓存的节点数据（避免重复读取）
    node: PersistentHOTNode,
}

// ============================================================================
// HOT Tree
// ============================================================================

/// Height Optimized Trie
///
/// # 类型参数
///
/// - `S`: 节点存储实现，必须实现 `NodeStore` trait
/// - `H`: 哈希算法，默认 Blake3
///
/// # 版本管理
///
/// `version` 不存储在结构中，而是作为 insert 参数传入，
pub struct HOTTree<S: NodeStore, H: Hasher = Blake3Hasher> {
    store: S,
    root_id: Option<NodeId>,
    _marker: PhantomData<H>,
}

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 创建空树
    pub fn new(store: S) -> Self {
        Self {
            store,
            root_id: None,
            _marker: PhantomData,
        }
    }

    /// 创建带有根节点的树
    pub fn with_root(store: S, root_id: NodeId) -> Self {
        Self {
            store,
            root_id: Some(root_id),
            _marker: PhantomData,
        }
    }

    /// 获取根节点 ID
    #[inline]
    pub fn root_id(&self) -> Option<&NodeId> {
        self.root_id.as_ref()
    }

    /// 获取存储引用
    #[inline]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// 获取可变存储引用
    #[inline]
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// 查找指定版本下 key 对应的值
    ///
    /// # 参数
    ///
    /// - `key`: 32 字节的 key
    ///
    /// # 返回
    ///
    /// - `Ok(Some(value))`: 找到匹配的 key，返回 value
    /// - `Ok(None)`: key 不存在或假阳性（partial key 匹配但完整 key 不匹配）
    /// - `Err(_)`: 存储错误
    pub fn lookup(&self, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let root_id = match &self.root_id {
            Some(id) => id,
            None => return Ok(None),
        };
        self.lookup_internal(root_id, key)
    }

    /// 内部递归查找
    fn lookup_internal(&self, node_id: &NodeId, key: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let node = self
            .store
            .get_node(node_id)?
            .ok_or(StoreError::NotFound)?;

        match node.search(key) {
            SearchResult::Found { index } => {
                match &node.children[index] {
                    ChildRef::Internal(child_id) => {
                        // 递归搜索子节点
                        self.lookup_internal(child_id, key)
                    }
                    ChildRef::Leaf(leaf_id) => {
                        // 获取叶子数据，验证 key 完全匹配
                        let leaf = self
                            .store
                            .get_leaf(leaf_id)?
                            .ok_or(StoreError::NotFound)?;
                        if &leaf.key == key {
                            Ok(Some(leaf.value.clone()))
                        } else {
                            Ok(None) // Key 不匹配（假阳性）
                        }
                    }
                }
            }
            SearchResult::NotFound { .. } => Ok(None),
        }
    }

    /// 检查树是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.root_id.is_none()
    }

    // ========================================================================
    // Insert 操作（栈 + 迭代模式）
    // ========================================================================

    /// 插入 key-value 对
    ///
    /// # 参数
    ///
    /// - `key`: 32 字节的 key
    /// - `value`: 任意长度的 value
    /// - `version`: 版本号（用于生成 NodeId）
    ///
    /// # 返回
    ///
    /// - `Ok(())`: 插入成功
    /// - `Err(_)`: 存储错误
    pub fn insert(&mut self, key: &[u8; 32], value: Vec<u8>, version: u64) -> Result<()> {
        // 创建并存储叶子
        let leaf = LeafData {
            key: *key,
            value,
        };
        let leaf_id = leaf.compute_node_id::<H>(version);
        self.store.put_leaf(&leaf_id, &leaf)?;

        match &self.root_id {
            None => {
                // 空树：创建单叶子节点作为根
                let node = PersistentHOTNode::single_leaf(leaf_id);
                let node_id = node.compute_node_id::<H>(version);
                self.store.put_node(&node_id, &node)?;
                self.root_id = Some(node_id);
                Ok(())
            }
            Some(root_id) => {
                // 非空树：使用栈模式插入
                self.insert_with_stack(root_id.clone(), key, leaf_id, version)
            }
        }
    }

    /// 使用栈模式插入（支持 Parent Pull Up）
    ///
    /// # 流程
    ///
    /// 1. Phase 1：向下搜索，构建 stack（记录从根到目标节点的路径）
    /// 2. Phase 2：在目标节点执行操作（Normal Insert / Leaf Pushdown / Overflow）
    /// 3. Phase 3：如果发生 overflow，调用 handle_overflow_with_stack 处理
    /// 4. Phase 4：向上传播指针更新
    fn insert_with_stack(
        &mut self,
        root_id: NodeId,
        key: &[u8; 32],
        leaf_id: NodeId,
        version: u64,
    ) -> Result<()> {
        let mut stack: Vec<InsertStackEntry> = Vec::new();
        let mut current_id = root_id;

        // Phase 1: 向下搜索，构建 stack
        loop {
            let node = self
                .store
                .get_node(&current_id)?
                .ok_or(StoreError::NotFound)?;

            match node.search(key) {
                SearchResult::Found { index } => {
                    // 先提取需要的信息，避免借用冲突
                    let child_ref = node.children[index].clone();

                    match child_ref {
                        ChildRef::Internal(child_id) => {
                            // 记录当前节点到栈，继续向下
                            stack.push(InsertStackEntry {
                                node_id: current_id,
                                child_index: index,
                                node,
                            });
                            current_id = child_id;
                            continue;
                        }
                        ChildRef::Leaf(existing_leaf_id) => {
                            // 到达叶子，处理碰撞
                            let existing_leaf = self
                                .store
                                .get_leaf(&existing_leaf_id)?
                                .ok_or(StoreError::NotFound)?;

                            let new_child_id = if &existing_leaf.key == key {
                                // 相同 key：替换值
                                let mut new_node = node.clone();
                                new_node.children[index] = ChildRef::Leaf(leaf_id);
                                let new_node_id = new_node.compute_node_id::<H>(version);
                                self.store.put_node(&new_node_id, &new_node)?;
                                new_node_id
                            } else {
                                // 不同 key：Leaf Node Pushdown
                                self.leaf_pushdown(
                                    &node,
                                    index,
                                    &existing_leaf.key,
                                    existing_leaf_id,
                                    key,
                                    leaf_id,
                                    version,
                                )?
                            };

                            // 向上传播更新
                            return self.propagate_pointer_updates(stack, new_child_id, version);
                        }
                    }
                }
                SearchResult::NotFound { dense_key } => {
                    // 没有匹配的 entry：需要添加新 entry
                    let new_node_id = self.add_entry_to_node_with_stack(
                        &mut stack,
                        current_id,
                        node,
                        key,
                        dense_key,
                        leaf_id,
                        version,
                    )?;

                    // 向上传播更新
                    return self.propagate_pointer_updates(stack, new_node_id, version);
                }
            }
        }
    }

    /// Leaf Node Pushdown: 创建新节点容纳两个叶子
    fn leaf_pushdown(
        &mut self,
        parent_node: &PersistentHOTNode,
        affected_index: usize,
        existing_key: &[u8; 32],
        existing_leaf_id: NodeId,
        new_key: &[u8; 32],
        new_leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 创建包含两个叶子的新节点
        let new_child = PersistentHOTNode::two_leaves(
            existing_key,
            existing_leaf_id,
            new_key,
            new_leaf_id,
        );
        let new_child_id = new_child.compute_node_id::<H>(version);
        self.store.put_node(&new_child_id, &new_child)?;

        // 更新父节点：将叶子替换为内部节点
        let mut new_parent = parent_node.clone();
        new_parent.children[affected_index] = ChildRef::Internal(new_child_id.clone());

        // 更新父节点高度
        new_parent.height = std::cmp::max(parent_node.height, new_child.height + 1);

        let new_parent_id = new_parent.compute_node_id::<H>(version);
        self.store.put_node(&new_parent_id, &new_parent)?;

        Ok(new_parent_id)
    }

    /// 向节点添加新 entry（带栈支持）
    fn add_entry_to_node_with_stack(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: PersistentHOTNode,
        key: &[u8; 32],
        dense_key: u32,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 检查节点是否已满
        if node.len() >= 32 {
            // 节点溢出：需要 Split + Parent Pull Up / Intermediate Node Creation
            return self.handle_overflow_with_stack(
                stack,
                current_id,
                &node,
                key,
                leaf_id,
                version,
            );
        }

        // 找到 affected entry
        let affected_index = self.find_affected_entry(&node, dense_key)
            .expect("HOT invariant violated: no matching entry found");
        let affected_child = &node.children[affected_index];

        // 获取 affected entry 的 key
        let affected_key = self.get_entry_key(affected_child)?;

        // 找到 diff bit
        let diff_bit = find_first_differing_bit(&affected_key, key)
            .expect("Keys must be different");
        let new_bit_value = extract_bit(key, diff_bit);

        // 使用 with_new_entry 创建新节点
        let new_node = node.with_new_entry(
            diff_bit,
            new_bit_value,
            affected_index,
            ChildRef::Leaf(leaf_id),
        );

        let new_node_id = new_node.compute_node_id::<H>(version);
        self.store.put_node(&new_node_id, &new_node)?;
        Ok(new_node_id)
    }

    /// 处理节点溢出（带栈支持的完整 Parent Pull Up / Intermediate Node Creation）
    ///
    /// # 逻辑
    ///
    /// 1. Split 当前节点
    /// 2. 在目标子节点完成叶子插入
    /// 3. 创建 BiNode
    /// 4. 检查父节点高度决定策略：
    ///    - `bi_node.height == parent.height` → Parent Pull Up
    ///    - `bi_node.height < parent.height` → Intermediate Node Creation
    /// 5. Parent Pull Up 可能递归（父节点也满了）
    fn handle_overflow_with_stack(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        _current_id: NodeId,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // Step 1: Split 当前节点
        let (disc_bit, left_node, right_node) = node.split();

        // Step 2: 确定新 entry 插入哪侧
        let new_bit_value = extract_bit(key, disc_bit);
        let (target_node, other_node) = if new_bit_value {
            (right_node, left_node)
        } else {
            (left_node, right_node)
        };

        // Step 3: 持久化另一侧节点
        let other_id = other_node.compute_node_id::<H>(version);
        let other_height = other_node.height;
        self.store.put_node(&other_id, &other_node)?;

        // Step 4: 在目标节点完成插入
        let new_target_id = self.insert_into_split_child(&target_node, key, leaf_id, version)?;

        // 获取更新后的目标子节点高度
        let new_target_node = self.store.get_node(&new_target_id)?
            .expect("Just inserted node should exist");
        let new_target_height = new_target_node.height;

        // Step 5: 创建 BiNode
        let (final_left_id, final_right_id) = if new_bit_value {
            (other_id.clone(), new_target_id)
        } else {
            (new_target_id, other_id.clone())
        };

        let max_child_height = std::cmp::max(new_target_height, other_height);
        // BiNode.height = 子节点最大高度 + 1（与 C++ HOTSingleThreadedNode.hpp L560 一致）
        // 这表示"如果将 BiNode 实体化为节点，该节点的高度"
        let mut bi_node = BiNode {
            discriminative_bit: disc_bit,
            left: final_left_id,
            right: final_right_id,
            height: max_child_height + 1,
        };

        // Step 6: 向上处理 - Parent Pull Up 或 Intermediate Node Creation
        while let Some(parent_entry) = stack.pop() {
            let parent = &parent_entry.node;

            if bi_node.height == parent.height {
                // ===== PARENT PULL UP =====
                let new_parent = parent.with_integrated_binode(parent_entry.child_index, &bi_node);

                if new_parent.is_full() {
                    // 父节点也满了，继续 split 并递归
                    let (d, l, r) = new_parent.split();
                    let l_id = l.compute_node_id::<H>(version);
                    let r_id = r.compute_node_id::<H>(version);
                    self.store.put_node(&l_id, &l)?;
                    self.store.put_node(&r_id, &r)?;

                    let l_height = l.height;
                    let r_height = r.height;
                    bi_node = BiNode {
                        discriminative_bit: d,
                        left: l_id,
                        right: r_id,
                        height: std::cmp::max(l_height, r_height) + 1,
                    };
                    // 继续 while 循环处理上层
                } else {
                    // 父节点未满，存储并结束
                    let new_parent_id = new_parent.compute_node_id::<H>(version);
                    self.store.put_node(&new_parent_id, &new_parent)?;

                    // 向上传播指针更新（take stack 避免 clone 开销）
                    self.propagate_pointer_updates(std::mem::take(stack), new_parent_id.clone(), version)?;
                    return Ok(new_parent_id);
                }
            } else {
                // ===== INTERMEDIATE NODE CREATION =====
                // bi_node.height < parent.height
                let intermediate = bi_node.to_two_entry_node();
                let intermediate_id = intermediate.compute_node_id::<H>(version);
                self.store.put_node(&intermediate_id, &intermediate)?;

                // 更新父节点的 child 引用
                let mut new_parent = parent.clone();
                new_parent.children[parent_entry.child_index] =
                    ChildRef::Internal(intermediate_id.clone());

                // 更新父节点高度
                new_parent.height =
                    std::cmp::max(new_parent.height, intermediate.height + 1);

                let new_parent_id = new_parent.compute_node_id::<H>(version);
                self.store.put_node(&new_parent_id, &new_parent)?;

                // 向上传播指针更新（take stack 避免 clone 开销）
                self.propagate_pointer_updates(std::mem::take(stack), new_parent_id.clone(), version)?;
                return Ok(new_parent_id);
            }
        }

        // Step 7: 到达 root 且仍需处理，创建新 root
        let new_root = bi_node.to_two_entry_node();
        let new_root_id = new_root.compute_node_id::<H>(version);
        self.store.put_node(&new_root_id, &new_root)?;
        self.root_id = Some(new_root_id.clone());

        Ok(new_root_id)
    }

    /// 插入到 split 后的子节点
    fn insert_into_split_child(
        &mut self,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 如果子节点为空，创建单叶子节点
        if node.len() == 0 {
            let new_node = PersistentHOTNode::single_leaf(leaf_id);
            let new_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_id, &new_node)?;
            return Ok(new_id);
        }

        if node.len() == 1 {
            // 只有一个 entry
            let existing_key = self.get_entry_key(&node.children[0])?;
            if &existing_key == key {
                // 相同 key，替换
                let mut new_node = node.clone();
                new_node.children[0] = ChildRef::Leaf(leaf_id);
                let new_id = new_node.compute_node_id::<H>(version);
                self.store.put_node(&new_id, &new_node)?;
                return Ok(new_id);
            }

            // 不同 key，创建两叶子节点
            match &node.children[0] {
                ChildRef::Leaf(existing_leaf_id) => {
                    let new_node = PersistentHOTNode::two_leaves(
                        &existing_key,
                        existing_leaf_id.clone(),
                        key,
                        leaf_id,
                    );
                    let new_id = new_node.compute_node_id::<H>(version);
                    self.store.put_node(&new_id, &new_node)?;
                    return Ok(new_id);
                }
                ChildRef::Internal(child_id) => {
                    // 与 Leaf 类似，创建包含旧 Internal 和新 Leaf 的两 entry 节点
                    // （与 C++ HOTSingleThreadedNode.hpp L533-536 一致）

                    // 获取 Internal 子节点的高度
                    let child_node = self.store.get_node(child_id)?
                        .expect("Internal child should exist");
                    let child_height = child_node.height;

                    // 找到 diff bit
                    let diff_bit = find_first_differing_bit(&existing_key, key)
                        .expect("Keys must be different");

                    // 根据新 key 在 diff_bit 上的值确定排序
                    // bit=0 的 entry 在前（sparse_key=0），bit=1 的在后（sparse_key=1）
                    let new_bit_value = extract_bit(key, diff_bit);
                    let (first_child, second_child) = if new_bit_value {
                        // key 的 bit=1，所以 Leaf 在后，Internal 在前
                        (ChildRef::Internal(child_id.clone()), ChildRef::Leaf(leaf_id))
                    } else {
                        // key 的 bit=0，所以 Leaf 在前，Internal 在后
                        (ChildRef::Leaf(leaf_id), ChildRef::Internal(child_id.clone()))
                    };

                    // 创建节点：height = max(child_height, 0) + 1 = child_height + 1
                    // （Leaf 的 height=0，Internal 的 height=child_height）
                    let mut new_node = PersistentHOTNode::empty(child_height + 1);
                    new_node.extraction_masks = PersistentHOTNode::masks_from_bits(&[diff_bit]);
                    new_node.sparse_partial_keys[0] = 0;
                    new_node.sparse_partial_keys[1] = 1;
                    new_node.children = vec![first_child, second_child];

                    let new_id = new_node.compute_node_id::<H>(version);
                    self.store.put_node(&new_id, &new_node)?;
                    return Ok(new_id);
                }
            }
        }

        // 正常情况：直接添加 entry（split 后节点最多约 16 entries）
        let dense_key = node.extract_dense_partial_key(key);
        let affected_index = self.find_affected_entry(node, dense_key)
            .expect("HOT invariant violated: no matching entry found");
        let affected_child = &node.children[affected_index];
        let affected_key = self.get_entry_key(affected_child)?;

        let diff_bit = find_first_differing_bit(&affected_key, key)
            .expect("Keys must be different");
        let new_bit_value = extract_bit(key, diff_bit);

        let new_node = node.with_new_entry(
            diff_bit,
            new_bit_value,
            affected_index,
            ChildRef::Leaf(leaf_id),
        );
        let new_id = new_node.compute_node_id::<H>(version);
        self.store.put_node(&new_id, &new_node)?;

        Ok(new_id)
    }

    /// 向上传播指针更新
    ///
    /// 从栈中依次取出父节点，更新其 child 引用
    fn propagate_pointer_updates(
        &mut self,
        mut stack: Vec<InsertStackEntry>,
        mut new_child_id: NodeId,
        version: u64,
    ) -> Result<()> {
        while let Some(entry) = stack.pop() {
            // 更新父节点的 child 引用
            let mut new_node = entry.node.clone();
            new_node.children[entry.child_index] = ChildRef::Internal(new_child_id.clone());

            // 读取新子节点获取高度（用于维护 height 不变量）
            if let Ok(Some(child)) = self.store.get_node(&new_child_id) {
                new_node.height = std::cmp::max(new_node.height, child.height + 1);
            }

            let new_node_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_node_id, &new_node)?;
            new_child_id = new_node_id;
        }

        // 更新根节点
        self.root_id = Some(new_child_id);
        Ok(())
    }

    /// 找到 affected entry 索引
    ///
    /// 使用 sparse matching 找到最后一个 (dense & sparse) == sparse 的 entry。
    /// 按照 HOT 设计，应该总是能找到匹配（至少 sparse=0 总是匹配）。
    /// 返回 None 表示数据结构不一致。
    fn find_affected_entry(&self, node: &PersistentHOTNode, dense_key: u32) -> Option<usize> {
        // 使用 sparse matching 找到最后一个 (dense & sparse) == sparse 的 entry
        for i in (0..node.len()).rev() {
            let sparse = node.sparse_partial_keys[i];
            if (dense_key & sparse) == sparse {
                return Some(i);
            }
        }
        None // 数据结构不一致
    }

    /// 获取 entry 对应的 key
    fn get_entry_key(&self, child: &ChildRef) -> Result<[u8; 32]> {
        match child {
            ChildRef::Leaf(leaf_id) => {
                let leaf = self
                    .store
                    .get_leaf(leaf_id)?
                    .ok_or(StoreError::NotFound)?;
                Ok(leaf.key)
            }
            ChildRef::Internal(node_id) => {
                // 对于内部节点，递归获取第一个叶子的 key
                let node = self
                    .store
                    .get_node(node_id)?
                    .ok_or(StoreError::NotFound)?;
                if node.len() > 0 {
                    self.get_entry_key(&node.children[0])
                } else {
                    Err(StoreError::NotFound)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Blake3Hasher;
    use crate::node::{LeafData, PersistentHOTNode};
    use crate::store::MemoryNodeStore;

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
            assert!(
                result.is_some(),
                "Key {} not found after overflow",
                i
            );
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
}
