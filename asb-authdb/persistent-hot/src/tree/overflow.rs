//! Overflow 处理（Split / Parent Pull Up / Intermediate Node Creation）

use crate::hash::Hasher;
use crate::node::{
    extract_bit, find_first_differing_bit, BiNode, InsertInformation, NodeId, PersistentHOTNode,
    SearchResult, SplitChild,
};
use crate::store::{NodeStore, Result, StoreError};

use super::core::{HOTTree, InsertStackEntry};

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 处理 Normal Insert 时的节点溢出
    ///
    /// Split 当前节点，在目标子节点中重新计算 InsertInformation 并添加 entry。
    pub(super) fn handle_overflow_normal_insert(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        insert_info: &InsertInformation,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // Step 0: C++ 对齐 - 若新 discriminative bit 更靠前，则直接创建 BiNode
        let first_bit = node
            .first_discriminative_bit()
            .expect("Cannot overflow insert into node with span=0");
        if insert_info.discriminative_bit <= first_bit {
            let existing_id = current_id;
            let bi_node_height = node.height + 1;

            let (left, right) = if insert_info.new_bit_value {
                (existing_id, leaf_id)
            } else {
                (leaf_id, existing_id)
            };

            let mut bi_node = BiNode {
                discriminative_bit: insert_info.discriminative_bit,
                left,
                right,
                height: bi_node_height,
            };

            return self.integrate_binode_upwards(stack, &mut bi_node, version);
        }

        // Step 1: Split 当前节点
        let (disc_bit, left_node, right_node) = node.split();

        // Step 2: 使用 key 的 disc_bit 确定新 entry 插入哪侧
        let goes_right = extract_bit(key, disc_bit);

        let (target_node, other_node) = if goes_right {
            (right_node, left_node)
        } else {
            (left_node, right_node)
        };

        // Step 3: 持久化另一侧节点
        let (other_id, other_height) =
            self.materialize_split_child_with_height(other_node, version)?;

        // Step 4: 在目标节点添加新 entry
        // 使用 key 在目标子节点中重新计算 InsertInformation
        let new_target_id = self.insert_into_split_child_ref(
            target_node,
            key,
            insert_info.discriminative_bit,
            insert_info.new_bit_value,
            leaf_id,
            version,
        )?;

        // 获取更新后的目标子节点高度
        let new_target_height = self.get_child_height(&new_target_id)?;

        // Step 5: 创建 BiNode
        let (final_left_id, final_right_id) = if goes_right {
            (other_id.clone(), new_target_id)
        } else {
            (new_target_id, other_id.clone())
        };

        let max_child_height = std::cmp::max(new_target_height, other_height);
        let mut bi_node = BiNode {
            discriminative_bit: disc_bit,
            left: final_left_id,
            right: final_right_id,
            height: max_child_height + 1,
        };

        // Step 6: 向上处理（复用现有逻辑）
        self.integrate_binode_upwards(stack, &mut bi_node, version)
    }

    /// 在 split 后的子节点中添加 entry
    ///
    /// 使用 key 在子节点中搜索并确定插入逻辑。
    pub(super) fn insert_into_split_child_ref(
        &mut self,
        split_child: SplitChild,
        key: &[u8; 32],
        orig_disc_bit: u16,
        orig_new_bit_value: bool,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        match split_child {
            SplitChild::Node(node) => self.insert_into_split_child(
                &node,
                key,
                orig_disc_bit,
                orig_new_bit_value,
                leaf_id,
                version,
            ),
            SplitChild::Existing(child_id) => match child_id {
                NodeId::Leaf(_) => {
                    let existing_key = self.get_entry_key(&child_id)?;
                    let new_node = PersistentHOTNode::two_leaves(&existing_key, child_id, key, leaf_id);
                    let new_node_id = new_node.compute_node_id::<H>(version);
                    self.store.put_node(&new_node_id, &new_node)?;
                    Ok(new_node_id)
                }
                NodeId::Internal(_) => {
                    let child_node = self
                        .store
                        .get_node(&child_id)?
                        .ok_or(StoreError::NotFound)?;
                    self.insert_into_split_child(
                        &child_node,
                        key,
                        orig_disc_bit,
                        orig_new_bit_value,
                        leaf_id,
                        version,
                    )
                }
            },
        }
    }

    pub(super) fn insert_into_split_child(
        &mut self,
        node: &PersistentHOTNode,
        key: &[u8; 32],
        orig_disc_bit: u16,
        orig_new_bit_value: bool,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<NodeId> {
        // 如果子节点为空，直接创建单叶子节点
        if node.len() == 0 {
            let new_node = PersistentHOTNode::single_leaf(leaf_id);
            let new_node_id = new_node.compute_node_id::<H>(version);
            self.store.put_node(&new_node_id, &new_node)?;
            return Ok(new_node_id);
        }

        // 在子节点中搜索
        match node.search(key) {
            SearchResult::Found { index } => {
                // 在子节点中重新计算 InsertInformation
                let new_info =
                    node.get_insert_information(index, orig_disc_bit, orig_new_bit_value);

                if node.len() >= 32 {
                    // 子节点也满了（罕见），递归处理
                    // 简化：直接 split 并创建中间节点
                    let (d, l, r) = node.split();

                    let goes_right = extract_bit(key, d);
                    let (target, other) = if goes_right { (r, l) } else { (l, r) };
                    let other_id = self.materialize_split_child(other, version)?;

                    // 在目标子节点中递归
                    let target_id = self.insert_into_split_child_ref(
                        target,
                        key,
                        orig_disc_bit,
                        orig_new_bit_value,
                        leaf_id,
                        version,
                    )?;

                    // 创建中间节点
                    let (final_left, final_right) = if goes_right {
                        (other_id, target_id)
                    } else {
                        (target_id, other_id)
                    };

                    let intermediate = BiNode {
                        discriminative_bit: d,
                        left: final_left.clone(),
                        right: final_right.clone(),
                        height: node.height, // 保守估计
                    }
                    .to_two_entry_node();

                    let intermediate_id = intermediate.compute_node_id::<H>(version);
                    self.store.put_node(&intermediate_id, &intermediate)?;
                    return Ok(intermediate_id);
                }

                // 使用 with_new_entry_from_info 添加 entry
                let new_node = node.with_new_entry_from_info(&new_info, leaf_id);
                let new_node_id = new_node.compute_node_id::<H>(version);
                self.store.put_node(&new_node_id, &new_node)?;
                Ok(new_node_id)
            }
            SearchResult::NotFound { dense_key } => {
                // 没有直接匹配：找到 affected entry 并添加
                if node.len() >= 32 {
                    // 满了，需要 split
                    let (d, l, r) = node.split();

                    let goes_right = extract_bit(key, d);
                    let (target, other) = if goes_right { (r, l) } else { (l, r) };
                    let other_id = self.materialize_split_child(other, version)?;

                    let target_id = self.insert_into_split_child_ref(
                        target,
                        key,
                        orig_disc_bit,
                        orig_new_bit_value,
                        leaf_id,
                        version,
                    )?;

                    let (final_left, final_right) = if goes_right {
                        (other_id, target_id)
                    } else {
                        (target_id, other_id)
                    };

                    let intermediate = BiNode {
                        discriminative_bit: d,
                        left: final_left.clone(),
                        right: final_right.clone(),
                        height: node.height,
                    }
                    .to_two_entry_node();

                    let intermediate_id = intermediate.compute_node_id::<H>(version);
                    self.store.put_node(&intermediate_id, &intermediate)?;
                    return Ok(intermediate_id);
                }

                // 找到 affected entry 并计算 diff bit
                let affected_index = self
                    .find_affected_entry(node, dense_key)
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
                    leaf_id,
                );
                let new_node_id = new_node.compute_node_id::<H>(version);
                self.store.put_node(&new_node_id, &new_node)?;
                Ok(new_node_id)
            }
        }
    }

    /// 将 BiNode 向上集成到父节点（提取自 handle_overflow_with_stack）
    pub(super) fn integrate_binode_upwards(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        bi_node: &mut BiNode,
        version: u64,
    ) -> Result<NodeId> {
        while let Some(parent_entry) = stack.pop() {
            let parent = &parent_entry.node;

            if bi_node.height == parent.height {
                // Parent Pull Up
                if parent.is_full() {
                    let (d, l, r) =
                        parent.split_with_binode(parent_entry.child_index, bi_node);
                    let (l_id, l_height) =
                        self.materialize_split_child_with_height(l, version)?;
                    let (r_id, r_height) =
                        self.materialize_split_child_with_height(r, version)?;

                    *bi_node = BiNode {
                        discriminative_bit: d,
                        left: l_id,
                        right: r_id,
                        height: std::cmp::max(l_height, r_height) + 1,
                    };
                } else {
                    let new_parent =
                        parent.with_integrated_binode(parent_entry.child_index, bi_node);

                    if new_parent.is_full() {
                        let (d, l, r) = new_parent.split();
                        let (l_id, l_height) =
                            self.materialize_split_child_with_height(l, version)?;
                        let (r_id, r_height) =
                            self.materialize_split_child_with_height(r, version)?;

                        *bi_node = BiNode {
                            discriminative_bit: d,
                            left: l_id,
                            right: r_id,
                            height: std::cmp::max(l_height, r_height) + 1,
                        };
                    } else {
                        let new_parent_id = new_parent.compute_node_id::<H>(version);
                        self.store.put_node(&new_parent_id, &new_parent)?;
                        self.propagate_pointer_updates(
                            std::mem::take(stack),
                            new_parent_id.clone(),
                            version,
                        )?;
                        return Ok(new_parent_id);
                    }
                }
            } else {
                // Intermediate Node Creation
                let intermediate = bi_node.to_two_entry_node();
                let intermediate_id = intermediate.compute_node_id::<H>(version);
                self.store.put_node(&intermediate_id, &intermediate)?;

                let mut new_parent = parent.clone();
                new_parent.children[parent_entry.child_index] = intermediate_id;
                new_parent.height = std::cmp::max(new_parent.height, intermediate.height + 1);

                let new_parent_id = new_parent.compute_node_id::<H>(version);
                self.store.put_node(&new_parent_id, &new_parent)?;
                self.propagate_pointer_updates(
                    std::mem::take(stack),
                    new_parent_id.clone(),
                    version,
                )?;
                return Ok(new_parent_id);
            }
        }

        // 创建新 root
        let new_root = bi_node.to_two_entry_node();
        let new_root_id = new_root.compute_node_id::<H>(version);
        self.store.put_node(&new_root_id, &new_root)?;
        self.root_id = Some(new_root_id.clone());
        Ok(new_root_id)
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
    pub(super) fn handle_overflow_with_stack(
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
        let (other_id, other_height) =
            self.materialize_split_child_with_height(other_node, version)?;

        // Step 4: 在目标节点完成插入
        // 注意：这是从 NotFound 分支来的，所以子节点搜索也会走 NotFound，
        // orig_disc_bit 和 orig_new_bit_value 不会被使用（传占位符值）
        let new_target_id =
            self.insert_into_split_child_ref(target_node, key, 0, false, leaf_id, version)?;

        // 获取更新后的目标子节点高度
        let new_target_height = self.get_child_height(&new_target_id)?;

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
                // 修复：先检查父节点是否已满，避免 with_integrated_binode 越界
                // 对应 C++ HOTSingleThreaded.hpp L516-536
                if parent.is_full() {
                    // 父节点已满：使用 split_with_binode 同时 split 并集成 BiNode
                    let (d, l, r) =
                        parent.split_with_binode(parent_entry.child_index, &bi_node);
                    let (l_id, l_height) =
                        self.materialize_split_child_with_height(l, version)?;
                    let (r_id, r_height) =
                        self.materialize_split_child_with_height(r, version)?;
                    bi_node = BiNode {
                        discriminative_bit: d,
                        left: l_id,
                        right: r_id,
                        height: std::cmp::max(l_height, r_height) + 1,
                    };
                    // 继续 while 循环处理上层
                } else {
                    // 父节点未满：直接集成 BiNode
                    let new_parent =
                        parent.with_integrated_binode(parent_entry.child_index, &bi_node);

                    if new_parent.is_full() {
                        // 集成后变满，继续 split 并递归
                        let (d, l, r) = new_parent.split();
                        let (l_id, l_height) =
                            self.materialize_split_child_with_height(l, version)?;
                        let (r_id, r_height) =
                            self.materialize_split_child_with_height(r, version)?;
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
                        self.propagate_pointer_updates(
                            std::mem::take(stack),
                            new_parent_id.clone(),
                            version,
                        )?;
                        return Ok(new_parent_id);
                    }
                }
            } else {
                // ===== INTERMEDIATE NODE CREATION =====
                // bi_node.height < parent.height
                let intermediate = bi_node.to_two_entry_node();
                let intermediate_id = intermediate.compute_node_id::<H>(version);
                self.store.put_node(&intermediate_id, &intermediate)?;

                // 更新父节点的 child 引用
                let mut new_parent = parent.clone();
                new_parent.children[parent_entry.child_index] = intermediate_id;

                // 更新父节点高度
                new_parent.height = std::cmp::max(new_parent.height, intermediate.height + 1);

                let new_parent_id = new_parent.compute_node_id::<H>(version);
                self.store.put_node(&new_parent_id, &new_parent)?;

                // 向上传播指针更新（take stack 避免 clone 开销）
                self.propagate_pointer_updates(
                    std::mem::take(stack),
                    new_parent_id.clone(),
                    version,
                )?;
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
}
