//! Overflow 处理（Split / Parent Pull Up / Intermediate Node Creation）

use crate::hash::Hasher;
use crate::node::{BiNode, InsertInformation, NodeId, PersistentHOTNode};
use crate::store::{NodeStore, Result};

use super::core::{HOTTree, InsertStackEntry};

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
    /// 处理 Normal Insert 时的节点溢出
    ///
    /// 使用 C++ 风格的 split_with_insert 同时完成 split 和 insert。
    ///
    /// 注意：此函数内部完成所有指针传播，调用者无需再调用 propagate_pointer_updates。
    pub(super) fn handle_overflow_normal_insert(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: &PersistentHOTNode,
        _key: &[u8; 32],
        insert_info: &InsertInformation,
        leaf_id: NodeId,
    ) -> Result<()> {
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

            return self.integrate_binode_upwards(stack, &mut bi_node);
        }

        // Step 1: 使用 split_with_insert 同时完成 split 和 insert（C++ 风格）
        let (disc_bit, left_child, right_child) = node.split_with_insert(
            insert_info.discriminative_bit,
            insert_info.new_bit_value,
            insert_info.first_index_in_affected_subtree,
            insert_info.number_entries_in_affected_subtree,
            insert_info.subtree_prefix_partial_key,
            leaf_id,
        );

        // Step 2: 持久化两侧节点
        let (left_id, left_height) =
            self.materialize_split_child_with_height(left_child)?;
        let (right_id, right_height) =
            self.materialize_split_child_with_height(right_child)?;

        // Step 3: 创建 BiNode
        let max_child_height = std::cmp::max(left_height, right_height);
        let mut bi_node = BiNode {
            discriminative_bit: disc_bit,
            left: left_id,
            right: right_id,
            height: max_child_height + 1,
        };

        // Step 4: 向上处理（复用现有逻辑）
        self.integrate_binode_upwards(stack, &mut bi_node)
    }

    /// 将 BiNode 向上集成到父节点（提取自 handle_overflow_with_stack）
    ///
    /// 注意：此函数内部完成所有指针传播，调用者无需再调用 propagate_pointer_updates。
    /// 对齐 C++ integrateBiNodeIntoTree：返回 void，所有更新原地完成。
    pub(super) fn integrate_binode_upwards(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        bi_node: &mut BiNode,
    ) -> Result<()> {
        while let Some(parent_entry) = stack.pop() {
            let parent = &parent_entry.node;

            if bi_node.height == parent.height {
                // Parent Pull Up
                if parent.is_full() {
                    let (d, l, r) =
                        parent.split_with_binode(parent_entry.child_index, bi_node);
                    let (l_id, l_height) =
                        self.materialize_split_child_with_height(l)?;
                    let (r_id, r_height) =
                        self.materialize_split_child_with_height(r)?;

                    *bi_node = BiNode {
                        discriminative_bit: d,
                        left: l_id,
                        right: r_id,
                        height: std::cmp::max(l_height, r_height) + 1,
                    };
                } else {
                    // C++ 对齐：父节点未满时直接集成，即使变成 32 entries 也不继续 split
                    // 只有下次插入时发现已满才需要 split
                    let new_parent =
                        parent.with_integrated_binode(parent_entry.child_index, bi_node);

                    let new_parent_id = new_parent.compute_node_id::<H>(self.version);
                    self.store.put_node(&new_parent_id, &new_parent)?;
                    self.propagate_pointer_updates(
                        std::mem::take(stack),
                        new_parent_id,
                    )?;
                    return Ok(());
                }
            } else {
                // Intermediate Node Creation
                let intermediate = bi_node.to_two_entry_node();
                let intermediate_id = intermediate.compute_node_id::<H>(self.version);
                self.store.put_node(&intermediate_id, &intermediate)?;

                let mut new_parent = parent.clone();
                new_parent.children[parent_entry.child_index] = intermediate_id;
                new_parent.height = std::cmp::max(new_parent.height, intermediate.height + 1);

                let new_parent_id = new_parent.compute_node_id::<H>(self.version);
                self.store.put_node(&new_parent_id, &new_parent)?;
                self.propagate_pointer_updates(
                    std::mem::take(stack),
                    new_parent_id,
                )?;
                return Ok(());
            }
        }

        // 创建新 root
        let new_root = bi_node.to_two_entry_node();
        let new_root_id = new_root.compute_node_id::<H>(self.version);
        self.store.put_node(&new_root_id, &new_root)?;
        self.root_id = Some(new_root_id);
        Ok(())
    }

    /// 处理节点溢出（带栈支持的完整 Parent Pull Up / Intermediate Node Creation）
    ///
    /// 使用 C++ 风格的 split_with_insert 同时完成 split 和 insert。
    ///
    /// 注意：此函数内部完成所有指针传播，调用者无需再调用 propagate_pointer_updates。
    /// 对齐 C++ integrateBiNodeIntoTree：返回 void，所有更新原地完成。
    ///
    /// # 逻辑
    ///
    /// 0. 若 disc_bit <= first_discriminative_bit，直接创建 BiNode（不 split）
    /// 1. 使用 split_with_insert 同时 split 并 insert
    /// 2. 持久化两侧节点
    /// 3. 创建 BiNode
    /// 4. 检查父节点高度决定策略：
    ///    - `bi_node.height == parent.height` → Parent Pull Up
    ///    - `bi_node.height < parent.height` → Intermediate Node Creation
    /// 5. Parent Pull Up 可能递归（父节点也满了）
    pub(super) fn handle_overflow_with_stack(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: &PersistentHOTNode,
        disc_bit: u16,
        new_bit_value: bool,
        first_affected_index: usize,
        num_affected_entries: usize,
        subtree_prefix: u32,
        leaf_id: NodeId,
    ) -> Result<()> {
        // Step 0: C++ 对齐 - 若新 discriminative bit 更靠前，则直接创建 BiNode
        // 与 handle_overflow_normal_insert 一致
        let first_bit = node
            .first_discriminative_bit()
            .expect("Cannot overflow insert into node with span=0");
        if disc_bit <= first_bit {
            let existing_id = current_id;
            let bi_node_height = node.height + 1;

            let (left, right) = if new_bit_value {
                (existing_id, leaf_id)
            } else {
                (leaf_id, existing_id)
            };

            let mut bi_node = BiNode {
                discriminative_bit: disc_bit,
                left,
                right,
                height: bi_node_height,
            };

            return self.integrate_binode_upwards(stack, &mut bi_node);
        }

        // Step 1: 使用 split_with_insert 同时完成 split 和 insert（C++ 风格）
        let (split_bit, left_child, right_child) = node.split_with_insert(
            disc_bit,
            new_bit_value,
            first_affected_index,
            num_affected_entries,
            subtree_prefix,
            leaf_id,
        );

        // Step 2: 持久化两侧节点
        let (left_id, left_height) =
            self.materialize_split_child_with_height(left_child)?;
        let (right_id, right_height) =
            self.materialize_split_child_with_height(right_child)?;

        // Step 3: 创建 BiNode
        let max_child_height = std::cmp::max(left_height, right_height);
        // BiNode.height = 子节点最大高度 + 1（与 C++ HOTSingleThreadedNode.hpp L560 一致）
        let mut bi_node = BiNode {
            discriminative_bit: split_bit,
            left: left_id,
            right: right_id,
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
                        self.materialize_split_child_with_height(l)?;
                    let (r_id, r_height) =
                        self.materialize_split_child_with_height(r)?;
                    bi_node = BiNode {
                        discriminative_bit: d,
                        left: l_id,
                        right: r_id,
                        height: std::cmp::max(l_height, r_height) + 1,
                    };
                    // 继续 while 循环处理上层
                } else {
                    // C++ 对齐：父节点未满时直接集成，即使变成 32 entries 也不继续 split
                    // 只有下次插入时发现已满才需要 split
                    let new_parent =
                        parent.with_integrated_binode(parent_entry.child_index, &bi_node);

                    let new_parent_id = new_parent.compute_node_id::<H>(self.version);
                    self.store.put_node(&new_parent_id, &new_parent)?;

                    // 向上传播指针更新（take stack 避免 clone 开销）
                    self.propagate_pointer_updates(
                        std::mem::take(stack),
                        new_parent_id,
                    )?;
                    return Ok(());
                }
            } else {
                // ===== INTERMEDIATE NODE CREATION =====
                // bi_node.height < parent.height
                let intermediate = bi_node.to_two_entry_node();
                let intermediate_id = intermediate.compute_node_id::<H>(self.version);
                self.store.put_node(&intermediate_id, &intermediate)?;

                // 更新父节点的 child 引用
                let mut new_parent = parent.clone();
                new_parent.children[parent_entry.child_index] = intermediate_id;

                // 更新父节点高度
                new_parent.height = std::cmp::max(new_parent.height, intermediate.height + 1);

                let new_parent_id = new_parent.compute_node_id::<H>(self.version);
                self.store.put_node(&new_parent_id, &new_parent)?;

                // 向上传播指针更新（take stack 避免 clone 开销）
                self.propagate_pointer_updates(
                    std::mem::take(stack),
                    new_parent_id,
                )?;
                return Ok(());
            }
        }

        // Step 7: 到达 root 且仍需处理，创建新 root
        let new_root = bi_node.to_two_entry_node();
        let new_root_id = new_root.compute_node_id::<H>(self.version);
        self.store.put_node(&new_root_id, &new_root)?;
        self.root_id = Some(new_root_id);

        Ok(())
    }
}
