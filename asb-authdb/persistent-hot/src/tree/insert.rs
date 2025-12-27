//! 插入操作

use crate::hash::Hasher;
use crate::node::{
    extract_bit, find_first_differing_bit, BiNode, InsertInformation, LeafData, NodeId,
    PersistentHOTNode, SearchResult,
};
use crate::store::{NodeStore, Result, StoreError};

use super::core::{HOTTree, InsertStackEntry};

impl<S: NodeStore, H: Hasher> HOTTree<S, H> {
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
    pub(super) fn insert_with_stack(
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
                    let child_ref = node.children[index];

                    // 获取 affected entry 的 key 以计算 diff bit
                    let affected_key = self.get_entry_key(&child_ref)?;

                    // 检查是否相同 key
                    if &affected_key == key {
                        // 相同 key：替换值
                        match child_ref {
                            NodeId::Leaf(_) => {
                                // 直接替换叶子
                                let mut new_node = node.clone();
                                new_node.children[index] = leaf_id;
                                let new_node_id = new_node.compute_node_id::<H>(version);
                                self.store.put_node(&new_node_id, &new_node)?;
                                return self.propagate_pointer_updates(stack, new_node_id, version);
                            }
                            NodeId::Internal(_) => {
                                // 递归进入子节点替换
                                stack.push(InsertStackEntry {
                                    node_id: current_id,
                                    child_index: index,
                                    node,
                                });
                                current_id = child_ref;
                                continue;
                            }
                        }
                    }

                    // 找到 diff bit
                    let diff_bit = find_first_differing_bit(&affected_key, key)
                        .expect("Keys must be different");
                    let new_bit_value = extract_bit(key, diff_bit);

                    // 获取 InsertInformation 来判断 isSingleEntry
                    // 对应 C++ getInsertInformation + isSingleEntry 检查
                    let insert_info = node.get_insert_information(index, diff_bit, new_bit_value);
                    let is_single_entry = insert_info.is_single_entry();
                    let is_leaf_entry = child_ref.is_leaf();

                    if is_single_entry && is_leaf_entry {
                        // ===== CASE 1: Leaf Node Pushdown =====
                        // 受影响子树只有一个 entry，且是叶子（child_ref 已经是 NodeId::Leaf）
                        // 对应 C++ integrateBiNodeIntoTree: 根据 height 判断 Parent Pull Up 或 Intermediate Node Creation
                        return self.leaf_pushdown_with_height_check(
                            &mut stack,
                            current_id,
                            node,
                            index,
                            diff_bit,
                            &affected_key,
                            child_ref, // child_ref 是 NodeId::Leaf
                            key,
                            leaf_id,
                            version,
                        );
                    } else if is_single_entry {
                        // ===== CASE 2: 递归进入子节点 =====
                        // 受影响子树只有一个 entry，但是内部节点（child_ref 是 NodeId::Internal）
                        stack.push(InsertStackEntry {
                            node_id: current_id,
                            child_index: index,
                            node,
                        });
                        current_id = child_ref;
                        continue;
                    } else {
                        // ===== CASE 3: Normal Insert =====
                        // 受影响子树有多个 entries，在当前节点添加新 entry
                        // normal_insert 内部完成所有指针传播
                        return self.normal_insert(
                            &mut stack,
                            current_id,
                            node,
                            key,
                            &insert_info,
                            leaf_id,
                            version,
                        );
                    }
                }
                SearchResult::NotFound { dense_key } => {
                    // 没有匹配的 entry：需要添加新 entry
                    // add_entry_to_node_with_stack 内部完成所有指针传播
                    return self.add_entry_to_node_with_stack(
                        &mut stack,
                        current_id,
                        node,
                        key,
                        dense_key,
                        leaf_id,
                        version,
                    );
                }
            }
        }
    }

    /// Leaf Node Pushdown（对齐 C++ integrateBiNodeIntoTree）
    ///
    /// 根据 height 判断策略：
    /// - `parent.height > bi_node.height` → Intermediate Node Creation
    /// - `parent.height == bi_node.height` → Parent Pull Up（直接在父节点添加 entry）
    ///
    /// # C++ 对应
    ///
    /// ```cpp
    /// if(existingParentNode->mHeight > splitEntries.mHeight) {
    ///     // Intermediate Node Creation
    ///     *insertStack[currentDepth].mChildPointer = createTwoEntriesNode(splitEntries);
    /// } else {
    ///     // Parent Pull Up
    ///     parentNode.addEntry(insertInformation, valueToInsert);
    /// }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub(super) fn leaf_pushdown_with_height_check(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        parent_node: PersistentHOTNode,
        affected_index: usize,
        diff_bit: u16,
        existing_key: &[u8; 32],
        existing_leaf_id: NodeId,
        new_key: &[u8; 32],
        new_leaf_id: NodeId,
        version: u64,
    ) -> Result<()> {
        // BiNode 高度 = max(leaf_height, leaf_height) + 1 = max(0, 0) + 1 = 1
        let bi_node_height: u8 = 1;

        // C++ integrateBiNodeIntoTree 的 height 判断
        if parent_node.height > bi_node_height {
            // ===== Intermediate Node Creation =====
            // parent.height > 1: 创建包含两个叶子的中间节点
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
            new_parent.children[affected_index] = new_child_id;
            // 高度不变（因为有 height gap）

            let new_parent_id = new_parent.compute_node_id::<H>(version);
            self.store.put_node(&new_parent_id, &new_parent)?;
            self.propagate_pointer_updates(std::mem::take(stack), new_parent_id, version)
        } else {
            // ===== Parent Pull Up =====
            // parent.height == 1: 直接在父节点添加新 entry
            //
            // 对齐 C++ integrateBiNodeIntoTree：
            // - newIsRight=true -> 插入 BiNode.right
            // - entryIndex 位置替换为 BiNode.left

            if parent_node.len() < 32 {
                // 父节点未满：两步操作
                let bi_node = BiNode::from_existing_and_new(
                    diff_bit,
                    existing_key,
                    existing_leaf_id,
                    new_leaf_id,
                    bi_node_height,
                );

                // newIsRight=true：使用 bit=1 生成 InsertInformation
                let insert_info = parent_node.get_insert_information(affected_index, diff_bit, true);
                let mut new_node = parent_node.with_new_entry_from_info(&insert_info, bi_node.right);

                // entryOffset=0：替换 entryIndex 位置
                new_node.children[affected_index] = bi_node.left;

                let new_node_id = new_node.compute_node_id::<H>(version);
                self.store.put_node(&new_node_id, &new_node)?;
                self.propagate_pointer_updates(std::mem::take(stack), new_node_id, version)
            } else {
                // 父节点已满：创建 BiNode 并向上处理 overflow
                // 对应 C++ integrateBiNodeIntoTree 中 parentNode.isFull() 分支
                let mut bi_node = BiNode::from_existing_and_new(
                    diff_bit,
                    existing_key,
                    existing_leaf_id.clone(),
                    new_leaf_id.clone(),
                    bi_node_height,
                );

                // 把当前节点 push 到 stack，然后调用 integrate_binode_upwards
                stack.push(InsertStackEntry {
                    node_id: current_id,
                    child_index: affected_index,
                    node: parent_node,
                });

                // integrate_binode_upwards 内部完成所有指针传播，无需再调用 propagate_pointer_updates
                self.integrate_binode_upwards(stack, &mut bi_node, version)
            }
        }
    }

    /// Normal Insert: 在当前节点添加新 entry
    ///
    /// 当 `isSingleEntry == false` 时使用，对应 C++ `insertNewValue`。
    /// 新 key 影响多个 entries，需要在当前节点添加新的 discriminative bit。
    ///
    /// 注意：此函数内部完成所有指针传播，调用者无需再调用 propagate_pointer_updates。
    ///
    /// # 参数
    ///
    /// - `stack`: 插入路径栈
    /// - `current_id`: 当前节点 ID
    /// - `node`: 当前节点
    /// - `key`: 新 key（用于 overflow 时在子节点中重新计算 InsertInformation）
    /// - `insert_info`: 插入信息（包含 affected subtree 信息）
    /// - `leaf_id`: 新叶子的 NodeId
    /// - `version`: 版本号
    pub(super) fn normal_insert(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: PersistentHOTNode,
        key: &[u8; 32],
        insert_info: &InsertInformation,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<()> {
        // 检查节点是否已满
        if node.len() >= 32 {
            // 节点溢出：handle_overflow_normal_insert 内部完成所有更新
            return self.handle_overflow_normal_insert(
                stack,
                current_id,
                &node,
                key,
                insert_info,
                leaf_id,
                version,
            );
        }

        // 使用 with_new_entry_from_info 创建新节点
        // 这会正确更新 affected subtree 中所有 entries 的 sparse key
        let new_node = node.with_new_entry_from_info(insert_info, leaf_id);

        let new_node_id = new_node.compute_node_id::<H>(version);
        self.store.put_node(&new_node_id, &new_node)?;
        // 非 overflow：自己调用 propagate_pointer_updates
        self.propagate_pointer_updates(std::mem::take(stack), new_node_id, version)
    }

    /// 向节点添加新 entry（带栈支持）
    ///
    /// 注意：此函数内部完成所有指针传播，调用者无需再调用 propagate_pointer_updates。
    pub(super) fn add_entry_to_node_with_stack(
        &mut self,
        stack: &mut Vec<InsertStackEntry>,
        current_id: NodeId,
        node: PersistentHOTNode,
        key: &[u8; 32],
        dense_key: u32,
        leaf_id: NodeId,
        version: u64,
    ) -> Result<()> {
        // 先计算 affected_index 和 disc_bit（无论是否 overflow 都需要）
        let affected_index = self
            .find_affected_entry(&node, dense_key)
            .expect("HOT invariant violated: no matching entry found");
        let affected_child = &node.children[affected_index];
        let affected_key = self.get_entry_key(affected_child)?;
        let diff_bit =
            find_first_differing_bit(&affected_key, key).expect("Keys must be different");
        let new_bit_value = extract_bit(key, diff_bit);

        // 检查节点是否已满
        if node.len() >= 32 {
            // 节点溢出：handle_overflow_with_stack 内部完成所有更新
            let insert_info = node.get_insert_information(affected_index, diff_bit, new_bit_value);
            return self.handle_overflow_with_stack(
                stack,
                current_id,
                &node,
                diff_bit,
                new_bit_value,
                insert_info.first_index_in_affected_subtree,
                insert_info.number_entries_in_affected_subtree,
                insert_info.subtree_prefix_partial_key,
                leaf_id,
                version,
            );
        }

        // 使用 with_new_entry 创建新节点
        let new_node = node.with_new_entry(
            diff_bit,
            new_bit_value,
            affected_index,
            leaf_id,
        );

        let new_node_id = new_node.compute_node_id::<H>(version);
        self.store.put_node(&new_node_id, &new_node)?;
        // 非 overflow：自己调用 propagate_pointer_updates
        self.propagate_pointer_updates(std::mem::take(stack), new_node_id, version)
    }
}
