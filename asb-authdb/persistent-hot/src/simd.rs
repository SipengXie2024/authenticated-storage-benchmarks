//! SIMD 搜索模块
//!
//! 提供 AVX2 优化的 sparse partial key 搜索，及软件回退实现。
//!
//! # 搜索算法
//!
//! HOT 使用 sparse matching: `(dense & sparse) == sparse`
//!
//! 由于 entries 按 trie 遍历顺序排列，取最后一个匹配即可。
//!
//! # SIMD 优化
//!
//! 使用 AVX2 一次比较 8 个 u32 partial keys：
//! 1. 广播 dense key 到 ymm 寄存器
//! 2. 加载 8 个 sparse keys
//! 3. AND 操作
//! 4. 比较是否等于 sparse keys
//! 5. 取最后一个匹配

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD 搜索结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimdSearchResult {
    /// 找到匹配，返回索引
    Found(usize),
    /// 未找到匹配
    NotFound,
}

/// 检测 AVX2 支持
#[inline]
pub fn has_avx2() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// SIMD 搜索（自动选择实现）
///
/// # 参数
/// - `sparse_keys`: sparse partial keys 数组（固定 32 个）
/// - `dense_key`: 要搜索的 dense partial key
/// - `len`: 有效 entries 数量
///
/// # 返回
/// - `SimdSearchResult::Found(index)`: 找到匹配的最后一个索引
/// - `SimdSearchResult::NotFound`: 未找到匹配
#[inline]
pub fn simd_search(sparse_keys: &[u32; 32], dense_key: u32, len: u8) -> SimdSearchResult {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: 已检测 AVX2 支持
            unsafe { simd_search_avx2(sparse_keys, dense_key, len) }
        } else {
            simd_search_scalar(sparse_keys, dense_key, len)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        simd_search_scalar(sparse_keys, dense_key, len)
    }
}

/// 软件回退实现
///
/// 简单的线性搜索，找最后一个匹配
#[inline]
pub fn simd_search_scalar(sparse_keys: &[u32; 32], dense_key: u32, len: u8) -> SimdSearchResult {
    let mut last_match: Option<usize> = None;

    for i in 0..len as usize {
        let sparse = sparse_keys[i];
        if (dense_key & sparse) == sparse {
            last_match = Some(i);
        }
    }

    match last_match {
        Some(idx) => SimdSearchResult::Found(idx),
        None => SimdSearchResult::NotFound,
    }
}

/// AVX2 优化实现
///
/// 使用 4 个 AVX2 向量操作覆盖全部 32 个 entries
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn simd_search_avx2(sparse_keys: &[u32; 32], dense_key: u32, len: u8) -> SimdSearchResult {
    // 广播 dense_key 到 8 个 lane
    let dense_vec = _mm256_set1_epi32(dense_key as i32);

    // 构建有效位掩码（哪些 entries 是有效的）
    let valid_mask: u32 = if len >= 32 {
        u32::MAX
    } else {
        (1u32 << len) - 1
    };

    let mut match_mask: u32 = 0;

    // 处理 4 组，每组 8 个 u32
    // 组 0: entries 0-7
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr() as *const __m256i);
        let and_result = _mm256_and_si256(dense_vec, sparse_vec);
        let cmp_result = _mm256_cmpeq_epi32(and_result, sparse_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        match_mask |= mask;
    }

    // 组 1: entries 8-15
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(8) as *const __m256i);
        let and_result = _mm256_and_si256(dense_vec, sparse_vec);
        let cmp_result = _mm256_cmpeq_epi32(and_result, sparse_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        match_mask |= mask << 8;
    }

    // 组 2: entries 16-23
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(16) as *const __m256i);
        let and_result = _mm256_and_si256(dense_vec, sparse_vec);
        let cmp_result = _mm256_cmpeq_epi32(and_result, sparse_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        match_mask |= mask << 16;
    }

    // 组 3: entries 24-31
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(24) as *const __m256i);
        let and_result = _mm256_and_si256(dense_vec, sparse_vec);
        let cmp_result = _mm256_cmpeq_epi32(and_result, sparse_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        match_mask |= mask << 24;
    }

    // 只保留有效 entries 的匹配
    match_mask &= valid_mask;

    if match_mask == 0 {
        SimdSearchResult::NotFound
    } else {
        // 取最高位的 1（最后一个匹配）
        let idx = 31 - match_mask.leading_zeros() as usize;
        SimdSearchResult::Found(idx)
    }
}

/// 批量搜索多个 dense keys
///
/// 用于优化批量查询场景
#[inline]
pub fn simd_batch_search(
    sparse_keys: &[u32; 32],
    dense_keys: &[u32],
    len: u8,
) -> Vec<SimdSearchResult> {
    dense_keys
        .iter()
        .map(|&dk| simd_search(sparse_keys, dk, len))
        .collect()
}

// ============================================================================
// SIMD 插入位置查找
// ============================================================================

/// 找到 sparse_key 应该插入的位置（保持升序）
///
/// 返回第一个大于 sparse_key 的索引，如果都不大于则返回 len
#[inline]
pub fn simd_find_insert_position(sparse_keys: &[u32; 32], sparse_key: u32, len: u8) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { simd_find_insert_position_avx2(sparse_keys, sparse_key, len) }
        } else {
            find_insert_position_scalar(sparse_keys, sparse_key, len)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        find_insert_position_scalar(sparse_keys, sparse_key, len)
    }
}

/// 软件回退实现
#[inline]
pub fn find_insert_position_scalar(sparse_keys: &[u32; 32], sparse_key: u32, len: u8) -> usize {
    for i in 0..len as usize {
        if sparse_keys[i] > sparse_key {
            return i;
        }
    }
    len as usize
}

/// AVX2 优化实现
///
/// 使用无符号比较（通过 XOR 0x80000000 转换为有符号比较）
///
/// AVX2 没有无符号比较指令，但可以通过翻转符号位来模拟：
/// - XOR 0x80000000 后，无符号顺序变成有符号顺序
/// - 例如：0x00000000 → 0x80000000 (最小), 0xFFFFFFFF → 0x7FFFFFFF (最大)
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
unsafe fn simd_find_insert_position_avx2(
    sparse_keys: &[u32; 32],
    sparse_key: u32,
    len: u8,
) -> usize {
    // 用于将无符号比较转换为有符号比较的常量
    let sign_bit = _mm256_set1_epi32(0x80000000u32 as i32);

    // 广播 sparse_key 并翻转符号位
    let key_vec = _mm256_xor_si256(_mm256_set1_epi32(sparse_key as i32), sign_bit);

    // 构建有效位掩码
    let valid_mask: u32 = if len >= 32 {
        u32::MAX
    } else {
        (1u32 << len) - 1
    };

    let mut gt_mask: u32 = 0;

    // 处理 4 组，每组 8 个 u32
    // 找所有 sparse_keys[i] > sparse_key 的位置（无符号比较）
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr() as *const __m256i);
        let sparse_xor = _mm256_xor_si256(sparse_vec, sign_bit);
        let cmp_result = _mm256_cmpgt_epi32(sparse_xor, key_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        gt_mask |= mask;
    }
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(8) as *const __m256i);
        let sparse_xor = _mm256_xor_si256(sparse_vec, sign_bit);
        let cmp_result = _mm256_cmpgt_epi32(sparse_xor, key_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        gt_mask |= mask << 8;
    }
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(16) as *const __m256i);
        let sparse_xor = _mm256_xor_si256(sparse_vec, sign_bit);
        let cmp_result = _mm256_cmpgt_epi32(sparse_xor, key_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        gt_mask |= mask << 16;
    }
    {
        let sparse_vec = _mm256_loadu_si256(sparse_keys.as_ptr().add(24) as *const __m256i);
        let sparse_xor = _mm256_xor_si256(sparse_vec, sign_bit);
        let cmp_result = _mm256_cmpgt_epi32(sparse_xor, key_vec);
        let mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp_result)) as u32;
        gt_mask |= mask << 24;
    }

    // 只保留有效 entries
    gt_mask &= valid_mask;

    if gt_mask == 0 {
        len as usize
    } else {
        // 取最低位的 1（第一个大于 sparse_key 的位置）
        gt_mask.trailing_zeros() as usize
    }
}

// ============================================================================
// 测试
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_search_basic() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 0b00;
        sparse_keys[1] = 0b01;
        sparse_keys[2] = 0b10;

        // dense=0b00 匹配 0
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b00, 3),
            SimdSearchResult::Found(0)
        );

        // dense=0b01 匹配 0 和 1，取最后一个
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b01, 3),
            SimdSearchResult::Found(1)
        );

        // dense=0b10 匹配 0 和 2，取最后一个
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b10, 3),
            SimdSearchResult::Found(2)
        );

        // dense=0b11 匹配 0, 1, 2，取最后一个
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b11, 3),
            SimdSearchResult::Found(2)
        );
    }

    #[test]
    fn test_scalar_search_no_match() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 0b11; // 需要 bit0=1 和 bit1=1

        // dense=0b01 不匹配（缺少 bit1）
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b01, 1),
            SimdSearchResult::NotFound
        );
    }

    #[test]
    fn test_scalar_search_respects_len() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 0b00;
        sparse_keys[1] = 0b01;
        sparse_keys[2] = 0b10;

        // len=2，忽略 index 2
        assert_eq!(
            simd_search_scalar(&sparse_keys, 0b11, 2),
            SimdSearchResult::Found(1)
        );
    }

    #[test]
    fn test_simd_search_consistency() {
        // SIMD 和 scalar 应该返回相同结果
        let mut sparse_keys = [0u32; 32];
        for i in 0..32 {
            sparse_keys[i] = i as u32;
        }

        for dense in 0..64u32 {
            let scalar_result = simd_search_scalar(&sparse_keys, dense, 32);
            let simd_result = simd_search(&sparse_keys, dense, 32);
            assert_eq!(scalar_result, simd_result, "Mismatch for dense={}", dense);
        }
    }

    #[test]
    fn test_simd_search_partial_fill() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 0b000;
        sparse_keys[1] = 0b001;
        sparse_keys[2] = 0b010;
        sparse_keys[3] = 0b011;
        sparse_keys[4] = 0b100;

        // 只有 5 个有效 entries
        assert_eq!(simd_search(&sparse_keys, 0b111, 5), SimdSearchResult::Found(4));
        assert_eq!(simd_search(&sparse_keys, 0b011, 5), SimdSearchResult::Found(3));
        assert_eq!(simd_search(&sparse_keys, 0b001, 5), SimdSearchResult::Found(1));
    }

    #[test]
    fn test_has_avx2() {
        // 这个测试只是确保函数可以被调用
        let _ = has_avx2();
    }

    #[test]
    fn test_batch_search() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 0b00;
        sparse_keys[1] = 0b01;
        sparse_keys[2] = 0b10;

        let dense_keys = vec![0b00, 0b01, 0b10, 0b11];
        let results = simd_batch_search(&sparse_keys, &dense_keys, 3);

        assert_eq!(results.len(), 4);
        assert_eq!(results[0], SimdSearchResult::Found(0));
        assert_eq!(results[1], SimdSearchResult::Found(1));
        assert_eq!(results[2], SimdSearchResult::Found(2));
        assert_eq!(results[3], SimdSearchResult::Found(2));
    }

    #[test]
    fn test_simd_full_node() {
        // 测试完整 32 entries
        let mut sparse_keys = [0u32; 32];
        for i in 0..32 {
            sparse_keys[i] = 1 << i;
        }

        // 每个 dense key 只匹配对应的 entry（加上 entry 0 因为 sparse=0b0 总是匹配）
        for i in 1..32 {
            let dense = 1u32 << i;
            let result = simd_search(&sparse_keys, dense, 32);
            assert_eq!(result, SimdSearchResult::Found(i), "Failed for i={}", i);
        }
    }

    // ========================================================================
    // find_insert_position 测试
    // ========================================================================

    #[test]
    fn test_find_insert_position_basic() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 10;
        sparse_keys[1] = 20;
        sparse_keys[2] = 30;

        // 插入到开头
        assert_eq!(find_insert_position_scalar(&sparse_keys, 5, 3), 0);
        assert_eq!(simd_find_insert_position(&sparse_keys, 5, 3), 0);

        // 插入到中间
        assert_eq!(find_insert_position_scalar(&sparse_keys, 15, 3), 1);
        assert_eq!(simd_find_insert_position(&sparse_keys, 15, 3), 1);

        assert_eq!(find_insert_position_scalar(&sparse_keys, 25, 3), 2);
        assert_eq!(simd_find_insert_position(&sparse_keys, 25, 3), 2);

        // 插入到末尾
        assert_eq!(find_insert_position_scalar(&sparse_keys, 35, 3), 3);
        assert_eq!(simd_find_insert_position(&sparse_keys, 35, 3), 3);

        // 等于已存在值时（插入到其后面）
        assert_eq!(find_insert_position_scalar(&sparse_keys, 20, 3), 2);
        assert_eq!(simd_find_insert_position(&sparse_keys, 20, 3), 2);
    }

    #[test]
    fn test_find_insert_position_empty() {
        let sparse_keys = [0u32; 32];
        assert_eq!(find_insert_position_scalar(&sparse_keys, 100, 0), 0);
        assert_eq!(simd_find_insert_position(&sparse_keys, 100, 0), 0);
    }

    #[test]
    fn test_find_insert_position_consistency() {
        // SIMD 和 scalar 应该返回相同结果
        let mut sparse_keys = [0u32; 32];
        for i in 0..32 {
            sparse_keys[i] = i as u32 * 10;
        }

        for key in 0..350u32 {
            let scalar_result = find_insert_position_scalar(&sparse_keys, key, 32);
            let simd_result = simd_find_insert_position(&sparse_keys, key, 32);
            assert_eq!(
                scalar_result, simd_result,
                "Mismatch for key={}: scalar={}, simd={}",
                key, scalar_result, simd_result
            );
        }
    }

    #[test]
    fn test_find_insert_position_respects_len() {
        let mut sparse_keys = [0u32; 32];
        sparse_keys[0] = 10;
        sparse_keys[1] = 20;
        sparse_keys[2] = 30;
        sparse_keys[3] = 40; // 不应被考虑

        // len=3，忽略 index 3
        assert_eq!(simd_find_insert_position(&sparse_keys, 35, 3), 3);
        // len=4，考虑 index 3
        assert_eq!(simd_find_insert_position(&sparse_keys, 35, 4), 3);
    }

    #[test]
    fn test_find_insert_position_unsigned_comparison() {
        // 测试 bit31 设置时的无符号比较
        // 这是 span=32 时可能出现的情况
        let mut sparse_keys = [0u32; 32];

        // 设置一些包含 bit31 的值（无符号很大，有符号为负）
        sparse_keys[0] = 0x00000001; // 1
        sparse_keys[1] = 0x7FFFFFFF; // 2^31 - 1 (最大正 i32)
        sparse_keys[2] = 0x80000000; // 2^31 (最小负 i32，但无符号更大)
        sparse_keys[3] = 0x80000001; // 2^31 + 1
        sparse_keys[4] = 0xFFFFFFFF; // 2^32 - 1 (最大 u32)

        // 无符号顺序: 1 < 0x7FFFFFFF < 0x80000000 < 0x80000001 < 0xFFFFFFFF

        // 插入 0 应该在位置 0
        assert_eq!(simd_find_insert_position(&sparse_keys, 0, 5), 0);

        // 插入 2 应该在位置 1 (在 1 和 0x7FFFFFFF 之间)
        assert_eq!(simd_find_insert_position(&sparse_keys, 2, 5), 1);

        // 插入 0x80000000 应该在位置 3 (找第一个 > key 的位置，即 0x80000001)
        assert_eq!(simd_find_insert_position(&sparse_keys, 0x80000000, 5), 3);

        // 插入 0x80000002 应该在位置 4 (在 0x80000001 和 0xFFFFFFFF 之间)
        assert_eq!(simd_find_insert_position(&sparse_keys, 0x80000002, 5), 4);

        // 验证 scalar 和 SIMD 一致
        for &key in &[0u32, 1, 2, 0x7FFFFFFF, 0x80000000, 0x80000001, 0xFFFFFFFE, 0xFFFFFFFF] {
            let scalar = find_insert_position_scalar(&sparse_keys, key, 5);
            let simd = simd_find_insert_position(&sparse_keys, key, 5);
            assert_eq!(scalar, simd, "Mismatch for key=0x{:08X}", key);
        }
    }
}
