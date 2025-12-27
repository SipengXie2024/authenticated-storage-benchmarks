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

