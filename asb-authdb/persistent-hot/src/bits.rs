//! 位操作辅助函数
//!
//! 提供 PEXT/PDEP 操作，用于：
//! - 搜索时：从 U256 key 提取 discriminative bits → dense partial key (PEXT)
//! - 插入时：扩展现有 sparse partial keys (PDEP)
//! - 删除时：压缩 sparse partial keys (PEXT)

// ============================================================================
// PEXT - Parallel Bits Extract
// ============================================================================

/// 64位 PEXT - 并行位提取
///
/// 从 source 中提取 mask 指定位置的 bits，压缩到结果的低位。
///
/// # 示例
/// ```
/// use persistent_hot::pext64;
/// // mask = 0b1010, source = 0b1111
/// // 提取 bit 1 和 bit 3，结果 = 0b11
/// assert_eq!(pext64(0b1111, 0b1010), 0b11);
/// ```
#[inline]
pub fn pext64(source: u64, mask: u64) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("bmi2") {
            // Safety: 已检测 BMI2 支持
            return unsafe { std::arch::x86_64::_pext_u64(source, mask) };
        }
    }
    pext64_soft(source, mask)
}

/// 32位 PEXT
#[inline]
pub fn pext32(source: u32, mask: u32) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe { std::arch::x86_64::_pext_u32(source, mask) };
        }
    }
    pext32_soft(source, mask)
}

/// 64位 PEXT 软件实现
fn pext64_soft(mut source: u64, mut mask: u64) -> u64 {
    let mut result = 0u64;
    let mut result_bit = 0;
    while mask != 0 {
        if mask & 1 != 0 {
            if source & 1 != 0 {
                result |= 1 << result_bit;
            }
            result_bit += 1;
        }
        source >>= 1;
        mask >>= 1;
    }
    result
}

/// 32位 PEXT 软件实现
fn pext32_soft(source: u32, mask: u32) -> u32 {
    pext64_soft(source as u64, mask as u64) as u32
}

// ============================================================================
// PDEP - Parallel Bits Deposit
// ============================================================================

/// 64位 PDEP - 并行位存放
///
/// 将 source 的低位 bits 分散到 mask 指定的位置。
///
/// # 示例
/// ```
/// use persistent_hot::pdep64;
/// // source = 0b11, mask = 0b1010
/// // 将 bit 0 和 bit 1 存放到位置 1 和 3，结果 = 0b1010
/// assert_eq!(pdep64(0b11, 0b1010), 0b1010);
/// ```
#[inline]
pub fn pdep64(source: u64, mask: u64) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe { std::arch::x86_64::_pdep_u64(source, mask) };
        }
    }
    pdep64_soft(source, mask)
}

/// 32位 PDEP
#[inline]
pub fn pdep32(source: u32, mask: u32) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("bmi2") {
            return unsafe { std::arch::x86_64::_pdep_u32(source, mask) };
        }
    }
    pdep32_soft(source, mask)
}

/// 64位 PDEP 软件实现
fn pdep64_soft(mut source: u64, mut mask: u64) -> u64 {
    let mut result = 0u64;
    let mut dst_bit = 1u64;
    while mask != 0 {
        if mask & 1 != 0 {
            if source & 1 != 0 {
                result |= dst_bit;
            }
            source >>= 1;
        }
        mask >>= 1;
        dst_bit <<= 1;
    }
    result
}

/// 32位 PDEP 软件实现
fn pdep32_soft(source: u32, mask: u32) -> u32 {
    pdep64_soft(source as u64, mask as u64) as u32
}

// ============================================================================
// Mask 计算
// ============================================================================

/// 计算 PDEP deposit mask（插入时扩展用）
///
/// 当添加新的 discriminative bit 时，需要扩展现有的 partial keys。
/// 此函数计算用于 PDEP 的 mask，将旧 bits 分散到新位置，新 bit 位置留空（填 0）。
///
/// # 参数
/// - `old_bits_count`: 当前 discriminative bits 数量
/// - `new_bit_position`: 新 bit 在数组中的位置 (0-indexed)
///
/// # 示例
/// ```
/// use persistent_hot::compute_deposit_mask;
/// // 2 个旧 bits，新 bit 插入位置 1
/// // 旧: [bit0, bit1] → 新: [bit0, NEW, bit1]
/// // mask = 0b101 (保留位置 0 和 2，跳过位置 1)
/// assert_eq!(compute_deposit_mask(2, 1), 0b101);
/// ```
#[inline]
pub fn compute_deposit_mask(old_bits_count: usize, new_bit_position: usize) -> u32 {
    debug_assert!(new_bit_position <= old_bits_count);
    debug_assert!(old_bits_count < 32);

    if old_bits_count == 0 {
        return 0;
    }

    let all_ones = (1u32 << old_bits_count) - 1;
    let low_mask = (1u32 << new_bit_position) - 1;
    let high_mask = all_ones & !low_mask;

    (high_mask << 1) | low_mask
}

/// 计算 PEXT compression mask（删除时压缩用）
///
/// 当移除一个 discriminative bit 时，需要压缩现有的 partial keys。
/// 此函数计算用于 PEXT 的 mask，提取除被删除位之外的所有 bits。
///
/// # 参数
/// - `total_bits`: 当前总 bits 数量
/// - `remove_position`: 要移除的 bit 位置
///
/// # 示例
/// ```
/// use persistent_hot::compute_compression_mask;
/// // 3 个 bits，移除位置 1
/// // mask = 0b101 (保留位置 0 和 2)
/// assert_eq!(compute_compression_mask(3, 1), 0b101);
/// ```
#[inline]
pub fn compute_compression_mask(total_bits: usize, remove_position: usize) -> u32 {
    debug_assert!(remove_position < total_bits);
    debug_assert!(total_bits <= 32);

    let all_ones = (1u32 << total_bits) - 1;
    all_ones & !(1u32 << remove_position)
}

// ============================================================================
// 批量操作
// ============================================================================

/// 批量扩展 sparse partial keys（插入时）
///
/// 只处理 [0..len] 范围的有效 entries。
/// 使用 PDEP 将旧的 partial key 分散到新位置，新 bit 位置填 0。
pub fn expand_partial_keys(keys: &mut [u32; 32], len: usize, deposit_mask: u32) {
    for key in keys.iter_mut().take(len) {
        *key = pdep32(*key, deposit_mask);
    }
}

/// 批量压缩 sparse partial keys（删除时）
///
/// 使用 PEXT 提取除被删除位之外的所有 bits。
pub fn compress_partial_keys(keys: &mut [u32; 32], len: usize, compression_mask: u32) {
    for key in keys.iter_mut().take(len) {
        *key = pext32(*key, compression_mask);
    }
}

