//! 测试辅助函数
//!
//! 对应 C++ PartialKeyMappingTestHelper

use std::collections::BTreeSet;

/// 创建包含指定 bits 的 256 字节数组
///
/// 对应 C++ getRawBytesWithBitsSet
///
/// # 位序约定 (MSB-first)
/// - bit 0 = key[0] 的最高位 (0x80)
/// - bit 7 = key[0] 的最低位 (0x01)
/// - bit 8 = key[1] 的最高位 (0x80)
pub fn get_raw_bytes_with_bits_set(bits: &[u16]) -> [u8; 256] {
    let mut raw_bytes = [0u8; 256];
    for &bit_pos in bits {
        let byte_index = (bit_pos / 8) as usize;
        let bit_in_byte = bit_pos % 8;
        // MSB-first: bit 0 in byte = 0x80, bit 7 in byte = 0x01
        let extraction_byte = 0x80u8 >> bit_in_byte;
        if byte_index < 256 {
            raw_bytes[byte_index] |= extraction_byte;
        }
    }
    raw_bytes
}

/// 创建只设置单个 bit 的数组
///
/// 对应 C++ getRawBytesWithSingleBitSet
pub fn get_raw_bytes_with_single_bit_set(bit: u16) -> [u8; 256] {
    get_raw_bytes_with_bits_set(&[bit])
}

/// 获取 bit 对应的字节索引
#[inline]
pub fn get_byte_index(bit: u16) -> usize {
    (bit / 8) as usize
}

/// 获取 bit 在字节内的提取掩码 (MSB-first)
#[inline]
pub fn get_extraction_byte(bit: u16) -> u8 {
    let bit_in_byte = bit % 8;
    0x80u8 >> bit_in_byte
}

/// 反转字节数组
pub fn invert_bytes(bytes: &[u8; 256]) -> [u8; 256] {
    let mut inverted = [0u8; 256];
    for i in 0..256 {
        inverted[i] = !bytes[i];
    }
    inverted
}

/// 通用验证套件
///
/// 对应 C++ checkExtractionInformationOnExpectedBits
/// 验证节点的 extraction masks 正确性
///
/// # 验证项目
/// 1. 全零输入 → 提取结果为 0
/// 2. bits 数量正确
/// 3. discriminative bits 集合正确
/// 4. 全 1 输入 → 提取结果为 all_bits_set
/// 5. 只设置 extraction bits → 提取结果为 all_bits_set
/// 6. 除 extraction bits 外全设置 → 提取结果为 0
/// 7. 单独设置每个 bit → 正确提取
pub fn check_extraction_information(
    extract_fn: impl Fn(&[u8; 256]) -> u32,
    get_mask_for_bit_fn: impl Fn(u16) -> u32,
    expected_bits: &[u16],
) {
    let expected_bits_set: BTreeSet<u16> = expected_bits.iter().copied().collect();
    let num_bits = expected_bits_set.len();

    // 1. 全零输入 → 提取结果为 0
    let all_zeros = [0u8; 256];
    let zero_mask = extract_fn(&all_zeros);
    assert_eq!(zero_mask, 0, "Zero input should extract to 0");

    // 2 & 3. bits 数量验证在调用方完成

    // 4. 全 1 输入 → 提取结果为 all_bits_set
    let all_bits_set = if num_bits >= 32 {
        u32::MAX
    } else {
        (1u32 << num_bits) - 1
    };
    let all_ones = [0xFFu8; 256];
    let all_ones_mask = extract_fn(&all_ones);
    assert_eq!(
        all_ones_mask, all_bits_set,
        "All ones input should extract to all bits set"
    );

    // 5. 只设置 extraction bits → 提取结果为 all_bits_set
    let only_extraction_bits = get_raw_bytes_with_bits_set(expected_bits);
    let extraction_mask = extract_fn(&only_extraction_bits);
    assert_eq!(
        extraction_mask, all_bits_set,
        "Only extraction bits set should extract to all bits set"
    );

    // 6. 除 extraction bits 外全设置 → 提取结果为 0
    let all_except_extraction = invert_bytes(&only_extraction_bits);
    let except_mask = extract_fn(&all_except_extraction);
    assert_eq!(
        except_mask, 0,
        "All bits except extraction bits should extract to 0"
    );

    // 7. 单独设置每个 bit → 正确提取
    let mut extracted_mask = 0u32;
    for &bit_pos in expected_bits {
        let single_bit_bytes = get_raw_bytes_with_single_bit_set(bit_pos);
        let single_extracted = extract_fn(&single_bit_bytes);

        // 应该只有一个 bit 被设置
        assert_eq!(
            single_extracted.count_ones(),
            1,
            "Single bit {} should extract to exactly one bit",
            bit_pos
        );

        // 不应该与之前提取的 bits 重叠
        assert_eq!(
            single_extracted & extracted_mask,
            0,
            "Bit {} should not overlap with previously extracted bits",
            bit_pos
        );

        // 验证 getMaskFor 一致性
        let mask_for_bit = get_mask_for_bit_fn(bit_pos);
        assert_eq!(
            single_extracted, mask_for_bit,
            "Extracted mask for bit {} should match getMaskFor",
            bit_pos
        );

        extracted_mask |= single_extracted;
    }

    assert_eq!(
        extracted_mask, all_bits_set,
        "All extracted bits should cover all bits set"
    );
}

/// 计算 popcount
#[inline]
pub fn popcount(x: u32) -> u32 {
    x.count_ones()
}

/// 确定性随机数生成器（用于可重复测试）
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// 生成下一个随机 u64
    pub fn next_u64(&mut self) -> u64 {
        // xorshift64
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    /// 生成 [min, max) 范围内的随机数
    pub fn next_range(&mut self, min: u64, max: u64) -> u64 {
        min + (self.next_u64() % (max - min))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_raw_bytes_with_bits_set() {
        // bit 0 = key[0] 的最高位
        let bytes = get_raw_bytes_with_single_bit_set(0);
        assert_eq!(bytes[0], 0x80);

        // bit 7 = key[0] 的最低位
        let bytes = get_raw_bytes_with_single_bit_set(7);
        assert_eq!(bytes[0], 0x01);

        // bit 8 = key[1] 的最高位
        let bytes = get_raw_bytes_with_single_bit_set(8);
        assert_eq!(bytes[1], 0x80);

        // 多个 bits
        let bytes = get_raw_bytes_with_bits_set(&[0, 7, 8]);
        assert_eq!(bytes[0], 0x81); // bit 0 和 bit 7
        assert_eq!(bytes[1], 0x80); // bit 8
    }

    #[test]
    fn test_extraction_byte() {
        assert_eq!(get_extraction_byte(0), 0x80);
        assert_eq!(get_extraction_byte(1), 0x40);
        assert_eq!(get_extraction_byte(7), 0x01);
        assert_eq!(get_extraction_byte(8), 0x80);
    }

    #[test]
    fn test_deterministic_rng() {
        let mut rng1 = DeterministicRng::new(12345);
        let mut rng2 = DeterministicRng::new(12345);

        for _ in 0..100 {
            assert_eq!(rng1.next_u64(), rng2.next_u64());
        }
    }
}
