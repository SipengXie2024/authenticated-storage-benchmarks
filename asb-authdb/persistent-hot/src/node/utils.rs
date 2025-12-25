//! 位操作辅助函数

/// 从字节数组中提取指定位置的 bit
///
/// # 位编码
/// 使用 MSB-first 编码（与 C++ HOT 一致）：
/// - bit 0 = 第 0 字节的 bit 7（最高位）
/// - bit 7 = 第 0 字节的 bit 0（最低位）
/// - bit 8 = 第 1 字节的 bit 7
/// - ...
///
/// # 返回
/// - `true`: bit 为 1
/// - `false`: bit 为 0（或超出 key 范围）
#[inline]
pub fn extract_bit(key: &[u8], bit_pos: u16) -> bool {
    let byte_idx = (bit_pos / 8) as usize;
    let bit_idx = 7 - (bit_pos % 8); // MSB-first

    if byte_idx >= key.len() {
        return false; // 超出范围视为 0
    }

    (key[byte_idx] >> bit_idx) & 1 == 1
}

/// 找到两个 key 的第一个不同 bit 位置
///
/// 对应 C++ 中的 `DiscriminativeBit` 计算。
///
/// # 返回
/// - `Some(bit_pos)`: 第一个不同的 bit 位置
/// - `None`: 两个 key 完全相同
pub fn find_first_differing_bit(key1: &[u8], key2: &[u8]) -> Option<u16> {
    let min_len = key1.len().min(key2.len());

    for i in 0..min_len {
        if key1[i] != key2[i] {
            let xor = key1[i] ^ key2[i];
            let bit_in_byte = xor.leading_zeros() as u16;
            return Some((i as u16) * 8 + bit_in_byte);
        }
    }

    // 检查长度不同的情况
    if key1.len() != key2.len() {
        let longer = if key1.len() > key2.len() { key1 } else { key2 };
        for i in min_len..longer.len() {
            if longer[i] != 0 {
                let bit_in_byte = longer[i].leading_zeros() as u16;
                return Some((i as u16) * 8 + bit_in_byte);
            }
        }
    }

    None
}
