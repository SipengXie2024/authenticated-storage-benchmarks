//! Hash 函数抽象层
//!
//! 提供模块化的 Hash 支持，便于在 blake3 和 keccak256 之间切换。
//! 这对于 benchmark 的公平性很重要：不同算法性能差异可被单独评估。
//!
//! # 与 C++ HOT 的关系
//! 原版 HOT 不涉及哈希，因为它是纯内存数据结构。
//! 持久化版本需要 content-addressed 存储，每个节点的 ID 是其内容的哈希。
//! 这也为后续 Merkle 化提供基础。

/// 32 字节哈希输出类型
pub type HashOutput = [u8; 32];

/// Hash 函数 trait
///
/// 所有实现必须满足：
/// 1. 确定性：相同输入产生相同输出
/// 2. 抗碰撞：不同输入极难产生相同输出
/// 3. 输出固定 32 字节
pub trait Hasher {
    /// 计算输入数据的哈希值
    fn hash(data: &[u8]) -> HashOutput;

    /// 返回算法名称（用于日志和调试）
    fn name() -> &'static str;
}

/// Blake3 哈希实现
///
/// 特点：
/// - 速度：比 SHA-256 快约 10x，比 keccak256 快约 5x
/// - 安全性：256-bit 安全级别
/// - 硬件加速：支持 SIMD 并行计算
///
/// 推荐用于性能敏感场景。
pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(data: &[u8]) -> HashOutput {
        blake3::hash(data).into()
    }

    fn name() -> &'static str {
        "blake3"
    }
}

/// Keccak256 哈希实现
///
/// 特点：
/// - 以太坊生态标准（与 H256 类型兼容）
/// - ZK-SNARK 友好（部分电路实现）
/// - 广泛验证的安全性
///
/// 推荐用于需要与以太坊工具链兼容的场景。
pub struct Keccak256Hasher;

impl Hasher for Keccak256Hasher {
    fn hash(data: &[u8]) -> HashOutput {
        use tiny_keccak::{Hasher as TinyHasher, Keccak};

        let mut output = [0u8; 32];
        let mut hasher = Keccak::v256();
        hasher.update(data);
        hasher.finalize(&mut output);
        output
    }

    fn name() -> &'static str {
        "keccak256"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blake3_determinism() {
        let data = b"persistent-hot-test-data";

        let hash1 = Blake3Hasher::hash(data);
        let hash2 = Blake3Hasher::hash(data);

        assert_eq!(hash1, hash2, "Blake3 should be deterministic");
    }

    #[test]
    fn test_keccak256_determinism() {
        let data = b"persistent-hot-test-data";

        let hash1 = Keccak256Hasher::hash(data);
        let hash2 = Keccak256Hasher::hash(data);

        assert_eq!(hash1, hash2, "Keccak256 should be deterministic");
    }

    #[test]
    fn test_different_hashers_produce_different_output() {
        let data = b"test-input";

        let blake3_hash = Blake3Hasher::hash(data);
        let keccak_hash = Keccak256Hasher::hash(data);

        assert_ne!(
            blake3_hash, keccak_hash,
            "Different hash algorithms should produce different outputs"
        );
    }

    #[test]
    fn test_different_inputs_produce_different_output() {
        let data1 = b"input-one";
        let data2 = b"input-two";

        assert_ne!(
            Blake3Hasher::hash(data1),
            Blake3Hasher::hash(data2),
            "Different inputs should produce different hashes"
        );

        assert_ne!(
            Keccak256Hasher::hash(data1),
            Keccak256Hasher::hash(data2),
            "Different inputs should produce different hashes"
        );
    }

    #[test]
    fn test_empty_input() {
        // 空输入也应该产生有效的哈希
        let empty: &[u8] = b"";

        let blake3_hash = Blake3Hasher::hash(empty);
        let keccak_hash = Keccak256Hasher::hash(empty);

        // 验证不是全零
        assert!(blake3_hash.iter().any(|&b| b != 0));
        assert!(keccak_hash.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_hasher_names() {
        assert_eq!(Blake3Hasher::name(), "blake3");
        assert_eq!(Keccak256Hasher::name(), "keccak256");
    }
}
