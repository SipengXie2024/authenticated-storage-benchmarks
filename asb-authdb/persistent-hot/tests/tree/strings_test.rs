//! 字符串键测试
//!
//! 对应 C++ HOTSingleThreadedTest.cpp 中的字符串测试
//!
//! 注意：Rust 实现使用 32 字节固定键，这里通过哈希字符串来模拟

use persistent_hot::hash::Blake3Hasher;
use persistent_hot::store::MemoryNodeStore;
use persistent_hot::tree::HOTTree;

#[path = "../common/mod.rs"]
mod common;

use common::sample_data::get_long_strings;

/// 辅助函数：创建测试树
fn create_test_tree() -> HOTTree<MemoryNodeStore, Blake3Hasher> {
    let store = MemoryNodeStore::new();
    HOTTree::new(store)
}

/// 辅助函数：将字符串转换为 32 字节键
fn string_to_key(s: &str) -> [u8; 32] {
    use blake3::Hasher;
    let mut hasher = Hasher::new();
    hasher.update(s.as_bytes());
    let hash = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(hash.as_bytes());
    key
}

/// 测试：短字符串键
///
/// 对应 C++ testShortStrings
#[test]
fn test_short_strings() {
    let mut tree = create_test_tree();

    let strings = vec![
        "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh",
    ];

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：长字符串键
///
/// 对应 C++ testLongStrings
#[test]
fn test_long_strings() {
    let mut tree = create_test_tree();

    let strings = get_long_strings();

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：相似前缀字符串
///
/// 对应 C++ testSimilarPrefixStrings
#[test]
fn test_similar_prefix_strings() {
    let mut tree = create_test_tree();

    let prefix = "common_prefix_";
    let strings: Vec<String> = (0..100).map(|i| format!("{}{:04}", prefix, i)).collect();

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：URL 风格字符串
///
/// 对应 C++ testURLStrings
#[test]
fn test_url_strings() {
    let mut tree = create_test_tree();

    let urls = vec![
        "https://example.com/page1",
        "https://example.com/page2",
        "https://example.com/api/v1/users",
        "https://example.com/api/v1/posts",
        "https://example.com/api/v2/users",
        "https://other.com/path",
        "http://localhost:8080/test",
        "ftp://files.example.com/download",
    ];

    for url in &urls {
        let key = string_to_key(url);
        tree.insert(&key, url.as_bytes().to_vec()).unwrap();
    }

    for url in &urls {
        let key = string_to_key(url);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：文件路径字符串
#[test]
fn test_file_path_strings() {
    let mut tree = create_test_tree();

    let paths = vec![
        "/home/user/documents/file1.txt",
        "/home/user/documents/file2.txt",
        "/home/user/downloads/archive.zip",
        "/var/log/system.log",
        "/etc/config.yaml",
        "/tmp/temp_file",
    ];

    for path in &paths {
        let key = string_to_key(path);
        tree.insert(&key, path.as_bytes().to_vec()).unwrap();
    }

    for path in &paths {
        let key = string_to_key(path);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：UUID 字符串
#[test]
fn test_uuid_strings() {
    let mut tree = create_test_tree();

    // 模拟 UUID 格式
    let uuids: Vec<String> = (0..100)
        .map(|i| {
            format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                i * 12345,
                i * 11,
                i * 22,
                i * 33,
                i as u64 * 44444
            )
        })
        .collect();

    for uuid in &uuids {
        let key = string_to_key(uuid);
        tree.insert(&key, uuid.as_bytes().to_vec()).unwrap();
    }

    for uuid in &uuids {
        let key = string_to_key(uuid);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：混合大小写字符串
#[test]
fn test_mixed_case_strings() {
    let mut tree = create_test_tree();

    let strings = vec![
        "Hello",
        "hello",
        "HELLO",
        "HeLLo",
        "hElLO",
        "HelloWorld",
        "helloworld",
        "HELLOWORLD",
    ];

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：Unicode 字符串
#[test]
fn test_unicode_strings() {
    let mut tree = create_test_tree();

    let strings = vec![
        "中文",
        "日本語",
        "한국어",
        "العربية",
        "🎉🎊",
        "mixed_中文_test",
        "Ñoño",
        "Ελληνικά",
    ];

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：数字字符串
#[test]
fn test_numeric_strings() {
    let mut tree = create_test_tree();

    // 数字字符串按字典序不等于数值序
    let numbers: Vec<String> = (0..1000).map(|i| format!("{}", i)).collect();

    for s in &numbers {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &numbers {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

/// 测试：空字符串和空白字符串
#[test]
fn test_empty_and_whitespace_strings() {
    let mut tree = create_test_tree();

    let strings = vec!["", " ", "  ", "\t", "\n", " \t\n ", "   a   "];

    for s in &strings {
        let key = string_to_key(s);
        tree.insert(&key, s.as_bytes().to_vec()).unwrap();
    }

    for s in &strings {
        let key = string_to_key(s);
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}
