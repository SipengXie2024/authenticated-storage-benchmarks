//! 节点存储错误类型

/// 节点存储错误类型
#[derive(Debug, Clone)]
pub enum StoreError {
    /// 序列化错误
    SerializationError(String),
    /// 反序列化错误
    DeserializationError(String),
    /// 底层存储错误
    StorageError(String),
    /// 节点不存在
    NotFound,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            StoreError::DeserializationError(msg) => write!(f, "Deserialization error: {}", msg),
            StoreError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            StoreError::NotFound => write!(f, "Node not found"),
        }
    }
}

impl std::error::Error for StoreError {}

/// 节点存储 Result 类型
pub type Result<T> = std::result::Result<T, StoreError>;
