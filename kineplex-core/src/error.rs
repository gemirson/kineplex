//! Error types for KinePlex Core

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("Graph not found: {0}")]
    GraphNotFound(String),
    
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Wasm runtime error: {0}")]
    WasmError(String),
    
    #[error("Data plane error: {0}")]
    DataPlaneError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Parquet error: {0}")]
    ParquetError(String),
    
    #[error("Arrow error: {0}")]
    ArrowError(String),
    
    #[error("Graph not in valid state for operation: {0}")]
    InvalidState(String),
    
    #[error("Resource quota exceeded: {0}")]
    QuotaExceeded(String),
    
    #[error("Authentication required")]
    AuthenticationRequired,
    
    #[error("Authorization failed: {0}")]
    AuthorizationFailed(String),
    
    #[error("Idempotency key already exists: {0}")]
    IdempotencyKeyExists(String),
    
    #[error("Protocol version mismatch: expected {expected}, got {got}")]
    ProtocolVersionMismatch { expected: String, got: String },
}

pub type Result<T> = std::result::Result<T, CoreError>;