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

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_not_found_error() {
        let err = CoreError::GraphNotFound("graph-123".to_string());
        assert_eq!(err.to_string(), "Graph not found: graph-123");
    }

    #[test]
    fn test_execution_failed_error() {
        let err = CoreError::ExecutionFailed("timeout".to_string());
        assert_eq!(err.to_string(), "Execution failed: timeout");
    }

    #[test]
    fn test_wasm_error() {
        let err = CoreError::WasmError("out of fuel".to_string());
        assert_eq!(err.to_string(), "Wasm runtime error: out of fuel");
    }

    #[test]
    fn test_data_plane_error() {
        let err = CoreError::DataPlaneError("connection lost".to_string());
        assert_eq!(err.to_string(), "Data plane error: connection lost");
    }

    #[test]
    fn test_serialization_error() {
        let err = CoreError::SerializationError("invalid json".to_string());
        assert_eq!(err.to_string(), "Serialization error: invalid json");
    }

    #[test]
    fn test_parquet_error() {
        let err = CoreError::ParquetError("write failed".to_string());
        assert_eq!(err.to_string(), "Parquet error: write failed");
    }

    #[test]
    fn test_arrow_error() {
        let err = CoreError::ArrowError("schema mismatch".to_string());
        assert_eq!(err.to_string(), "Arrow error: schema mismatch");
    }

    #[test]
    fn test_invalid_state_error() {
        let err = CoreError::InvalidState("graph already running".to_string());
        assert_eq!(err.to_string(), "Graph not in valid state for operation: graph already running");
    }

    #[test]
    fn test_quota_exceeded_error() {
        let err = CoreError::QuotaExceeded("memory limit".to_string());
        assert_eq!(err.to_string(), "Resource quota exceeded: memory limit");
    }

    #[test]
    fn test_authentication_required_error() {
        let err = CoreError::AuthenticationRequired;
        assert_eq!(err.to_string(), "Authentication required");
    }

    #[test]
    fn test_authorization_failed_error() {
        let err = CoreError::AuthorizationFailed("invalid token".to_string());
        assert_eq!(err.to_string(), "Authorization failed: invalid token");
    }

    #[test]
    fn test_idempotency_key_exists_error() {
        let err = CoreError::IdempotencyKeyExists("key-123".to_string());
        assert_eq!(err.to_string(), "Idempotency key already exists: key-123");
    }

    #[test]
    fn test_protocol_version_mismatch_error() {
        let err = CoreError::ProtocolVersionMismatch {
            expected: "1.0.0".to_string(),
            got: "0.9.0".to_string(),
        };
        assert_eq!(err.to_string(), "Protocol version mismatch: expected 1.0.0, got 0.9.0");
    }

    #[test]
    fn test_result_type() {
        fn example() -> Result<String> {
            Ok("success".to_string())
        }
        
        let result = example();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }
}