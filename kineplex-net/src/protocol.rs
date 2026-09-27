//! Protocol versioned contracts for control plane and gossip
//! 
//! This module implements FT-077:
//! - Versioned protocol messages
//! - Version negotiation
//! - Deprecation policies

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Protocol version
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl ProtocolVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }
    
    pub fn current() -> Self {
        Self { major: 1, minor: 0, patch: 0 }
    }
    
    pub fn to_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
    
    /// Check if this version is compatible with another
    pub fn is_compatible(&self, other: &ProtocolVersion) -> bool {
        // Major version must match for compatibility
        self.major == other.major
    }
    
    /// Check if this version is newer than another
    pub fn is_newer_than(&self, other: &ProtocolVersion) -> bool {
        if self.major != other.major {
            return self.major > other.major;
        }
        if self.minor != other.minor {
            return self.minor > other.minor;
        }
        self.patch > other.patch
    }
}

/// Message types in the protocol
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageType {
    // Control plane messages
    SubmitGraph,
    GetStatus,
    CancelExecution,
    AllocateNodes,
    
    // Gossip messages
    Gossip,
    Heartbeat,
    Join,
    Leave,
    Failure,
    
    // Data plane messages
    ExecuteStage,
    StageComplete,
    DataTransfer,
}

/// Protocol message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolMessage {
    /// Message ID
    pub id: String,
    /// Protocol version
    pub version: ProtocolVersion,
    /// Message type
    pub message_type: MessageType,
    /// Payload (JSON serialized)
    pub payload: String,
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// Source node ID
    pub source: String,
    /// Target node ID (optional)
    pub target: Option<String>,
}

impl ProtocolMessage {
    pub fn new(message_type: MessageType, payload: String, source: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            version: ProtocolVersion::current(),
            message_type,
            payload,
            timestamp: Utc::now(),
            source,
            target: None,
        }
    }
    
    pub fn with_target(mut self, target: String) -> Self {
        self.target = Some(target);
        self
    }
    
    /// Validate message version compatibility
    pub fn validate_version(&self, supported_versions: &[ProtocolVersion]) -> Result<(), ProtocolError> {
        for supported in supported_versions {
            if self.version.is_compatible(supported) {
                return Ok(());
            }
        }
        Err(ProtocolError::VersionMismatch {
            expected: supported_versions.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            got: self.version.to_string(),
        })
    }
}

/// Protocol errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolError {
    VersionMismatch { expected: String, got: String },
    InvalidMessage(String),
    SerializationError(String),
    UnsupportedMessageType(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::VersionMismatch { expected, got } => {
                write!(f, "Version mismatch: expected one of {}, got {}", expected, got)
            }
            ProtocolError::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
            ProtocolError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ProtocolError::UnsupportedMessageType(msg) => write!(f, "Unsupported message type: {}", msg),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Version negotiation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionNegotiationRequest {
    pub supported_versions: Vec<ProtocolVersion>,
    pub client_version: ProtocolVersion,
}

/// Version negotiation response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionNegotiationResponse {
    pub accepted_version: ProtocolVersion,
    pub deprecation_warning: Option<String>,
}

/// Deprecation policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeprecationPolicy {
    pub version: ProtocolVersion,
    pub deprecated_at: DateTime<Utc>,
    pub removal_at: DateTime<Utc>,
    pub migration_guide: String,
}

impl DeprecationPolicy {
    pub fn new(version: ProtocolVersion, deprecation_days: i64, migration_guide: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            version,
            deprecated_at: now + chrono::Duration::days(deprecation_days),
            removal_at: now + chrono::Duration::days(deprecation_days * 2),
            migration_guide: migration_guide.into(),
        }
    }
    
    pub fn is_deprecated(&self) -> bool {
        Utc::now() >= self.deprecated_at
    }
    
    pub fn should_remove(&self) -> bool {
        Utc::now() >= self.removal_at
    }
}

/// Compatibility matrix entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityEntry {
    pub client_version: ProtocolVersion,
    pub server_version: ProtocolVersion,
    pub compatible: bool,
    pub tested_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_version_compatibility() {
        let v1 = ProtocolVersion::new(1, 0, 0);
        let v2 = ProtocolVersion::new(1, 1, 0);
        let v3 = ProtocolVersion::new(2, 0, 0);
        
        assert!(v1.is_compatible(&v2));
        assert!(!v1.is_compatible(&v3));
    }
    
    #[test]
    fn test_version_newer() {
        let v1 = ProtocolVersion::new(1, 0, 0);
        let v2 = ProtocolVersion::new(1, 1, 0);
        let v3 = ProtocolVersion::new(2, 0, 0);
        
        assert!(v2.is_newer_than(&v1));
        assert!(v3.is_newer_than(&v1));
    }
    
    #[test]
    fn test_message_version_validation() {
        let msg = ProtocolMessage::new(
            MessageType::SubmitGraph,
            "{}".to_string(),
            "node-1".to_string(),
        );
        
        let supported = vec![ProtocolVersion::new(1, 0, 0)];
        assert!(msg.validate_version(&supported).is_ok());
    }
}

#[test]
    fn test_version_current() {
        let v = ProtocolVersion::current();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 0);
    }
    
    #[test]
    fn test_version_to_string() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");
    }
    
    #[test]
    fn test_version_equal() {
        let v1 = ProtocolVersion::new(1, 2, 3);
        let v2 = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v1, v2);
    }
    
    #[test]
    fn test_version_not_compatible_different_major() {
        let v1 = ProtocolVersion::new(1, 0, 0);
        let v2 = ProtocolVersion::new(2, 0, 0);
        assert!(!v1.is_compatible(&v2));
    }
    
    #[test]
    fn test_version_not_newer() {
        let v1 = ProtocolVersion::new(1, 0, 0);
        let v2 = ProtocolVersion::new(1, 0, 0);
        assert!(!v2.is_newer_than(&v1));
    }
    
    #[test]
    fn test_message_type_variants() {
        // Test all message type variants exist
        let _ = MessageType::SubmitGraph;
        let _ = MessageType::GetStatus;
        let _ = MessageType::CancelExecution;
        let _ = MessageType::AllocateNodes;
        let _ = MessageType::Gossip;
        let _ = MessageType::Heartbeat;
        let _ = MessageType::Join;
        let _ = MessageType::Leave;
        let _ = MessageType::Failure;
        let _ = MessageType::ExecuteStage;
        let _ = MessageType::StageComplete;
        let _ = MessageType::DataTransfer;
    }
    
    #[test]
    fn test_protocol_message_creation() {
        let msg = ProtocolMessage::new(
            MessageType::SubmitGraph,
            r#"{"key": "value"}"#.to_string(),
            "node-1".to_string(),
        );
        
        assert_eq!(msg.message_type, MessageType::SubmitGraph);
        assert_eq!(msg.source, "node-1");
        assert!(msg.target.is_none());
    }
    
    #[test]
    fn test_protocol_message_with_target() {
        let msg = ProtocolMessage::new(
            MessageType::ExecuteStage,
            "{}".to_string(),
            "node-1".to_string(),
        ).with_target("node-2".to_string());
        
        assert_eq!(msg.target, Some("node-2".to_string()));
    }
    
    #[test]
    fn test_protocol_message_validation_fails() {
        let msg = ProtocolMessage::new(
            MessageType::SubmitGraph,
            "{}".to_string(),
            "node-1".to_string(),
        );
        
        let supported = vec![ProtocolVersion::new(2, 0, 0)]; // Different major version
        let result = msg.validate_version(&supported);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_protocol_error_display() {
        let err = ProtocolError::VersionMismatch {
            expected: "1.0.0".to_string(),
            got: "2.0.0".to_string(),
        };
        assert!(err.to_string().contains("Version mismatch"));
        
        let err = ProtocolError::InvalidMessage("test".to_string());
        assert!(err.to_string().contains("Invalid message"));
        
        let err = ProtocolError::SerializationError("test".to_string());
        assert!(err.to_string().contains("Serialization error"));
        
        let err = ProtocolError::UnsupportedMessageType("test".to_string());
        assert!(err.to_string().contains("Unsupported message type"));
    }
    
    #[test]
    fn test_deprecation_policy_creation() {
        let policy = DeprecationPolicy::new(
            ProtocolVersion::new(1, 0, 0),
            90,
            "Migration guide URL",
        );
        
        assert_eq!(policy.version.major, 1);
        assert!(!policy.is_deprecated()); // Just created
        assert!(!policy.should_remove()); // Not time yet
    }
    
    #[test]
    fn test_deprecation_policy_serialization() {
        let policy = DeprecationPolicy::new(
            ProtocolVersion::new(1, 0, 0),
            90,
            "Migration guide",
        );
        
        let serialized = serde_json::to_string(&policy).unwrap();
        let deserialized: DeprecationPolicy = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(policy.version, deserialized.version);
    }
    
    #[test]
    fn test_compatibility_entry() {
        let entry = CompatibilityEntry {
            client_version: ProtocolVersion::new(1, 0, 0),
            server_version: ProtocolVersion::new(1, 1, 0),
            compatible: true,
            tested_at: Utc::now(),
        };
        
        assert!(entry.compatible);
        assert!(entry.client_version.is_compatible(&entry.server_version));
    }
    
    #[test]
    fn test_version_negotiation_request() {
        let request = VersionNegotiationRequest {
            supported_versions: vec![
                ProtocolVersion::new(1, 0, 0),
                ProtocolVersion::new(1, 1, 0),
            ],
            client_version: ProtocolVersion::new(1, 0, 0),
        };
        
        assert_eq!(request.supported_versions.len(), 2);
    }
    
    #[test]
    fn test_version_negotiation_response() {
        let response = VersionNegotiationResponse {
            accepted_version: ProtocolVersion::new(1, 0, 0),
            deprecation_warning: Some("Version 1.0.0 will be deprecated".to_string()),
        };
        
        assert_eq!(response.accepted_version.major, 1);
        assert!(response.deprecation_warning.is_some());
    }
