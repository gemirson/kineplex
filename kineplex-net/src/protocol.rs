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
                write!(f, "Protocol version mismatch: expected one of {}, got {}", expected, got)
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
            deprecated_at: now,
            removal_at: now + chrono::Duration::days(deprecation_days),
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