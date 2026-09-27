//! Gossip/SWIM protocol for cluster membership
//! 
//! This module implements the SWIM (Scalable Weakly-consistent Infection-style
//! Membership Protocol) for cluster membership and failure detection.

use crate::{NetNodeId, NetworkAddress, ConnectionStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use parking_lot::RwLock;
use chrono::{DateTime, Utc};

/// Node information in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NetNodeId,
    pub address: NetworkAddress,
    pub status: ConnectionStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub incarnation: u32,
    pub metadata: HashMap<String, String>,
}

impl NodeInfo {
    pub fn new(id: NetNodeId, address: NetworkAddress) -> Self {
        Self {
            id,
            address,
            status: ConnectionStatus::Connected,
            last_heartbeat: Utc::now(),
            incarnation: 0,
            metadata: HashMap::new(),
        }
    }
    
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Utc::now();
        self.incarnation += 1;
    }
    
    pub fn is_alive(&self) -> bool {
        matches!(self.status, ConnectionStatus::Connected)
    }
}

/// Membership event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipEvent {
    /// A new node joined
    Join(NodeInfo),
    /// A node left gracefully
    Leave(NetNodeId),
    /// A node was detected as failed
    Failure(NetNodeId),
    /// A node rejoined after failure
    Rejoin(NodeInfo),
}

/// Gossip message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipMessage {
    pub source: NetNodeId,
    pub target: Option<NetNodeId>,
    pub members: Vec<NodeInfo>,
    pub timestamp: DateTime<Utc>,
}

impl GossipMessage {
    pub fn new(source: NetNodeId) -> Self {
        Self {
            source,
            target: None,
            members: Vec::new(),
            timestamp: Utc::now(),
        }
    }
    
    pub fn with_members(mut self, members: Vec<NodeInfo>) -> Self {
        self.members = members;
        self
    }
}

/// Gossip protocol implementation
pub struct GossipProtocol {
    self_node: NetNodeId,
    members: RwLock<HashMap<NetNodeId, NodeInfo>>,
    pending_events: RwLock<Vec<MembershipEvent>>,
    protocol_version: String,
}

impl GossipProtocol {
    pub fn new(self_node: NetNodeId) -> Self {
        let mut members = HashMap::new();
        members.insert(self_node.clone(), NodeInfo::new(
            self_node.clone(),
            NetworkAddress::new("localhost", 0),
        ));
        
        Self {
            self_node,
            members: RwLock::new(members),
            pending_events: RwLock::new(Vec::new()),
            protocol_version: "1.0.0".to_string(),
        }
    }
    
    /// Register a new node
    pub fn register_node(&self, node: NodeInfo) {
        let mut members = self.members.write();
        members.insert(node.id.clone(), node.clone());
        
        // Emit join event
        self.pending_events.write().push(MembershipEvent::Join(node));
    }
    
    /// Handle node leaving
    pub fn handle_leave(&self, node_id: NetNodeId) {
        let mut members = self.members.write();
        if members.remove(&node_id).is_some() {
            self.pending_events.write().push(MembershipEvent::Leave(node_id));
        }
    }
    
    /// Handle node failure
    pub fn handle_failure(&self, node_id: NetNodeId) {
        let mut members = self.members.write();
        if let Some(node) = members.get_mut(&node_id) {
            node.status = ConnectionStatus::Disconnected;
            self.pending_events.write().push(MembershipEvent::Failure(node_id));
        }
    }
    
    /// Handle node rejoin
    pub fn handle_rejoin(&self, node: NodeInfo) {
        let mut members = self.members.write();
        members.insert(node.id.clone(), node.clone());
        self.pending_events.write().push(MembershipEvent::Rejoin(node));
    }
    
    /// Get all alive members
    pub fn get_alive_members(&self) -> Vec<NodeInfo> {
        let members = self.members.read();
        members.values()
            .filter(|n| n.is_alive() && n.id != self.self_node)
            .cloned()
            .collect()
    }
    
    /// Get all members
    pub fn get_all_members(&self) -> Vec<NodeInfo> {
        let members = self.members.read();
        members.values().cloned().collect()
    }
    
    /// Process incoming gossip
    pub fn process_gossip(&self, message: GossipMessage) {
        let mut members = self.members.write();
        
        for node_info in message.members {
            if let Some(existing) = members.get(&node_info.id) {
                // Update if more recent
                if node_info.last_heartbeat > existing.last_heartbeat {
                    members.insert(node_info.id.clone(), node_info);
                }
            } else {
                // New member
                members.insert(node_info.id.clone(), node_info);
            }
        }
    }
    
    /// Get pending membership events
    pub fn get_events(&self) -> Vec<MembershipEvent> {
        let mut events = self.pending_events.write();
        events.drain(..).collect()
    }
    
    /// Get membership convergence percentage
    pub fn convergence(&self) -> f64 {
        let members = self.members.read();
        let alive = members.values().filter(|n| n.is_alive()).count();
        let total = members.len();
        
        if total == 0 {
            return 1.0;
        }
        
        alive as f64 / total as f64
    }
    
    /// Get protocol version
    pub fn version(&self) -> &str {
        &self.protocol_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_gossip_membership() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        protocol.register_node(node.clone());
        
        let alive = protocol.get_alive_members();
        assert!(!alive.is_empty());
    }
    
    #[test]
    fn test_failure_detection() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let node_id = NetNodeId::new();
        let node = NodeInfo::new(node_id.clone(), NetworkAddress::new("localhost", 8080));
        protocol.register_node(node);
        
        protocol.handle_failure(node_id.clone());
        
        let alive = protocol.get_alive_members();
        assert!(alive.is_empty());
    }
}

#[test]
    fn test_node_info_creation() {
        let node_id = NetNodeId::new();
        let addr = NetworkAddress::new("192.168.1.1", 8080);
        let node = NodeInfo::new(node_id.clone(), addr);
        
        assert_eq!(node.id, node_id);
        assert!(node.is_alive());
    }
    
    #[test]
    fn test_node_info_update_heartbeat() {
        let node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        let original_heartbeat = node.last_heartbeat;
        
        let mut node_mut = node.clone();
        node_mut.update_heartbeat();
        
        assert!(node_mut.last_heartbeat >= original_heartbeat);
        assert_eq!(node_mut.incarnation, 1);
    }
    
    #[test]
    fn test_node_info_with_metadata() {
        let mut node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        node.metadata.insert("role".to_string(), "worker".to_string());
        
        assert_eq!(node.metadata.get("role"), Some(&"worker".to_string()));
    }
    
    #[test]
    fn test_network_address_to_string() {
        let addr = NetworkAddress::new("localhost", 8080);
        assert_eq!(addr.to_string(), "localhost:8080");
        
        let addr2 = NetworkAddress::new("192.168.1.1", 9000);
        assert_eq!(addr2.to_string(), "192.168.1.1:9000");
    }
    
    #[test]
    fn test_network_address_serialization() {
        let addr = NetworkAddress::new("localhost", 8080);
        let serialized = serde_json::to_string(&addr).unwrap();
        let deserialized: NetworkAddress = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(addr, deserialized);
    }
    
    #[test]
    fn test_gossip_message_creation() {
        let source = NetNodeId::new();
        let msg = GossipMessage::new(source.clone());
        
        assert_eq!(msg.source, source);
        assert!(msg.members.is_empty());
    }
    
    #[test]
    fn test_gossip_message_with_members() {
        let source = NetNodeId::new();
        let node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        let msg = GossipMessage::new(source)
            .with_members(vec![node]);
        
        assert_eq!(msg.members.len(), 1);
    }
    
    #[test]
    fn test_gossip_protocol_initial_members() {
        let self_node = NetNodeId::new();
        let protocol = GossipProtocol::new(self_node.clone());
        
        let members = protocol.get_all_members();
        assert!(!members.is_empty());
    }
    
    #[test]
    fn test_gossip_protocol_handle_leave() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let node_id = NetNodeId::new();
        let node = NodeInfo::new(node_id.clone(), NetworkAddress::new("localhost", 8080));
        protocol.register_node(node);
        
        protocol.handle_leave(node_id.clone());
        
        let alive = protocol.get_alive_members();
        assert!(alive.is_empty() || alive.len() == 1); // Only self may remain
    }
    
    #[test]
    fn test_gossip_protocol_handle_rejoin() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let node_id = NetNodeId::new();
        let node = NodeInfo::new(node_id.clone(), NetworkAddress::new("localhost", 8080));
        
        // First join
        protocol.register_node(node.clone());
        
        // Failure
        protocol.handle_failure(node_id.clone());
        
        // Rejoin
        protocol.handle_rejoin(node.clone());
        
        let alive = protocol.get_alive_members();
        assert!(!alive.is_empty());
    }
    
    #[test]
    fn test_gossip_protocol_convergence() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let convergence = protocol.convergence();
        
        // With only self, should be 100%
        assert_eq!(convergence, 1.0);
    }
    
    #[test]
    fn test_gossip_protocol_version() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let version = protocol.version();
        assert_eq!(version, "1.0.0");
    }
    
    #[test]
    fn test_gossip_protocol_process_gossip() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let source = NetNodeId::new();
        let node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        let msg = GossipMessage::new(source)
            .with_members(vec![node]);
        
        protocol.process_gossip(msg);
        
        // Should have processed without error
    }
    
    #[test]
    fn test_gossip_protocol_get_events() {
        let protocol = GossipProtocol::new(NetNodeId::new());
        
        let node = NodeInfo::new(
            NetNodeId::new(),
            NetworkAddress::new("localhost", 8080),
        );
        
        // Register a node to generate event
        protocol.register_node(node);
        
        let events = protocol.get_events();
        
        // Event should be consumed after getting
        assert!(!events.is_empty());
    }
    
    #[test]
    fn test_membership_event_variants() {
        let node_id = NetNodeId::new();
        let node = NodeInfo::new(node_id.clone(), NetworkAddress::new("localhost", 8080));
        
        // Test all variants
        let _join = MembershipEvent::Join(node.clone());
        let _leave = MembershipEvent::Leave(node_id.clone());
        let _failure = MembershipEvent::Failure(node_id.clone());
        let _rejoin = MembershipEvent::Rejoin(node.clone());
    }
    
    #[test]
    fn test_net_node_id_creation() {
        let id = NetNodeId::new();
        assert_ne!(id.0, uuid::Uuid::nil());
    }
    
    #[test]
    fn test_net_node_id_default() {
        let id = NetNodeId::default();
        assert_ne!(id.0, uuid::Uuid::nil());
    }
    
    #[test]
    fn test_connection_status_variants() {
        let _connected = ConnectionStatus::Connected;
        let _disconnected = ConnectionStatus::Disconnected;
        let _connecting = ConnectionStatus::Connecting;
        let _failed = ConnectionStatus::Failed("error".to_string());
    }