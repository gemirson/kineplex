//! KinePlex Node - Control plane for graph submission, validation, and allocation
//! 
//! This module provides:
//! - Graph submission and validation
//! - Node allocation in the cluster
//! - Control plane API

pub mod control;
pub mod execution;

pub use control::{NodeControl, SubmissionRequest, AllocationResult};
pub use execution::PipelineExecutor;

use kineplex_core::{GraphId, Graph, GraphStatus, Result, create_distributed_graph, GraphConfig};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;

/// Node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Node state in the cluster
#[derive(Debug, Clone)]
pub enum NodeState {
    /// Node is starting up
    Starting,
    /// Node is ready to accept work
    Ready,
    /// Node is executing a graph
    Executing(GraphId),
    /// Node is shutting down
    ShuttingDown,
    /// Node has failed
    Failed(String),
}

/// Cluster node information
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: NodeId,
    pub state: NodeState,
    pub capacity: NodeCapacity,
    pub current_load: f64,
}

/// Capacity of a node
#[derive(Debug, Clone)]
pub struct NodeCapacity {
    pub max_concurrent: u32,
    pub memory_mb: u64,
    pub cpu_cores: u32,
}

impl Default for NodeCapacity {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            memory_mb: 2048,
            cpu_cores: 4,
        }
    }
}

/// Cluster manager for node coordination
pub struct ClusterManager {
    nodes: RwLock<HashMap<NodeId, ClusterNode>>,
    self_node: NodeId,
}

impl ClusterManager {
    pub fn new() -> Self {
        let self_node = NodeId::new();
        let mut nodes = HashMap::new();
        
        // Add self as a node
        nodes.insert(self_node.clone(), ClusterNode {
            id: self_node.clone(),
            state: NodeState::Ready,
            capacity: NodeCapacity::default(),
            current_load: 0.0,
        });
        
        Self {
            nodes: RwLock::new(nodes),
            self_node,
        }
    }
    
    /// Register a node in the cluster
    pub fn register_node(&self, node: ClusterNode) {
        let mut nodes = self.nodes.write();
        nodes.insert(node.id.clone(), node);
    }
    
    /// Get nodes that are ready to execute
    pub fn get_ready_nodes(&self) -> Vec<NodeId> {
        let nodes = self.nodes.read();
        nodes.values()
            .filter(|n| matches!(n.state, NodeState::Ready))
            .map(|n| n.id.clone())
            .collect()
    }
    
    /// Allocate nodes for graph execution
    pub fn allocate_nodes(&self, required: u32) -> Vec<NodeId> {
        let ready = self.get_ready_nodes();
        ready.into_iter().take(required as usize).collect()
    }
    
    /// Update node state
    pub fn update_node_state(&self, node_id: &NodeId, state: NodeState) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            node.state = state;
        }
    }
    
    /// Get self node ID
    pub fn self_node_id(&self) -> &NodeId {
        &self.self_node
    }
    
    /// Handle node rejoin after failure
    pub fn handle_rejoin(&self, node_id: NodeId) {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(&node_id) {
            node.state = NodeState::Ready;
        } else {
            // Re-register the node
            nodes.insert(node_id.clone(), ClusterNode {
                id: node_id,
                state: NodeState::Ready,
                capacity: NodeCapacity::default(),
                current_load: 0.0,
            });
        }
    }
}

impl Default for ClusterManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kineplex_core::{GraphId, GraphStatus};
    
    // NodeId tests
    #[test]
    fn test_node_id_creation() {
        let id = NodeId::new();
        assert!(!id.0.is_empty());
    }
    
    #[test]
    fn test_node_id_default() {
        let id = NodeId::default();
        assert!(!id.0.is_empty());
    }
    
    #[test]
    fn test_node_id_equality() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_node_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let id = NodeId::new();
        set.insert(id.clone());
        assert!(set.contains(&id));
    }

    // NodeState tests
    #[test]
    fn test_node_state_variants() {
        let _starting = NodeState::Starting;
        let _ready = NodeState::Ready;
        let _executing = NodeState::Executing(GraphId::new());
        let _shutting_down = NodeState::ShuttingDown;
        let _failed = NodeState::Failed("error".to_string());
    }
    
    #[test]
    fn test_node_state_debug() {
        let state = NodeState::Ready;
        let _ = format!("{:?}", state);
    }

    // NodeCapacity tests
    #[test]
    fn test_node_capacity_default() {
        let capacity = NodeCapacity::default();
        assert_eq!(capacity.max_concurrent, 4);
        assert_eq!(capacity.memory_mb, 2048);
        assert_eq!(capacity.cpu_cores, 4);
    }
    
    #[test]
    fn test_node_capacity_custom() {
        let capacity = NodeCapacity {
            max_concurrent: 8,
            memory_mb: 4096,
            cpu_cores: 8,
        };
        
        assert_eq!(capacity.max_concurrent, 8);
        assert_eq!(capacity.memory_mb, 4096);
    }

    // ClusterNode tests
    #[test]
    fn test_cluster_node_creation() {
        let node = ClusterNode {
            id: NodeId::new(),
            state: NodeState::Ready,
            capacity: NodeCapacity::default(),
            current_load: 0.5,
        };
        
        assert_eq!(node.current_load, 0.5);
    }

    // ClusterManager tests
    #[test]
    fn test_cluster_manager_new() {
        let manager = ClusterManager::new();
        
        // Should have at least self node
        let nodes = manager.nodes.read();
        assert!(!nodes.is_empty());
    }
    
    #[test]
    fn test_cluster_manager_get_ready_nodes() {
        let manager = ClusterManager::new();
        
        let ready = manager.get_ready_nodes();
        assert!(!ready.is_empty());
    }
    
    #[test]
    fn test_cluster_manager_allocate_nodes() {
        let manager = ClusterManager::new();
        
        let allocated = manager.allocate_nodes(2);
        assert!(allocated.len() <= 2);
    }
    
    #[test]
    fn test_cluster_manager_allocate_more_than_available() {
        let manager = ClusterManager::new();
        
        // Try to allocate more nodes than available
        let allocated = manager.allocate_nodes(100);
        // Should allocate what is available
        assert!(allocated.len() >= 1);
    }
    
    #[test]
    fn test_cluster_manager_update_node_state() {
        let manager = ClusterManager::new();
        let node_id = manager.self_node_id().clone();
        
        // Update to executing
        manager.update_node_state(&node_id, NodeState::Executing(GraphId::new()));
        
        // Verify state changed
        let nodes = manager.nodes.read();
        let node = nodes.get(&node_id).unwrap();
        assert!(matches!(node.state, NodeState::Executing(_)));
        
        // Reset to ready
        manager.update_node_state(&node_id, NodeState::Ready);
    }
    
    #[test]
    fn test_cluster_manager_self_node_id() {
        let manager = ClusterManager::new();
        let self_id = manager.self_node_id();
        
        assert!(!self_id.0.is_empty());
    }
    
    #[test]
    fn test_cluster_manager_handle_rejoin() {
        let manager = ClusterManager::new();
        
        let node_id = NodeId::new();
        
        // Handle rejoin of new node
        manager.handle_rejoin(node_id.clone());
        
        // Verify node is registered
        let nodes = manager.nodes.read();
        assert!(nodes.contains_key(&node_id));
    }
    
    #[test]
    fn test_cluster_manager_register_node() {
        let manager = ClusterManager::new();
        
        let new_node = ClusterNode {
            id: NodeId::new(),
            state: NodeState::Ready,
            capacity: NodeCapacity::default(),
            current_load: 0.0,
        };
        
        manager.register_node(new_node.clone());
        
        let nodes = manager.nodes.read();
        assert!(nodes.contains_key(&new_node.id));
    }
    
    #[test]
    fn test_cluster_manager_update_nonexistent_node() {
        let manager = ClusterManager::new();
        
        // Try to update a node that doesn't exist - should not panic
        let fake_id = NodeId::new();
        manager.update_node_state(&fake_id, NodeState::Ready);
    }
}