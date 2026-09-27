//! Library support for the KinePlex node executable.

pub mod cli;
pub mod control;
pub mod execution;
pub mod profiling;

pub use control::{NodeControl, SubmissionRequest, AllocationResult, SubmissionConfig, ClientCredentials, TenantQuotaInfo};
pub use execution::PipelineExecutor;

use kineplex_core::GraphId;
use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;

/// Node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
    
    #[test]
    fn test_node_id_creation() {
        let id = NodeId::new();
        assert!(!id.0.is_empty());
    }
}
