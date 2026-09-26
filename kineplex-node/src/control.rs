//! Control plane - Graph submission, validation, and allocation
//! 
//! This module implements the control plane logic including:
//! - Submission validation
//! - Graph allocation
//! - Execution coordination

use crate::{ClusterManager, NodeId, ClusterNode, NodeState, NodeCapacity};
use kineplex_core::{
    Result, CoreError, GraphId, Graph, GraphStatus, GraphConfig, 
    create_distributed_graph, ExecutionResult, Stage,
};
use crate::execution::PipelineExecutor;
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Submission request from client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionRequest {
    /// Unique submission ID
    pub submission_id: String,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: String,
    /// Graph configuration
    pub config: SubmissionConfig,
    /// Idempotency key (optional)
    pub idempotency_key: Option<String>,
    /// Client credentials
    pub credentials: Option<ClientCredentials>,
}

/// Configuration for submission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionConfig {
    /// Maximum memory in MB
    pub max_memory_mb: Option<u64>,
    /// Maximum fuel
    pub max_fuel: Option<u64>,
    /// Enable SIMD
    pub enable_simd: Option<bool>,
    /// Output path for results
    pub output_path: Option<String>,
    /// Required nodes for execution
    pub required_nodes: Option<u32>,
}

impl Default for SubmissionConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: Some(512),
            max_fuel: Some(1_000_000),
            enable_simd: Some(true),
            output_path: None,
            required_nodes: Some(1),
        }
    }
}

/// Client credentials for authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCredentials {
    /// Client certificate (for mTLS)
    pub cert: Option<String>,
    /// Client key (for mTLS)
    pub key: Option<String>,
    /// API token
    pub token: Option<String>,
}

/// Allocation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationResult {
    /// Graph ID
    pub graph_id: GraphId,
    /// Allocated nodes
    pub allocated_nodes: Vec<NodeId>,
    /// Estimated completion time
    pub estimated_completion: DateTime<Utc>,
}

/// Node control service
pub struct NodeControl {
    cluster: Arc<ClusterManager>,
    executor: Arc<PipelineExecutor>,
    graphs: RwLock<HashMap<GraphId, Graph>>,
    submissions: RwLock<HashMap<String, SubmissionRequest>>,
    auth_enabled: RwLock<bool>,
    tenant_quotas: RwLock<HashMap<String, TenantQuotaInfo>>,
}

/// Tenant quota information
#[derive(Debug, Clone)]
pub struct TenantQuotaInfo {
    pub tenant_id: String,
    pub max_concurrent_graphs: u32,
    pub max_memory_mb: u64,
    pub max_fuel: u64,
    pub active_graphs: u32,
}

impl NodeControl {
    pub fn new(cluster: Arc<ClusterManager>, executor: Arc<PipelineExecutor>) -> Self {
        Self {
            cluster,
            executor,
            graphs: RwLock::new(HashMap::new()),
            submissions: RwLock::new(HashMap::new()),
            auth_enabled: RwLock::new(false),
            tenant_quotas: RwLock::new(HashMap::new()),
        }
    }
    
    /// Enable or disable authentication
    pub fn set_auth_enabled(&self, enabled: bool) {
        *self.auth_enabled.write() = enabled;
    }
    
    /// Register a tenant with quota
    pub fn register_tenant(&self, quota: TenantQuotaInfo) {
        let mut quotas = self.tenant_quotas.write();
        quotas.insert(quota.tenant_id.clone(), quota);
    }
    
    /// Validate submission request
    pub fn validate_submission(&self, request: &SubmissionRequest) -> Result<()> {
        // Check authentication if enabled
        let auth_enabled = *self.auth_enabled.read();
        if auth_enabled {
            if request.credentials.is_none() {
                return Err(CoreError::AuthenticationRequired);
            }
        }
        
        // Check tenant quota
        let mut quotas = self.tenant_quotas.write();
        if let Some(quota) = quotas.get_mut(&request.tenant_id) {
            if quota.active_graphs >= quota.max_concurrent_graphs {
                return Err(CoreError::QuotaExceeded(format!(
                    "Tenant {} has reached maximum concurrent graphs",
                    request.tenant_id
                )));
            }
            quota.active_graphs += 1;
        }
        
        Ok(())
    }
    
    /// Submit a graph for execution
    pub fn submit(&self, request: SubmissionRequest) -> Result<AllocationResult> {
        // Validate submission
        self.validate_submission(&request)?;
        
        // Create graph config
        let config = GraphConfig {
            max_memory_mb: request.config.max_memory_mb.unwrap_or(512),
            max_fuel: request.config.max_fuel.unwrap_or(1_000_000),
            enable_simd: request.config.enable_simd.unwrap_or(true),
            output_path: request.config.output_path.clone(),
        };
        
        // Create graph with distributed pipeline
        let graph = create_distributed_graph(Some(request.tenant_id.clone()), config);
        let graph_id = graph.id.clone();
        
        // Store graph
        self.graphs.write().insert(graph_id.clone(), graph);
        
        // Store submission
        self.submissions.write().insert(request.submission_id.clone(), request);
        
        // Allocate nodes
        let required_nodes = request.config.required_nodes.unwrap_or(1) as u32;
        let allocated = self.cluster.allocate_nodes(required_nodes);
        
        // Update node states
        for node_id in &allocated {
            self.cluster.update_node_state(node_id, NodeState::Executing(graph_id.clone()));
        }
        
        let result = AllocationResult {
            graph_id,
            allocated_nodes: allocated,
            estimated_completion: Utc::now() + chrono::Duration::minutes(5),
        };
        
        Ok(result)
    }
    
    /// Get graph status
    pub fn get_graph_status(&self, graph_id: &GraphId) -> Result<GraphStatus> {
        let graphs = self.graphs.read();
        let graph = graphs.get(graph_id)
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))?;
        Ok(graph.status.clone())
    }
    
    /// Cancel a graph execution
    pub fn cancel(&self, graph_id: &GraphId) -> Result<()> {
        let mut graphs = self.graphs.write();
        let graph = graphs.get_mut(graph_id)
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))?;
        
        // Only cancel if running
        match graph.status {
            GraphStatus::Running => {
                graph.status = GraphStatus::Cancelled;
                
                // Update cluster nodes
                self.cluster.update_node_state(
                    self.cluster.self_node_id(),
                    NodeState::Ready,
                );
                Ok(())
            }
            _ => Err(CoreError::InvalidState("Graph is not running".to_string())),
        }
    }
    
    /// Handle node failure and reallocation
    pub fn handle_node_failure(&self, failed_node: &NodeId, graph_id: &GraphId) -> Result<Vec<NodeId>> {
        // Update failed node
        self.cluster.update_node_state(
            failed_node,
            NodeState::Failed("Node failed".to_string()),
        );
        
        // Reallocate to another node
        let new_allocation = self.cluster.allocate_nodes(1);
        
        // Update new node state
        for node_id in &new_allocation {
            self.cluster.update_node_state(node_id, NodeState::Executing(graph_id.clone()));
        }
        
        Ok(new_allocation)
    }
    
    /// Handle node rejoin
    pub fn handle_node_rejoin(&self, node_id: NodeId) {
        self.cluster.handle_rejoin(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_submission() {
        let cluster = Arc::new(ClusterManager::new());
        let executor = Arc::new(PipelineExecutor::new());
        let control = NodeControl::new(cluster, executor);
        
        let request = SubmissionRequest {
            submission_id: "sub-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            config: SubmissionConfig::default(),
            idempotency_key: None,
            credentials: None,
        };
        
        let result = control.submit(request);
        assert!(result.is_ok());
    }
}