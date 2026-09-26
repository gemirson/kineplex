//! KinePlex Core - Core infrastructure for distributed data plane execution
//! 
//! This module provides the fundamental building blocks for the KinePlex system:
//! - Graph execution engine
//! - Wasm runtime integration
//! - Arrow/IPC data handling
//! - Metrics and observability

pub mod wasm;
pub mod graph;
pub mod data;
pub mod metrics;
pub mod error;

pub use error::{CoreError, Result};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::sync::Arc;
use parking_lot::RwLock;

/// Unique identifier for a graph execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphId(pub Uuid);

impl GraphId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for GraphId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stage in the execution pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Stage {
    /// Reception stage - receives input data
    Receptor,
    /// Wasm execution stage
    Wasm,
    /// Aggregation stage
    Aggregation,
    /// Terminal stage - produces final output
    Terminal,
}

/// Status of a graph execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GraphStatus {
    /// Graph is pending execution
    Pending,
    /// Graph is currently executing
    Running,
    /// Graph execution completed successfully
    Completed,
    /// Graph execution failed
    Failed(String),
    /// Graph was cancelled
    Cancelled,
}

/// Graph definition for execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    /// Unique identifier
    pub id: GraphId,
    /// Current status
    pub status: GraphStatus,
    /// Stages in the pipeline
    pub stages: Vec<Stage>,
    /// Tenant ID for multi-tenant isolation
    pub tenant_id: Option<String>,
    /// Configuration
    pub config: GraphConfig,
}

/// Configuration for graph execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// Maximum memory in MB
    pub max_memory_mb: u64,
    /// Maximum fuel (instructions)
    pub max_fuel: u64,
    /// Enable SIMD optimizations
    pub enable_simd: bool,
    /// Output path for Parquet files
    pub output_path: Option<String>,
}

impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            max_memory_mb: 512,
            max_fuel: 1_000_000,
            enable_simd: true,
            output_path: None,
        }
    }
}

/// Graph executor trait - implements the distributed execution pipeline
pub trait GraphExecutor: Send + Sync {
    /// Execute a graph across the distributed pipeline
    fn execute(&self, graph: &Graph) -> impl std::future::Future<Output = Result<ExecutionResult>> + Send;
    
    /// Get current execution status
    fn status(&self, graph_id: &GraphId) -> Result<GraphStatus>;
    
    /// Cancel a running execution
    fn cancel(&self, graph_id: &GraphId) -> Result<()>;
}

/// Result of graph execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Graph ID
    pub graph_id: GraphId,
    /// Final status
    pub status: GraphStatus,
    /// Output data (Arrow IPC format)
    pub output: Option<Vec<u8>>,
    /// Output Parquet path (if materialized)
    pub output_path: Option<String>,
    /// Execution metrics
    pub metrics: ExecutionMetrics,
}

/// Metrics collected during execution
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutionMetrics {
    /// Total execution time in milliseconds
    pub total_time_ms: u64,
    /// Time per stage in milliseconds
    pub stage_times_ms: std::collections::HashMap<String, u64>,
    /// Memory peak in bytes
    pub memory_peak_bytes: u64,
    /// CPU time in milliseconds
    pub cpu_time_ms: u64,
    /// Number of rows processed
    pub rows_processed: u64,
    /// Number of nodes involved
    pub nodes_involved: u32,
}

/// Create a new graph with the standard distributed pipeline
pub fn create_distributed_graph(tenant_id: Option<String>, config: GraphConfig) -> Graph {
    Graph {
        id: GraphId::new(),
        status: GraphStatus::Pending,
        stages: vec![
            Stage::Receptor,
            Stage::Wasm,
            Stage::Aggregation,
            Stage::Terminal,
        ],
        tenant_id,
        config,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_creation() {
        let graph = create_distributed_graph(
            Some("tenant-1".to_string()),
            GraphConfig::default(),
        );
        
        assert_eq!(graph.stages.len(), 4);
        assert_eq!(graph.status, GraphStatus::Pending);
    }

    #[test]
    fn test_graph_id() {
        let id1 = GraphId::new();
        let id2 = GraphId::new();
        
        assert_ne!(id1, id2);
    }
}