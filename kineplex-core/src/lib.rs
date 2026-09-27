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
pub mod geometry;

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

impl std::fmt::Display for GraphId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stage in the execution pipeline
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

impl Stage {
    pub fn name(&self) -> &'static str {
        match self {
            Stage::Receptor => "receptor",
            Stage::Wasm => "wasm",
            Stage::Aggregation => "aggregation",
            Stage::Terminal => "terminal",
        }
    }
    
    pub fn all() -> Vec<Stage> {
        vec![Stage::Receptor, Stage::Wasm, Stage::Aggregation, Stage::Terminal]
    }
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

impl GraphStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, GraphStatus::Completed | GraphStatus::Failed(_) | GraphStatus::Cancelled)
    }
    
    pub fn is_active(&self) -> bool {
        matches!(self, GraphStatus::Pending | GraphStatus::Running)
    }
    
    pub fn as_str(&self) -> &str {
        match self {
            GraphStatus::Pending => "pending",
            GraphStatus::Running => "running",
            GraphStatus::Completed => "completed",
            GraphStatus::Failed(_) => "failed",
            GraphStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for GraphStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphStatus::Pending => write!(f, "pending"),
            GraphStatus::Running => write!(f, "running"),
            GraphStatus::Completed => write!(f, "completed"),
            GraphStatus::Failed(msg) => write!(f, "failed: {}", msg),
            GraphStatus::Cancelled => write!(f, "cancelled"),
        }
    }
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

impl Graph {
    pub fn new(id: GraphId, config: GraphConfig) -> Self {
        Self {
            id,
            status: GraphStatus::Pending,
            stages: Stage::all(),
            tenant_id: None,
            config,
        }
    }
    
    pub fn with_tenant(mut self, tenant_id: String) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }
    
    pub fn with_status(mut self, status: GraphStatus) -> Self {
        self.status = status;
        self
    }
    
    pub fn stages_count(&self) -> usize {
        self.stages.len()
    }
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

impl GraphConfig {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_max_memory(mut self, mb: u64) -> Self {
        self.max_memory_mb = mb;
        self
    }
    
    pub fn with_max_fuel(mut self, fuel: u64) -> Self {
        self.max_fuel = fuel;
        self
    }
    
    pub fn with_simd(mut self, enable: bool) -> Self {
        self.enable_simd = enable;
        self
    }
    
    pub fn with_output_path(mut self, path: String) -> Self {
        self.output_path = Some(path);
        self
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

impl ExecutionResult {
    pub fn new(graph_id: GraphId, status: GraphStatus, metrics: ExecutionMetrics) -> Self {
        Self {
            graph_id,
            status,
            output: None,
            output_path: None,
            metrics,
        }
    }
    
    pub fn with_output(mut self, data: Vec<u8>) -> Self {
        self.output = Some(data);
        self
    }
    
    pub fn with_output_path(mut self, path: String) -> Self {
        self.output_path = Some(path);
        self
    }
    
    pub fn is_success(&self) -> bool {
        self.status == GraphStatus::Completed
    }
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

impl ExecutionMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_stage_time(mut self, stage: &str, time_ms: u64) -> Self {
        self.stage_times_ms.insert(stage.to_string(), time_ms);
        self
    }
    
    pub fn with_memory(mut self, bytes: u64) -> Self {
        self.memory_peak_bytes = bytes;
        self
    }
    
    pub fn with_cpu_time(mut self, time_ms: u64) -> Self {
        self.cpu_time_ms = time_ms;
        self
    }
    
    pub fn with_rows(mut self, count: u64) -> Self {
        self.rows_processed = count;
        self
    }
    
    pub fn with_nodes(mut self, count: u32) -> Self {
        self.nodes_involved = count;
        self
    }
    
    pub fn total_stage_time(&self) -> u64 {
        self.stage_times_ms.values().sum()
    }
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

/// Create a simple graph with a single stage for testing
pub fn create_simple_graph(tenant_id: Option<String>) -> Graph {
    create_distributed_graph(tenant_id, GraphConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    // GraphId tests
    #[test]
    fn test_graph_id_creation() {
        let id = GraphId::new();
        assert_ne!(id.0, Uuid::nil());
    }
    
    #[test]
    fn test_graph_id_default() {
        let id = GraphId::default();
        assert_ne!(id.0, Uuid::nil());
    }
    
    #[test]
    fn test_graph_id_display() {
        let id = GraphId::new();
        let _ = format!("{}", id);
    }
    
    #[test]
    fn test_graph_id_serialization() {
        let id = GraphId::new();
        let serialized = serde_json::to_string(&id).unwrap();
        let deserialized: GraphId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(id, deserialized);
    }

    // Stage tests
    #[test]
    fn test_stage_name() {
        assert_eq!(Stage::Receptor.name(), "receptor");
        assert_eq!(Stage::Wasm.name(), "wasm");
        assert_eq!(Stage::Aggregation.name(), "aggregation");
        assert_eq!(Stage::Terminal.name(), "terminal");
    }
    
    #[test]
    fn test_stage_all() {
        let stages = Stage::all();
        assert_eq!(stages.len(), 4);
    }

    // GraphStatus tests
    #[test]
    fn test_graph_status_is_terminal() {
        assert!(!GraphStatus::Pending.is_terminal());
        assert!(!GraphStatus::Running.is_terminal());
        assert!(GraphStatus::Completed.is_terminal());
        assert!(GraphStatus::Failed("error".to_string()).is_terminal());
        assert!(GraphStatus::Cancelled.is_terminal());
    }
    
    #[test]
    fn test_graph_status_is_active() {
        assert!(GraphStatus::Pending.is_active());
        assert!(GraphStatus::Running.is_active());
        assert!(!GraphStatus::Completed.is_active());
        assert!(!GraphStatus::Failed("error".to_string()).is_active());
        assert!(!GraphStatus::Cancelled.is_active());
    }
    
    #[test]
    fn test_graph_status_as_str() {
        assert_eq!(GraphStatus::Pending.as_str(), "pending");
        assert_eq!(GraphStatus::Running.as_str(), "running");
        assert_eq!(GraphStatus::Completed.as_str(), "completed");
        assert_eq!(GraphStatus::Failed("err".to_string()).as_str(), "failed");
        assert_eq!(GraphStatus::Cancelled.as_str(), "cancelled");
    }
    
    #[test]
    fn test_graph_status_display() {
        assert_eq!(format!("{}", GraphStatus::Pending), "pending");
        assert_eq!(format!("{}", GraphStatus::Running), "running");
        assert_eq!(format!("{}", GraphStatus::Completed), "completed");
        assert_eq!(format!("{}", GraphStatus::Failed("err".to_string())), "failed: err");
        assert_eq!(format!("{}", GraphStatus::Cancelled), "cancelled");
    }
    
    #[test]
    fn test_graph_status_serialization() {
        let status = GraphStatus::Failed("test error".to_string());
        let serialized = serde_json::to_string(&status).unwrap();
        let deserialized: GraphStatus = serde_json::from_str(&serialized).unwrap();
        assert_eq!(status, deserialized);
    }

    // GraphConfig tests
    #[test]
    fn test_graph_config_default() {
        let config = GraphConfig::default();
        assert_eq!(config.max_memory_mb, 512);
        assert_eq!(config.max_fuel, 1_000_000);
        assert!(config.enable_simd);
        assert!(config.output_path.is_none());
    }
    
    #[test]
    fn test_graph_config_builder() {
        let config = GraphConfig::new()
            .with_max_memory(1024)
            .with_max_fuel(2_000_000)
            .with_simd(false)
            .with_output_path("/output".to_string());
        
        assert_eq!(config.max_memory_mb, 1024);
        assert_eq!(config.max_fuel, 2_000_000);
        assert!(!config.enable_simd);
        assert_eq!(config.output_path, Some("/output".to_string()));
    }
    
    #[test]
    fn test_graph_config_serialization() {
        let config = GraphConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: GraphConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    // Graph tests
    #[test]
    fn test_graph_creation() {
        let graph = create_distributed_graph(
            Some("tenant-1".to_string()),
            GraphConfig::default(),
        );
        
        assert_eq!(graph.stages.len(), 4);
        assert_eq!(graph.status, GraphStatus::Pending);
        assert_eq!(graph.tenant_id, Some("tenant-1".to_string()));
    }
    
    #[test]
    fn test_graph_new() {
        let graph = Graph::new(GraphId::new(), GraphConfig::default());
        assert_eq!(graph.stages.len(), 4);
    }
    
    #[test]
    fn test_graph_with_tenant() {
        let graph = Graph::new(GraphId::new(), GraphConfig::default())
            .with_tenant("tenant-2".to_string());
        assert_eq!(graph.tenant_id, Some("tenant-2".to_string()));
    }
    
    #[test]
    fn test_graph_with_status() {
        let graph = Graph::new(GraphId::new(), GraphConfig::default())
            .with_status(GraphStatus::Running);
        assert_eq!(graph.status, GraphStatus::Running);
    }
    
    #[test]
    fn test_graph_stages_count() {
        let graph = create_distributed_graph(None, GraphConfig::default());
        assert_eq!(graph.stages_count(), 4);
    }
    
    #[test]
    fn test_graph_serialization() {
        let graph = create_distributed_graph(
            Some("tenant-1".to_string()),
            GraphConfig::default(),
        );
        let serialized = serde_json::to_string(&graph).unwrap();
        let deserialized: Graph = serde_json::from_str(&serialized).unwrap();
        assert_eq!(graph.id, deserialized.id);
        assert_eq!(graph.tenant_id, deserialized.tenant_id);
    }

    // ExecutionMetrics tests
    #[test]
    fn test_execution_metrics_default() {
        let metrics = ExecutionMetrics::default();
        assert_eq!(metrics.total_time_ms, 0);
        assert!(metrics.stage_times_ms.is_empty());
    }
    
    #[test]
    fn test_execution_metrics_builder() {
        let metrics = ExecutionMetrics::new()
            .with_stage_time("receptor", 100)
            .with_stage_time("wasm", 200)
            .with_memory(1024)
            .with_cpu_time(300)
            .with_rows(1000)
            .with_nodes(2);
        
        assert_eq!(metrics.stage_times_ms.len(), 2);
        assert_eq!(metrics.memory_peak_bytes, 1024);
        assert_eq!(metrics.cpu_time_ms, 300);
        assert_eq!(metrics.rows_processed, 1000);
        assert_eq!(metrics.nodes_involved, 2);
    }
    
    #[test]
    fn test_execution_metrics_total_stage_time() {
        let metrics = ExecutionMetrics::new()
            .with_stage_time("receptor", 100)
            .with_stage_time("wasm", 200);
        
        assert_eq!(metrics.total_stage_time(), 300);
    }

    // ExecutionResult tests
    #[test]
    fn test_execution_result_creation() {
        let result = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Completed,
            ExecutionMetrics::default(),
        );
        
        assert_eq!(result.status, GraphStatus::Completed);
        assert!(result.output.is_none());
        assert!(result.output_path.is_none());
    }
    
    #[test]
    fn test_execution_result_with_output() {
        let result = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Completed,
            ExecutionMetrics::default(),
        ).with_output(vec![1, 2, 3]);
        
        assert!(result.output.is_some());
    }
    
    #[test]
    fn test_execution_result_with_output_path() {
        let result = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Completed,
            ExecutionMetrics::default(),
        ).with_output_path("/output/data.parquet".to_string());
        
        assert!(result.output_path.is_some());
    }
    
    #[test]
    fn test_execution_result_is_success() {
        let success = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Completed,
            ExecutionMetrics::default(),
        );
        
        let failure = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Failed("error".to_string()),
            ExecutionMetrics::default(),
        );
        
        assert!(success.is_success());
        assert!(!failure.is_success());
    }
    
    #[test]
    fn test_execution_result_serialization() {
        let result = ExecutionResult::new(
            GraphId::new(),
            GraphStatus::Completed,
            ExecutionMetrics::default(),
        );
        
        let serialized = serde_json::to_string(&result).unwrap();
        let deserialized: ExecutionResult = serde_json::from_str(&serialized).unwrap();
        assert_eq!(result.graph_id, deserialized.graph_id);
    }

    // Helper functions tests
    #[test]
    fn test_create_distributed_graph() {
        let graph = create_distributed_graph(Some("tenant-1".to_string()), GraphConfig::default());
        
        assert_eq!(graph.stages.len(), 4);
        assert_eq!(graph.stages[0], Stage::Receptor);
        assert_eq!(graph.stages[1], Stage::Wasm);
        assert_eq!(graph.stages[2], Stage::Aggregation);
        assert_eq!(graph.stages[3], Stage::Terminal);
    }
    
    #[test]
    fn test_create_simple_graph() {
        let graph = create_simple_graph(Some("tenant-1".to_string()));
        assert_eq!(graph.stages.len(), 4);
    }
}