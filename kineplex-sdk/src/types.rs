//! Types for the SDK

use serde::{Deserialize, Serialize};

/// Request to submit a graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSubmitRequest {
    /// Tenant ID
    pub tenant_id: String,
    /// Graph configuration
    pub config: GraphConfigDto,
    /// Idempotency key (optional)
    pub idempotency_key: Option<String>,
}

/// Graph configuration DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfigDto {
    /// Max memory in MB
    pub max_memory_mb: Option<u64>,
    /// Max fuel
    pub max_fuel: Option<u64>,
    /// Enable SIMD
    pub enable_simd: Option<bool>,
    /// Output path
    pub output_path: Option<String>,
    /// Required nodes
    pub required_nodes: Option<u32>,
}

impl Default for GraphConfigDto {
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

/// Response for graph status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStatusResponse {
    pub graph_id: String,
    pub status: String,
    pub stages_completed: Vec<String>,
    pub metrics: Option<MetricsDto>,
}

/// Metrics DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsDto {
    pub total_time_ms: u64,
    pub rows_processed: u64,
    pub nodes_involved: u32,
}

/// Allocation result DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationResultDto {
    pub graph_id: String,
    pub allocated_nodes: Vec<String>,
    pub estimated_completion: String,
}