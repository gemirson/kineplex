//! Graph execution engine
//! 
//! This module provides the graph execution logic including:
//! - Graph state management
//! - Execution pipeline orchestration
//! - Idempotency checking

use crate::{Result, CoreError, GraphId, Graph, GraphStatus, ExecutionResult, ExecutionMetrics};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::time::Instant;
use chrono::Utc;

/// Graph store for managing graph state
pub struct GraphStore {
    graphs: RwLock<HashMap<GraphId, Graph>>,
    idempotency_keys: RwLock<HashMap<String, GraphId>>,
}

impl GraphStore {
    pub fn new() -> Self {
        Self {
            graphs: RwLock::new(HashMap::new()),
            idempotency_keys: RwLock::new(HashMap::new()),
        }
    }
    
    /// Create a new graph
    pub fn create_graph(&self, graph: Graph) -> Result<GraphId> {
        let mut graphs = self.graphs.write();
        graphs.insert(graph.id.clone(), graph.clone());
        Ok(graph.id)
    }
    
    /// Get a graph by ID
    pub fn get_graph(&self, graph_id: &GraphId) -> Result<Graph> {
        let graphs = self.graphs.read();
        graphs.get(graph_id)
            .cloned()
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))
    }
    
    /// Update graph status
    pub fn update_status(&self, graph_id: &GraphId, status: GraphStatus) -> Result<()> {
        let mut graphs = self.graphs.write();
        let graph = graphs.get_mut(graph_id)
            .ok_or_else(|| CoreError::GraphNotFound(graph_id.0.to_string()))?;
        graph.status = status;
        Ok(())
    }
    
    /// Check idempotency key - returns existing graph ID if found
    pub fn check_idempotency(&self, key: &str) -> Option<GraphId> {
        let keys = self.idempotency_keys.read();
        keys.get(key).cloned()
    }
    
    /// Register idempotency key
    pub fn register_idempotency(&self, key: String, graph_id: GraphId) {
        let mut keys = self.idempotency_keys.write();
        keys.insert(key, graph_id);
    }
    
    /// List all graphs
    pub fn list_graphs(&self) -> Vec<Graph> {
        let graphs = self.graphs.read();
        graphs.values().cloned().collect()
    }
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Idempotency key for submission requests
#[derive(Debug, Clone)]
pub struct IdempotencyKey {
    /// Key value
    pub key: String,
    /// Created at timestamp
    pub created_at: chrono::DateTime<Utc>,
    /// Expires at
    pub expires_at: chrono::DateTime<Utc>,
}

impl IdempotencyKey {
    pub fn new(key: String, ttl_hours: i64) -> Self {
        let now = Utc::now();
        Self {
            key,
            created_at: now,
            expires_at: now + chrono::Duration::hours(ttl_hours),
        }
    }
    
    pub fn is_valid(&self) -> bool {
        Utc::now() < self.expires_at
    }
}

/// Graph executor that orchestrates the distributed pipeline
pub struct DistributedGraphExecutor {
    store: Arc<GraphStore>,
}

impl DistributedGraphExecutor {
    pub fn new(store: Arc<GraphStore>) -> Self {
        Self { store }
    }
    
    /// Execute a graph through the distributed pipeline
    pub async fn execute(&self, graph_id: &GraphId) -> Result<ExecutionResult> {
        let graph = self.store.get_graph(graph_id)?;
        
        let start_time = Instant::now();
        let mut stage_times = HashMap::new();
        
        // Update status to running
        self.store.update_status(graph_id, GraphStatus::Running)?;
        
        // Execute each stage
        for stage in &graph.stages {
            let stage_start = Instant::now();
            
            // Execute stage (in real implementation, this would involve
            // actual distributed execution across nodes)
            match stage {
                crate::Stage::Receptor => {
                    // Reception stage
                    tracing::info!("Executing Receptor stage for graph {}", graph_id.0);
                }
                crate::Stage::Wasm => {
                    // Wasm execution stage
                    tracing::info!("Executing Wasm stage for graph {}", graph_id.0);
                }
                crate::Stage::Aggregation => {
                    // Aggregation stage
                    tracing::info!("Executing Aggregation stage for graph {}", graph_id.0);
                }
                crate::Stage::Terminal => {
                    // Terminal stage - produces output
                    tracing::info!("Executing Terminal stage for graph {}", graph_id.0);
                }
            }
            
            let stage_elapsed = stage_start.elapsed().as_millis() as u64;
            stage_times.insert(format!("{:?}", stage), stage_elapsed);
        }
        
        let total_time = start_time.elapsed().as_millis() as u64;
        
        // Create execution result
        let result = ExecutionResult {
            graph_id: graph_id.clone(),
            status: GraphStatus::Completed,
            output: Some(vec![]), // IPC data would go here
            output_path: graph.config.output_path.clone(),
            metrics: ExecutionMetrics {
                total_time_ms: total_time,
                stage_times_ms: stage_times,
                memory_peak_bytes: 0,
                cpu_time_ms: 0,
                rows_processed: 0,
                nodes_involved: 1,
            },
        };
        
        // Update graph status
        self.store.update_status(graph_id, GraphStatus::Completed)?;
        
        Ok(result)
    }
    
    /// Submit a new graph for execution (with idempotency)
    pub fn submit(&self, graph: Graph, idempotency_key: Option<String>) -> Result<GraphId> {
        // Check idempotency
        if let Some(ref key) = idempotency_key {
            if let Some(existing_id) = self.store.check_idempotency(key) {
                return Ok(existing_id);
            }
        }
        
        let graph_id = graph.id.clone();
        self.store.create_graph(graph.clone())?;
        
        // Register idempotency key
        if let Some(key) = idempotency_key {
            self.store.register_idempotency(key, graph_id.clone());
        }
        
        Ok(graph_id)
    }
    
    /// Cancel a running graph
    pub fn cancel(&self, graph_id: &GraphId) -> Result<()> {
        self.store.update_status(graph_id, GraphStatus::Cancelled)?;
        Ok(())
    }
    
    /// Get graph status
    pub fn status(&self, graph_id: &GraphId) -> Result<GraphStatus> {
        let graph = self.store.get_graph(graph_id)?;
        Ok(graph.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_graph_store() {
        let store = GraphStore::new();
        
        let graph = crate::create_distributed_graph(
            Some("tenant-1".to_string()),
            crate::GraphConfig::default(),
        );
        
        let graph_id = store.create_graph(graph).unwrap();
        let retrieved = store.get_graph(&graph_id).unwrap();
        
        assert_eq!(retrieved.id, graph_id);
    }
    
    #[test]
    fn test_idempotency() {
        let store = GraphStore::new();
        
        let graph = crate::create_distributed_graph(
            Some("tenant-1".to_string()),
            crate::GraphConfig::default(),
        );
        
        let graph_id = store.create_graph(graph).unwrap();
        store.register_idempotency("my-key".to_string(), graph_id.clone());
        
        let existing = store.check_idempotency("my-key");
        assert!(existing.is_some());
        assert_eq!(existing.unwrap(), graph_id);
        
        let not_exists = store.check_idempotency("non-existent");
        assert!(not_exists.is_none());
    }
}



#[cfg(test)]
mod coverage_tests {
    use super::*;

    fn graph() -> Graph {
        crate::create_distributed_graph(Some("tenant".to_string()), crate::GraphConfig::default())
    }

    #[test]
    fn store_updates_status_and_lists_graphs() {
        let store = GraphStore::new();
        let id = store.create_graph(graph()).unwrap();
        store.update_status(&id, GraphStatus::Running).unwrap();
        assert_eq!(store.get_graph(&id).unwrap().status, GraphStatus::Running);
        assert_eq!(store.list_graphs().len(), 1);
        assert!(store.update_status(&GraphId::new(), GraphStatus::Running).is_err());
    }

    #[test]
    fn idempotency_key_expiration_and_default_store() {
        let key = IdempotencyKey::new("key".to_string(), 1);
        assert!(key.is_valid());
        let expired = IdempotencyKey::new("expired".to_string(), -1);
        assert!(!expired.is_valid());
        let store = GraphStore::default();
        assert!(store.list_graphs().is_empty());
    }

    #[tokio::test]
    async fn distributed_executor_runs_and_reports_status() {
        let store = Arc::new(GraphStore::new());
        let executor = DistributedGraphExecutor::new(store.clone());
        let id = executor.submit(graph(), Some("request-1".to_string())).unwrap();
        assert_eq!(executor.submit(graph(), Some("request-1".to_string())).unwrap(), id);
        let result = executor.execute(&id).await.unwrap();
        assert!(result.is_success());
        assert_eq!(executor.status(&id).unwrap(), GraphStatus::Completed);
        assert_eq!(result.metrics.stage_times_ms.len(), 4);
    }

    #[test]
    fn distributed_executor_cancel_and_missing_errors() {
        let store = Arc::new(GraphStore::new());
        let executor = DistributedGraphExecutor::new(store);
        let id = executor.submit(graph(), None).unwrap();
        executor.cancel(&id).unwrap();
        assert_eq!(executor.status(&id).unwrap(), GraphStatus::Cancelled);
        assert!(executor.status(&GraphId::new()).is_err());
    }
}