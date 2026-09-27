//! Parsing and validation for the live KinePlex DAG.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::Direction;
use serde_json::{Map, Value};

/// A parsed graph node identified by a stable client-supplied string.
#[derive(Clone, Debug, PartialEq)]
pub struct KineNode {
    /// Stable identifier referenced by graph edges.
    pub id: String,
    /// Original node attributes, preserved for later execution validation.
    pub payload: Value,
}

/// A parsed directed graph edge.
#[derive(Clone, Debug, PartialEq)]
pub struct KineEdge {
    /// Source node identifier.
    pub source: String,
    /// Destination node identifier.
    pub target: String,
    /// Original edge attributes, preserved for later execution validation.
    pub payload: Value,
}

/// Validated directed acyclic execution graph.
///
/// The underlying `petgraph` representation is private so callers consume a
/// validated graph rather than mutating topology after validation.
#[derive(Debug)]
pub struct KineGraph {
    graph: DiGraph<KineNode, KineEdge>,
    indices: HashMap<String, NodeIndex>,
}

impl KineGraph {
    /// Parses JSON node and edge arrays and validates their topology.
    ///
    /// Nodes must be objects containing a non-empty string `id`. Edges must be
    /// objects containing non-empty string `source` and `target` identifiers.
    /// Extra attributes are preserved in each internal payload.
    ///
    /// # Errors
    ///
    /// Returns a specific parse or topology error for malformed records,
    /// duplicate IDs, unknown endpoints, orphan nodes, missing terminals, or cycles.
    pub fn from_json(nodes: &[Value], edges: &[Value]) -> Result<Self, GraphValidationError> {
        let mut graph = DiGraph::new();
        let mut indices = HashMap::with_capacity(nodes.len());

        for (position, value) in nodes.iter().enumerate() {
            let object = value
                .as_object()
                .ok_or_else(|| GraphValidationError::InvalidNode {
                    position,
                    reason: "node must be a JSON object".to_owned(),
                })?;
            let id = required_string(object, "id")
                .map_err(|reason| GraphValidationError::InvalidNode { position, reason })?;
            if indices.contains_key(&id) {
                return Err(GraphValidationError::DuplicateNodeId { id });
            }

            let index = graph.add_node(KineNode {
                id: id.clone(),
                payload: Value::Object(object.clone()),
            });
            indices.insert(id, index);
        }

        for (position, value) in edges.iter().enumerate() {
            let object = value
                .as_object()
                .ok_or_else(|| GraphValidationError::InvalidEdge {
                    position,
                    reason: "edge must be a JSON object".to_owned(),
                })?;
            let source = required_string(object, "source")
                .map_err(|reason| GraphValidationError::InvalidEdge { position, reason })?;
            let target = required_string(object, "target")
                .map_err(|reason| GraphValidationError::InvalidEdge { position, reason })?;
            let source_index = indices.get(&source).copied().ok_or_else(|| {
                GraphValidationError::UnknownEndpoint {
                    position,
                    endpoint: source.clone(),
                }
            })?;
            let target_index = indices.get(&target).copied().ok_or_else(|| {
                GraphValidationError::UnknownEndpoint {
                    position,
                    endpoint: target.clone(),
                }
            })?;
            graph.add_edge(
                source_index,
                target_index,
                KineEdge {
                    source,
                    target,
                    payload: Value::Object(object.clone()),
                },
            );
        }

        let parsed = Self { graph, indices };
        parsed.validate_topology()?;
        Ok(parsed)
    }

    /// Returns the number of validated nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Returns the number of validated directed edges, including parallel edges.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Returns the stable identifiers in their submitted order.
    #[must_use]
    pub fn stage_ids(&self) -> Vec<String> {
        self.graph
            .node_indices()
            .map(|index| self.graph[index].id.clone())
            .collect()
    }

    /// Returns the original JSON payload for one stage.
    #[must_use]
    pub fn node_payload(&self, id: &str) -> Option<&Value> {
        self.node_index(id).map(|index| &self.graph[index].payload)
    }

    /// Returns a node index for a stable identifier.
    #[must_use]
    pub fn node_index(&self, id: &str) -> Option<NodeIndex> {
        self.indices.get(id).copied()
    }

    /// Returns the validated petgraph view for read-only graph algorithms.
    #[must_use]
    pub fn graph(&self) -> &DiGraph<KineNode, KineEdge> {
        &self.graph
    }

    fn validate_topology(&self) -> Result<(), GraphValidationError> {
        if self.graph.node_count() > 1 {
            for index in self.graph.node_indices() {
                let has_incoming = self
                    .graph
                    .neighbors_directed(index, Direction::Incoming)
                    .next()
                    .is_some();
                let has_outgoing = self
                    .graph
                    .neighbors_directed(index, Direction::Outgoing)
                    .next()
                    .is_some();
                if !has_incoming && !has_outgoing {
                    return Err(GraphValidationError::OrphanNodeDetected {
                        id: self.graph[index].id.clone(),
                    });
                }
            }
        }

        if let Some(index) = self.find_cycle()? {
            return Err(GraphValidationError::CycleDetected {
                id: self.graph[index].id.clone(),
            });
        }

        let has_source = self.graph.node_indices().any(|index| {
            self.graph
                .neighbors_directed(index, Direction::Incoming)
                .next()
                .is_none()
        });
        if !has_source {
            return Err(GraphValidationError::MissingSourceNode);
        }

        let has_terminal = self.graph.node_indices().any(|index| {
            self.graph
                .neighbors_directed(index, Direction::Outgoing)
                .next()
                .is_none()
        });
        if !has_terminal {
            return Err(GraphValidationError::MissingTerminalNode);
        }

        Ok(())
    }

    fn find_cycle(&self) -> Result<Option<NodeIndex>, GraphValidationError> {
        let mut states = HashMap::with_capacity(self.graph.node_count());
        for index in self.graph.node_indices() {
            if !states.contains_key(&index) && Self::visit(self, index, &mut states)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn visit(
        &self,
        index: NodeIndex,
        states: &mut HashMap<NodeIndex, VisitState>,
    ) -> Result<bool, GraphValidationError> {
        states.insert(index, VisitState::Visiting);
        for next in self.graph.neighbors_directed(index, Direction::Outgoing) {
            match states.get(&next) {
                Some(VisitState::Visiting) => return Ok(true),
                Some(VisitState::Visited) => {}
                None => {
                    if self.visit(next, states)? {
                        return Ok(true);
                    }
                }
            }
        }
        states.insert(index, VisitState::Visited);
        Ok(false)
    }
}

#[derive(Clone, Copy)]
enum VisitState {
    Visiting,
    Visited,
}

fn required_string(object: &Map<String, Value>, field: &str) -> Result<String, String> {
    match object.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(_) => Err(format!("{field} must not be empty")),
        None => Err(format!("{field} must be a string")),
    }
}

/// Specific parsing or topology validation failure for a submitted graph.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphValidationError {
    /// A node record is not structurally valid.
    InvalidNode { position: usize, reason: String },
    /// An edge record is not structurally valid.
    InvalidEdge { position: usize, reason: String },
    /// Two nodes share the same stable identifier.
    DuplicateNodeId { id: String },
    /// An edge references a node that was not declared.
    UnknownEndpoint { position: usize, endpoint: String },
    /// A multi-node graph contains a disconnected node.
    OrphanNodeDetected { id: String },
    /// The DFS found a directed cycle.
    CycleDetected { id: String },
    /// Every node has an incoming edge.
    MissingSourceNode,
    /// Every node has an outgoing edge.
    MissingTerminalNode,
}

impl Display for GraphValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNode { position, reason } => {
                write!(formatter, "invalid node at index {position}: {reason}")
            }
            Self::InvalidEdge { position, reason } => {
                write!(formatter, "invalid edge at index {position}: {reason}")
            }
            Self::DuplicateNodeId { id } => write!(formatter, "duplicate node id '{id}'"),
            Self::UnknownEndpoint { position, endpoint } => write!(
                formatter,
                "edge at index {position} references unknown node '{endpoint}'"
            ),
            Self::OrphanNodeDetected { id } => {
                write!(formatter, "orphan node detected: '{id}' has no connections")
            }
            Self::CycleDetected { id } => write!(formatter, "cycle detected near node '{id}'"),
            Self::MissingSourceNode => {
                formatter.write_str("graph must contain at least one source node")
            }
            Self::MissingTerminalNode => {
                formatter.write_str("graph must contain at least one terminal node")
            }
        }
    }
}

impl Error for GraphValidationError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{GraphValidationError, KineGraph};

    fn graph(nodes: &[&str], edges: &[(&str, &str)]) -> KineGraph {
        let nodes = nodes.iter().map(|id| json!({"id": id})).collect::<Vec<_>>();
        let edges = edges
            .iter()
            .map(|(source, target)| json!({"source": source, "target": target}))
            .collect::<Vec<_>>();
        KineGraph::from_json(&nodes, &edges).expect("fixture graph must be valid")
    }

    #[test]
    fn parses_linear_dag_and_preserves_attributes() {
        let nodes = vec![
            json!({"id":"A", "kind":"source"}),
            json!({"id":"B"}),
            json!({"id":"C"}),
        ];
        let edges = vec![
            json!({"source":"A", "target":"B", "weight": 2}),
            json!({"source":"B", "target":"C"}),
        ];
        let parsed = KineGraph::from_json(&nodes, &edges).expect("A -> B -> C is a DAG");

        assert_eq!(parsed.node_count(), 3);
        assert_eq!(parsed.edge_count(), 2);
        assert_eq!(
            parsed.graph()[parsed.node_index("A").expect("A exists")].payload["kind"],
            "source"
        );
        assert_eq!(
            parsed
                .graph()
                .edge_weights()
                .next()
                .expect("edge exists")
                .payload["weight"],
            2
        );
    }

    #[test]
    fn rejects_cycle_with_dfs() {
        let error = KineGraph::from_json(
            &[json!({"id":"A"}), json!({"id":"B"}), json!({"id":"C"})],
            &[
                json!({"source":"A","target":"B"}),
                json!({"source":"B","target":"C"}),
                json!({"source":"C","target":"A"}),
            ],
        )
        .expect_err("A -> B -> C -> A must be rejected");
        assert!(matches!(error, GraphValidationError::CycleDetected { .. }));
    }

    #[test]
    fn rejects_orphan_node() {
        let error = KineGraph::from_json(
            &[json!({"id":"A"}), json!({"id":"B"}), json!({"id":"C"})],
            &[json!({"source":"A","target":"B"})],
        )
        .expect_err("C is disconnected");
        assert_eq!(
            error,
            GraphValidationError::OrphanNodeDetected { id: "C".to_owned() }
        );
    }

    #[test]
    fn accepts_bifurcation_and_merge() {
        let parsed = graph(
            &["A", "B", "C", "D"],
            &[("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")],
        );
        assert_eq!(parsed.node_count(), 4);
        assert_eq!(parsed.edge_count(), 4);
    }

    #[test]
    fn rejects_duplicate_and_unknown_ids() {
        let duplicate = KineGraph::from_json(&[json!({"id":"A"}), json!({"id":"A"})], &[])
            .expect_err("duplicate IDs must fail");
        assert!(matches!(
            duplicate,
            GraphValidationError::DuplicateNodeId { .. }
        ));

        let unknown = KineGraph::from_json(
            &[json!({"id":"A"}), json!({"id":"B"})],
            &[json!({"source":"A","target":"C"})],
        )
        .expect_err("unknown endpoints must fail");
        assert!(matches!(
            unknown,
            GraphValidationError::UnknownEndpoint { .. }
        ));
    }

    #[test]
    fn accepts_single_node_as_source_and_terminal() {
        let parsed = graph(&["A"], &[]);
        assert_eq!(parsed.node_count(), 1);
    }
}


// ==========================================
// Graph Store & Idempotency (from develop)
// ==========================================

//  Graph execution engine
//  
//  This module provides the graph execution logic including:
//  - Graph state management
//  - Execution pipeline orchestration
//  - Idempotency checking

use crate::error::Result;
use crate::{CoreError, GraphId, Graph, GraphStatus, ExecutionResult, ExecutionMetrics};
use std::sync::Arc;
use parking_lot::RwLock;
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
mod graph_store_tests {
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