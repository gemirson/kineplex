//! Declarative client-side builder for KinePlex DAG submissions.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

/// Native operator descriptions understood by the current execution API.
#[derive(Clone, Debug, PartialEq)]
pub enum NativeOperator {
    FilterGreaterThanF32 { column: String, threshold: f32 },
}

/// Client-declared topological constraints checked before submission.
#[derive(Clone, Debug, PartialEq)]
pub enum GraphInvariant {
    PreserveOrder,
    MaxDeformation { tolerance: f64 },
    MaxRouteResistance { maximum: f64 },
}

impl GraphInvariant {
    fn to_json(&self) -> Value {
        match self {
            Self::PreserveOrder => json!({ "kind": "preserve_order" }),
            Self::MaxDeformation { tolerance } => {
                json!({ "kind": "max_deformation", "tolerance": tolerance })
            }
            Self::MaxRouteResistance { maximum } => {
                json!({ "kind": "max_route_resistance", "maximum": maximum })
            }
        }
    }
}

/// Fluent DAG builder. Pipeline methods link each new stage to the prior stage.
#[derive(Debug, Default)]
pub struct KineGraph {
    nodes: Vec<Value>,
    edges: Vec<Value>,
    previous: Option<String>,
    next_id: usize,
    invariants: Vec<GraphInvariant>,
}

impl KineGraph {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn receptor(mut self, source: impl Into<String>) -> Self {
        self.push_node("receptor", json!({ "source": source.into() }))
    }

    pub fn synapse_wasm(mut self, module: impl Into<String>) -> Self {
        self.push_node("wasm", json!({ "wasm": module.into() }))
    }

    pub fn synapse_native(mut self, operator: NativeOperator) -> Self {
        let operator = match operator {
            NativeOperator::FilterGreaterThanF32 { column, threshold } => json!({
                "operator": "filter_gt_f32",
                "column": column,
                "threshold": threshold,
            }),
        };
        self.push_node("native", operator)
    }

    pub fn terminal(mut self, destination: impl Into<String>) -> Self {
        self.push_node("terminal", json!({ "destination": destination.into() }))
    }

    /// Adds an explicit edge between existing stage IDs.
    pub fn synapse(mut self, source: impl Into<String>, target: impl Into<String>) -> Self {
        self.edges
            .push(json!({ "source": source.into(), "target": target.into() }));
        self
    }

    /// Adds a topological invariant that must hold before the payload is emitted.
    pub fn invariant(mut self, invariant: GraphInvariant) -> Self {
        self.invariants.push(invariant);
        self
    }

    /// Validates the topology and returns the standard JSON graph payload.
    pub fn build(self) -> Result<GraphPayload, GraphBuildError> {
        validate_dag(&self.nodes, &self.edges)?;
        validate_invariants(&self.nodes, &self.edges, &self.invariants)?;
        Ok(GraphPayload {
            nodes: self.nodes,
            edges: self.edges,
            invariants: self
                .invariants
                .iter()
                .map(GraphInvariant::to_json)
                .collect(),
        })
    }

    fn push_node(&mut self, kind: &str, attributes: Value) -> Self {
        let id = format!("stage-{}", self.next_id);
        self.next_id += 1;
        if let Some(previous) = self.previous.replace(id.clone()) {
            self.edges.push(json!({ "source": previous, "target": id }));
        }
        let mut node = attributes.as_object().cloned().unwrap_or_default();
        node.insert("id".to_owned(), Value::String(id));
        node.insert("kind".to_owned(), Value::String(kind.to_owned()));
        self.nodes.push(Value::Object(node));
        self.clone_self()
    }

    fn clone_self(&mut self) -> Self {
        Self {
            nodes: std::mem::take(&mut self.nodes),
            edges: std::mem::take(&mut self.edges),
            previous: self.previous.take(),
            next_id: std::mem::take(&mut self.next_id),
            invariants: std::mem::take(&mut self.invariants),
        }
    }
}

/// Serialized graph accepted by the KinePlex control plane.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphPayload {
    pub nodes: Vec<Value>,
    pub edges: Vec<Value>,
    pub invariants: Vec<Value>,
}

impl GraphPayload {
    pub fn to_json(&self) -> Value {
        json!({ "nodes": self.nodes, "edges": self.edges, "invariants": self.invariants })
    }

    pub fn into_json(self) -> Value {
        json!({ "nodes": self.nodes, "edges": self.edges, "invariants": self.invariants })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphBuildError {
    InvalidNode(usize),
    UnknownEndpoint(String),
    Cycle,
    OrphanNode(String),
    InvalidInvariant(String),
}

impl std::fmt::Display for GraphBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidNode(index) => write!(f, "node at index {index} has no ID"),
            Self::UnknownEndpoint(id) => write!(f, "edge references unknown stage {id:?}"),
            Self::Cycle => f.write_str("graph contains a directed cycle"),
            Self::OrphanNode(id) => write!(f, "stage {id:?} is disconnected from the graph"),
            Self::InvalidInvariant(reason) => write!(f, "graph invariant failed: {reason}"),
        }
    }
}

fn validate_invariants(
    nodes: &[Value],
    edges: &[Value],
    invariants: &[GraphInvariant],
) -> Result<(), GraphBuildError> {
    for invariant in invariants {
        match invariant {
            GraphInvariant::PreserveOrder => {
                let expected = nodes.windows(2).all(|pair| {
                    let source = pair[0].get("id").and_then(Value::as_str);
                    let target = pair[1].get("id").and_then(Value::as_str);
                    edges.iter().any(|edge| {
                        edge["source"].as_str() == source && edge["target"].as_str() == target
                    })
                });
                if !expected {
                    return Err(GraphBuildError::InvalidInvariant(
                        "preserve_order requires an edge between each adjacent stage".to_owned(),
                    ));
                }
            }
            GraphInvariant::MaxDeformation { tolerance }
                if !tolerance.is_finite() || *tolerance < 0.0 =>
            {
                return Err(GraphBuildError::InvalidInvariant(
                    "max_deformation must be a finite non-negative value".to_owned(),
                ));
            }
            GraphInvariant::MaxRouteResistance { maximum }
                if !maximum.is_finite() || *maximum < 0.0 =>
            {
                return Err(GraphBuildError::InvalidInvariant(
                    "max_route_resistance must be a finite non-negative value".to_owned(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

impl std::error::Error for GraphBuildError {}

fn validate_dag(nodes: &[Value], edges: &[Value]) -> Result<(), GraphBuildError> {
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut ids = HashSet::new();
    for (index, node) in nodes.iter().enumerate() {
        let id = node
            .get("id")
            .and_then(Value::as_str)
            .ok_or(GraphBuildError::InvalidNode(index))?;
        ids.insert(id);
        adjacency.entry(id).or_default();
    }
    for edge in edges {
        let source = edge
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| GraphBuildError::UnknownEndpoint("<missing source>".to_owned()))?;
        let target = edge
            .get("target")
            .and_then(Value::as_str)
            .ok_or_else(|| GraphBuildError::UnknownEndpoint("<missing target>".to_owned()))?;
        if !ids.contains(source) {
            return Err(GraphBuildError::UnknownEndpoint(source.to_owned()));
        }
        if !ids.contains(target) {
            return Err(GraphBuildError::UnknownEndpoint(target.to_owned()));
        }
        adjacency.entry(source).or_default().push(target);
    }
    let mut states: HashMap<&str, u8> = HashMap::new();
    for id in ids.iter().copied() {
        if visit(id, &adjacency, &mut states) {
            return Err(GraphBuildError::Cycle);
        }
    }
    if nodes.len() > 1 {
        for id in ids {
            let has_in = edges.iter().any(|edge| edge["target"] == id);
            let has_out = adjacency.get(id).is_some_and(|next| !next.is_empty());
            if !has_in && !has_out {
                return Err(GraphBuildError::OrphanNode(id.to_owned()));
            }
        }
    }
    Ok(())
}

fn visit<'a>(
    id: &'a str,
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
    states: &mut HashMap<&'a str, u8>,
) -> bool {
    match states.get(id) {
        Some(1) => return true,
        Some(2) => return false,
        _ => {}
    }
    states.insert(id, 1);
    if adjacency
        .get(id)
        .is_some_and(|children| children.iter().any(|child| visit(child, adjacency, states)))
    {
        return true;
    }
    states.insert(id, 2);
    false
}

#[cfg(test)]
mod tests {
    use super::{GraphBuildError, GraphInvariant, KineGraph, NativeOperator};

    #[test]
    fn fluent_pipeline_links_stages_and_builds_json() {
        let payload = KineGraph::new()
            .receptor("s3://bucket/input.parquet")
            .synapse_wasm("filter.wasm")
            .synapse_native(NativeOperator::FilterGreaterThanF32 {
                column: "score".into(),
                threshold: 10.0,
            })
            .invariant(GraphInvariant::PreserveOrder)
            .invariant(GraphInvariant::MaxDeformation { tolerance: 0.1 })
            .terminal("s3://bucket/output/")
            .build()
            .expect("linear graph is acyclic");
        assert_eq!(payload.nodes.len(), 4);
        assert_eq!(payload.edges.len(), 3);
        assert_eq!(payload.invariants.len(), 2);
        assert_eq!(payload.to_json()["nodes"][1]["wasm"], "filter.wasm");
    }

    #[test]
    fn explicit_cycle_is_rejected() {
        let error = KineGraph::new()
            .receptor("input")
            .synapse("stage-0", "stage-0")
            .build()
            .expect_err("self-edge cycles");
        assert_eq!(error, GraphBuildError::Cycle);
    }

    #[test]
    fn invalid_numeric_invariant_is_rejected_before_submission() {
        let error = KineGraph::new()
            .receptor("input")
            .terminal("output")
            .invariant(GraphInvariant::MaxRouteResistance { maximum: f64::NAN })
            .build()
            .expect_err("NaN is not a valid invariant threshold");
        assert!(matches!(error, GraphBuildError::InvalidInvariant(_)));
    }
}
