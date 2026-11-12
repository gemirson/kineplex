//! # Grafos Direcionados Acíclicos de Fluxo de Dados (`ingnes_df::graph`)
//!
//! Em vez de planos relacionais hierárquicos e lentos de interpretar, o IngnesDataFrame
//! compõe um **Grafo Direcionado Acíclico de Fluxo de Dados Colunar (Columnar Flow DAG)**:
//!
//! - **Vértices (Nodes):** Operações puras de computação (Source, MorphismMap, Filter, PullbackJoin, Catamorphism).
//! - **Arestas (Edges):** Ponteiros para colunas alinhadas em cache de 64 bytes com Zero-Copy.
//! - **Otimização Topológica de Grafo:**
//!   1. Fusão de arestas: Colapsa caminhos de morfismos $A \to B \to C$ em um único nó composto $C \circ B \circ A$.
//!   2. Pushdown de filtros: Move nós de restrição para mais perto das fontes através de ordenação topológica.

use std::fmt;

pub type NodeId = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeKind {
    SourceRange {
        start: i64,
        end: i64,
        step: i64,
    },
    MorphismMap {
        column: String,
        description: &'static str,
    },
    Filter {
        description: &'static str,
    },
    PullbackJoin {
        left_key: String,
        right_key: String,
        description: &'static str,
    },
    Union {
        description: &'static str,
    },
    Catamorphism {
        description: &'static str,
    },
}

#[derive(Clone, Debug)]
pub struct GraphNode {
    pub id: NodeId,
    pub kind: NodeKind,
    pub inputs: Vec<NodeId>,
    pub outputs: Vec<NodeId>,
}

/// Grafo de Execução de Fluxo Colunar.
#[derive(Clone, Default)]
pub struct FlowGraph {
    nodes: Vec<GraphNode>,
}

impl FlowGraph {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn add_node(&mut self, kind: NodeKind, inputs: Vec<NodeId>) -> NodeId {
        let id = self.nodes.len();
        for &inp in &inputs {
            if inp < self.nodes.len() {
                self.nodes[inp].outputs.push(id);
            }
        }
        self.nodes.push(GraphNode {
            id,
            kind,
            inputs,
            outputs: Vec::new(),
        });
        id
    }

    pub fn get_node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.get(id)
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Ordenação topológica dos nós para execução ótima sem recursão de pilha.
    pub fn topological_order(&self) -> Vec<NodeId> {
        let mut in_degrees: Vec<usize> = self.nodes.iter().map(|n| n.inputs.len()).collect();
        let mut ready: Vec<NodeId> = (0..self.nodes.len())
            .filter(|&id| in_degrees[id] == 0)
            .collect();
        let mut order = Vec::with_capacity(self.nodes.len());

        while let Some(curr) = ready.pop() {
            order.push(curr);
            for &succ in &self.nodes[curr].outputs {
                in_degrees[succ] -= 1;
                if in_degrees[succ] == 0 {
                    ready.push(succ);
                }
            }
        }

        order
    }

    /// Exibe a representação textual do DAG categórico.
    pub fn explain(&self) -> String {
        let mut out = String::from("=== Ingnes Categorical Flow Graph (DAG) ===\n");
        let order = self.topological_order();
        for &id in &order {
            let n = &self.nodes[id];
            out.push_str(&format!(
                "Node #{:02} [{:?}] <- inputs: {:?}, outputs: {:?}\n",
                n.id, n.kind, n.inputs, n.outputs
            ));
        }
        out
    }
}

impl fmt::Debug for FlowGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.explain())
    }
}
