/// Operador físico de junção selecionado pelo CBO Riemanniano
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalJoinOperator {
    /// \kappa(x, y) > 0.15: Vizinhanças superpostas em L^p -> Merge local nas células de Laguerre
    LaguerreCoLocatedMergeJoin,
    /// |\kappa(x, y)| <= 0.15: Espaço plano euclidiano -> Hash Join vetorizado em AVX-512
    SimdPartitionedHashJoin,
    /// \kappa(x, y) < -0.15: Gargalo hiperbólico (Heavy Skew) -> Separa hubs via Broadcast + Quotient Coalesce
    HyperbolicSkewBroadcastJoin,
}

/// Estatísticas de uma aresta do grafo ponderado de junção
#[derive(Clone, Debug, PartialEq)]
pub struct GraphEdgeStatistics {
    pub metric_distance_dg: f64,     // Distância entre os baricentros d_G(x, y) no índice .khvp
    pub wasserstein_w1: f64,         // Distância de Transporte Ótimo W_1(m_x, m_y) entre as partições
    pub left_cardinality: u64,
    pub right_cardinality: u64,
    pub is_laguerre_aligned: bool,   // Se ambas as tabelas já estão co-particionadas por MK-EMD
}

impl GraphEdgeStatistics {
    pub fn new(
        metric_distance_dg: f64,
        wasserstein_w1: f64,
        left_cardinality: u64,
        right_cardinality: u64,
        is_laguerre_aligned: bool,
    ) -> Self {
        Self {
            metric_distance_dg,
            wasserstein_w1,
            left_cardinality,
            right_cardinality,
            is_laguerre_aligned,
        }
    }

    /// Calcula a Curvatura de Ollivier-Ricci: \kappa(x, y) = 1 - W_1(m_x, m_y) / d_G(x, y)
    #[inline(always)]
    pub fn ollivier_ricci_curvature(&self) -> f64 {
        let denom = self.metric_distance_dg.max(1e-9);
        1.0 - (self.wasserstein_w1 / denom)
    }
}
