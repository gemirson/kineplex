use crate::cbo::curvature::{GraphEdgeStatistics, PhysicalJoinOperator};

/// Otimizador Cascades Riemanniano orientado à Curvatura de Ollivier-Ricci
pub struct KinePlexCascadesOptimizer {
    pub cpu_cost_weight: f64,
    pub io_cost_weight: f64,
    pub net_enforcer_weight: f64,
}

impl Default for KinePlexCascadesOptimizer {
    fn default() -> Self {
        Self {
            cpu_cost_weight: 1.0,
            io_cost_weight: 1.5,
            net_enforcer_weight: 2.0,
        }
    }
}

impl KinePlexCascadesOptimizer {
    pub fn new(cpu_cost_weight: f64, io_cost_weight: f64, net_enforcer_weight: f64) -> Self {
        Self {
            cpu_cost_weight,
            io_cost_weight,
            net_enforcer_weight,
        }
    }

    /// Regra de Substituição Física guiada pela Curvatura de Ollivier-Ricci e Bellman-Ford Pruning
    pub fn select_optimal_join_plan(
        &self,
        stats: &GraphEdgeStatistics,
    ) -> (PhysicalJoinOperator, f64) {
        let kappa = stats.ollivier_ricci_curvature();
        let n_total = (stats.left_cardinality + stats.right_cardinality) as f64;

        // Fator de amplificação geodésica para arestas hiperbólicas (curvatura negativa = skew severo)
        let curvature_amplifier = (-kappa).exp().clamp(0.2, 20.0);

        // Custo Candidato 1: Co-Located Laguerre Merge Join (sem sobrecarga de hash table)
        let enforcer_cost = if stats.is_laguerre_aligned {
            0.0
        } else {
            self.net_enforcer_weight * n_total * curvature_amplifier
        };
        let cost_laguerre = self.cpu_cost_weight * n_total * 0.6 + enforcer_cost;

        // Custo Candidato 2: SIMD Partitioned Hash Join (sofre com colisão de hash se \kappa << 0)
        let cost_hash = self.cpu_cost_weight * n_total * 1.35 * curvature_amplifier
            + self.io_cost_weight * (stats.right_cardinality as f64) * 0.2;

        // Custo Candidato 3: Hyperbolic Skew Broadcast + Quotient Coalesce
        let smaller_side = stats.left_cardinality.min(stats.right_cardinality) as f64;
        let cost_hyperbolic = self.cpu_cost_weight * n_total * 0.9
            + self.net_enforcer_weight * smaller_side * 2.5;

        if kappa > 0.15 && cost_laguerre <= cost_hash {
            (PhysicalJoinOperator::LaguerreCoLocatedMergeJoin, cost_laguerre)
        } else if kappa < -0.15 && cost_hyperbolic < cost_hash {
            (PhysicalJoinOperator::HyperbolicSkewBroadcastJoin, cost_hyperbolic)
        } else {
            (PhysicalJoinOperator::SimdPartitionedHashJoin, cost_hash)
        }
    }

    /// Avalia se um ramo físico deve ser podado pela cota de Bellman-Ford
    #[inline(always)]
    pub fn should_prune_branch(&self, branch_lower_bound: f64, best_known_cost: f64) -> bool {
        branch_lower_bound >= best_known_cost
    }
}
