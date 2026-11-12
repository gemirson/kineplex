pub mod curvature;
pub mod memo;

pub use curvature::{GraphEdgeStatistics, PhysicalJoinOperator};
pub use memo::KinePlexCascadesOptimizer;

/// Contrato formal para otimizadores de custo Cascades
pub trait CascadesCostOptimizer: Send + Sync {
    fn select_optimal_join_plan(
        &self,
        stats: &GraphEdgeStatistics,
    ) -> (PhysicalJoinOperator, f64);
}

impl CascadesCostOptimizer for KinePlexCascadesOptimizer {
    fn select_optimal_join_plan(
        &self,
        stats: &GraphEdgeStatistics,
    ) -> (PhysicalJoinOperator, f64) {
        self.select_optimal_join_plan(stats)
    }
}
