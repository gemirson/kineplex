use kineplex_core::cbo::{
    CascadesCostOptimizer, GraphEdgeStatistics, KinePlexCascadesOptimizer, PhysicalJoinOperator,
};

#[test]
fn test_ollivier_ricci_curvature_computation() {
    // 1. Curvatura positiva: W_1 menor que d_G (vizinhanças sobrepostas / agrupamento compacto)
    let stats_pos = GraphEdgeStatistics::new(10.0, 3.0, 100_000, 100_000, true);
    let kappa_pos = stats_pos.ollivier_ricci_curvature();
    assert!((kappa_pos - 0.70).abs() < 1e-6);

    // 2. Curvatura nula: W_1 igual a d_G (geometria plana euclidiana)
    let stats_flat = GraphEdgeStatistics::new(10.0, 10.0, 50_000, 50_000, false);
    let kappa_flat = stats_flat.ollivier_ricci_curvature();
    assert!((kappa_flat - 0.0).abs() < 1e-6);

    // 3. Curvatura negativa: W_1 maior que d_G (gargalo hiperbólico / heavy skew)
    let stats_neg = GraphEdgeStatistics::new(5.0, 15.0, 1_000_000, 10_000, false);
    let kappa_neg = stats_neg.ollivier_ricci_curvature();
    assert!((kappa_neg - (-2.0)).abs() < 1e-6);

    // 4. Caso de borda: d_G = 0 não dispara divisão por zero
    let stats_zero = GraphEdgeStatistics::new(0.0, 5.0, 10_000, 10_000, false);
    let kappa_zero = stats_zero.ollivier_ricci_curvature();
    assert!(kappa_zero.is_finite());
}

#[test]
fn test_cbo_physical_join_selection_rules() {
    let optimizer = KinePlexCascadesOptimizer::default();

    // Caso 1: \kappa > 0.15 e alinhado por Laguerre -> LaguerreCoLocatedMergeJoin
    let stats_merge = GraphEdgeStatistics::new(10.0, 2.0, 100_000, 100_000, true);
    let (plan_merge, cost_merge) = optimizer.select_optimal_join_plan(&stats_merge);
    assert_eq!(plan_merge, PhysicalJoinOperator::LaguerreCoLocatedMergeJoin);
    assert!(cost_merge > 0.0);

    // Caso 2: |\kappa| <= 0.15 -> SimdPartitionedHashJoin
    let stats_hash = GraphEdgeStatistics::new(10.0, 9.5, 100_000, 100_000, false);
    let (plan_hash, cost_hash) = optimizer.select_optimal_join_plan(&stats_hash);
    assert_eq!(plan_hash, PhysicalJoinOperator::SimdPartitionedHashJoin);
    assert!(cost_hash > 0.0);

    // Caso 3: \kappa < -0.15 (heavy skew) -> HyperbolicSkewBroadcastJoin
    let stats_skew = GraphEdgeStatistics::new(5.0, 20.0, 10_000_000, 1_000, false);
    let (plan_skew, cost_skew) = optimizer.select_optimal_join_plan(&stats_skew);
    assert_eq!(plan_skew, PhysicalJoinOperator::HyperbolicSkewBroadcastJoin);
    assert!(cost_skew > 0.0);

    // Teste via trait
    let (plan_trait, _) = CascadesCostOptimizer::select_optimal_join_plan(&optimizer, &stats_merge);
    assert_eq!(plan_trait, PhysicalJoinOperator::LaguerreCoLocatedMergeJoin);
}

#[test]
fn test_bellman_ford_pruning() {
    let optimizer = KinePlexCascadesOptimizer::default();

    // Ramo com custo inferior ao melhor conhecido não deve ser podado
    assert!(!optimizer.should_prune_branch(500.0, 1000.0));

    // Ramo com custo igual ou superior deve ser podado imediatamente
    assert!(optimizer.should_prune_branch(1000.0, 1000.0));
    assert!(optimizer.should_prune_branch(1500.0, 1000.0));
}
