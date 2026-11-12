use kineplex_core::cbo::{GraphEdgeStatistics, KinePlexCascadesOptimizer, PhysicalJoinOperator};
use kineplex_core::kinetxt::driver::{KineTxtDriver, TxtColumnSpec};
use kineplex_core::laguerre::config::KantorovichDistributionConfig;
use kineplex_core::laguerre::distributor::LaguerreOptimalDistributor;
use kineplex_core::lakehouse::{
    can_commute_concurrent_commit, IcebergManifestLattice, UnifiedDataFileAction,
};
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::test]
async fn test_e2e_cbo_parser_lakehouse_pipeline() {
    // 1. Parsing com KineTxt Driver acelerado por bitmasks
    let raw_csv = b"\
1001;\"DESCRICAO_A;INTERNA\";150.25
1002;\"DESCRICAO_B;NORMAL\";250.50
1003;\"DESCRICAO_C;OUTRA\";350.75
";

    let specs = vec![
        TxtColumnSpec::Int64 {
            name: "id".to_string(),
            col_index: 0,
            fixed_range: None,
        },
        TxtColumnSpec::Utf8 {
            name: "descricao".to_string(),
            col_index: 1,
            fixed_range: None,
        },
        TxtColumnSpec::Float64 {
            name: "valor".to_string(),
            col_index: 2,
            fixed_range: None,
            scale: 1.0,
        },
    ];

    let driver = KineTxtDriver::new(b';', specs);
    let batch = driver.parse_delimited_chunk_to_arrow(raw_csv).unwrap();
    assert_eq!(batch.num_rows(), 3);

    // 2. Planejamento CBO com Curvatura de Ollivier-Ricci
    let optimizer = KinePlexCascadesOptimizer::default();
    let stats = GraphEdgeStatistics::new(10.0, 1.0, batch.num_rows() as u64, 1000, true);
    let (chosen_op, cost) = optimizer.select_optimal_join_plan(&stats);
    assert_eq!(chosen_op, PhysicalJoinOperator::LaguerreCoLocatedMergeJoin);
    assert!(cost > 0.0);

    // 3. Distribuição ótima e escrita Parquet no espaço de Banach
    let store = Arc::new(InMemory::new());
    let seeds = vec![[0.0, 0.0, 0.0, 0.0]];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "bucket-lake".to_string(),
        "delta-output".to_string(),
        seeds,
        KantorovichDistributionConfig::default(),
    );

    distributor
        .route_and_distribute(batch, [0.1, 0.2, 0.3, 0.4])
        .await
        .unwrap();
    let sealed = distributor.finalize_optimal_distribution().await.unwrap();
    assert_eq!(sealed.len(), 1);

    // 4. Registro comutativo no Monoide Unificado Lakehouse (Delta Lake + Iceberg)
    let action = UnifiedDataFileAction::new(
        sealed[0].0.clone(),
        1024, // file size in bytes
        3,    // record count
        vec![1001.0, 150.25],
        vec![1003.0, 350.75],
        vec![0, 0],
        1700000000,
    );

    // Valida Delta Lake JSON
    let delta_json = action.to_delta_log_json_line();
    assert!(delta_json.contains(r#""dataChange":true"#));

    // Valida Iceberg Manifest Lattice
    let (mins, maxs, _) =
        IcebergManifestLattice::compute_manifest_bounds(std::slice::from_ref(&action));
    assert_eq!(mins, vec![1001.0, 150.25]);
    assert_eq!(maxs, vec![1003.0, 350.75]);

    // Valida comutatividade SSI contra transação concorrente externa
    let ext_read_min = vec![2000.0, 500.0];
    let ext_read_max = vec![3000.0, 800.0];
    assert!(can_commute_concurrent_commit(
        &[action],
        &ext_read_min,
        &ext_read_max
    ));
}
