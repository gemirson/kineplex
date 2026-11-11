use kineplex_core::lakehouse::{
    can_commute_concurrent_commit, IcebergManifestLattice, LakehouseCommitEngine,
    UnifiedDataFileAction, UnifiedLakehouseEngine,
};

#[test]
fn test_delta_log_json_serialization() {
    let action = UnifiedDataFileAction::new(
        "part-00001.parquet".to_string(),
        134217728, // 128 MB
        1_000_000,
        vec![10.0, 100.0],
        vec![50.0, 500.0],
        vec![0, 12],
        1700000000,
    );

    let json_line = action.to_delta_log_json_line();
    assert!(json_line.contains(r#""path":"part-00001.parquet""#));
    assert!(json_line.contains(r#""size":134217728"#));
    assert!(json_line.contains(r#"\"numRecords\":1000000"#));

    // Valida que o JSON é parseável e a string de stats é JSON válido
    let parsed: serde_json::Value = serde_json::from_str(&json_line).unwrap();
    assert_eq!(parsed["add"]["path"], "part-00001.parquet");
    let stats_str = parsed["add"]["stats"].as_str().unwrap();
    let stats: serde_json::Value = serde_json::from_str(stats_str).unwrap();
    assert_eq!(stats["numRecords"], 1_000_000);
}

#[test]
fn test_ssi_commutativity_diamond_property() {
    let action1 = UnifiedDataFileAction::new(
        "file1.parquet".to_string(),
        128_000_000,
        100_000,
        vec![0.0, 0.0],
        vec![100.0, 50.0],
        vec![0, 0],
        1700000000,
    );

    // Caso 1: Consulta lê [200.0, 300.0] -> Disjunta! Deve comutar sem conflito (true)
    let read_min_disjoint = vec![200.0, 60.0];
    let read_max_disjoint = vec![300.0, 100.0];
    assert!(can_commute_concurrent_commit(
        std::slice::from_ref(&action1),
        &read_min_disjoint,
        &read_max_disjoint
    ));

    // Caso 2: Consulta lê [50.0, 150.0] -> Intersecta! Não comuta (false)
    let read_min_overlap = vec![50.0, 10.0];
    let read_max_overlap = vec![150.0, 30.0];
    assert!(!can_commute_concurrent_commit(
        &[action1],
        &read_min_overlap,
        &read_max_overlap
    ));
}

#[test]
fn test_iceberg_manifest_lattice_aggregation() {
    let action1 = UnifiedDataFileAction::new(
        "f1.parquet".to_string(),
        1000,
        100,
        vec![10.0, 50.0],
        vec![20.0, 70.0],
        vec![1, 2],
        1000,
    );
    let action2 = UnifiedDataFileAction::new(
        "f2.parquet".to_string(),
        2000,
        200,
        vec![5.0, 60.0],
        vec![15.0, 95.0],
        vec![3, 0],
        1001,
    );

    let (mins, maxs, nulls) =
        IcebergManifestLattice::compute_manifest_bounds(&[action1, action2]);

    assert_eq!(mins, vec![5.0, 50.0]); // Ínfimo
    assert_eq!(maxs, vec![20.0, 95.0]); // Supremo
    assert_eq!(nulls, vec![4, 2]); // Soma de nulos
}

#[test]
fn test_unified_lakehouse_engine_trait() {
    let engine = UnifiedLakehouseEngine;
    let action = UnifiedDataFileAction::new(
        "f1.parquet".to_string(),
        1000,
        100,
        vec![1.0],
        vec![10.0],
        vec![0],
        1000,
    );

    let lines = engine.serialize_delta_log(std::slice::from_ref(&action));
    assert_eq!(lines.len(), 1);

    let commutes = engine.validate_ssi_commutativity(&[action], &[20.0], &[30.0]);
    assert!(commutes);
}
