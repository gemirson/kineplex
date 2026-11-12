use kineplex_bench::comparative::ComparativeBenchmarkEngine;
use kineplex_bench::config::{BenchmarkQueryType, BenchmarkRunConfig};
use kineplex_bench::ComparativeBenchmarkRunner;

#[test]
fn test_comparative_runner_execution_and_speedups() {
    let tmp_dir = std::env::temp_dir().join(format!("tpch_comp_test_{}", uuid::Uuid::new_v4()));
    let config = BenchmarkRunConfig {
        scale_factor: 1,
        queries: vec![BenchmarkQueryType::Tpch(1), BenchmarkQueryType::Tpch(6)],
        warmup_runs: 1,
        measurement_runs: 2,
        data_directory: tmp_dir.to_str().unwrap().to_string(),
        output_directory: tmp_dir.to_str().unwrap().to_string(),
        enable_hardware_telemetry: false,
        ..Default::default()
    };

    let engine = ComparativeBenchmarkEngine::new();
    let summary = engine.run_comparative_suite(&config).unwrap();

    assert_eq!(summary.query_results.len(), 2);
    assert!(summary.geomean_kineplex_ms > 0.0);
    assert!(summary.overall_speedup_vs_duckdb >= 1.0);
    assert!(summary.overall_speedup_vs_spark >= 1.0);

    let json_file = tmp_dir.join("summary.json");
    engine.export_results_json(&summary, json_file.to_str().unwrap()).unwrap();
    assert!(json_file.exists());

    let _ = std::fs::remove_dir_all(&tmp_dir);
}
