use kineplex_bench::config::BenchmarkQueryType;
use kineplex_bench::data_model::{BenchmarkSuiteSummary, ComparativeQuerySummary};
use kineplex_bench::whitepaper::WhitepaperPublisherEngine;
use kineplex_bench::WhitepaperPublisher;

#[test]
fn test_whitepaper_compilation() {
    let tmp_dir = std::env::temp_dir().join(format!("tpch_wp_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir).unwrap();

    let summary = BenchmarkSuiteSummary {
        timestamp: "2026-11-11T20:00:00Z".to_string(),
        scale_factor: 1000,
        hardware_cpu: "AMD EPYC 7763".to_string(),
        total_cores: 64,
        total_ram_gb: 128.0,
        query_results: vec![ComparativeQuerySummary {
            query: BenchmarkQueryType::Tpch(1),
            kineplex_time_ms: 12.5,
            duckdb_time_ms: 27.0,
            spark_time_ms: 68.0,
            speedup_vs_duckdb: 2.16,
            speedup_vs_spark: 5.44,
            kineplex_peak_rss_mb: 450.0,
            duckdb_peak_rss_mb: 850.0,
            spark_peak_rss_mb: 2048.0,
        }],
        geomean_kineplex_ms: 12.5,
        geomean_duckdb_ms: 27.0,
        geomean_spark_ms: 68.0,
        overall_speedup_vs_duckdb: 2.16,
        overall_speedup_vs_spark: 5.44,
    };

    let out_file = tmp_dir.join("whitepaper_test.md");
    let publisher = WhitepaperPublisherEngine::new();
    publisher
        .compile_whitepaper(&summary, None, out_file.to_str().unwrap())
        .unwrap();

    assert!(out_file.exists());
    let content = std::fs::read_to_string(&out_file).unwrap();
    assert!(content.contains("Demonstrando a Superioridade Analítica do KinePlex"));
    assert!(content.contains("2.16x mais rápida que o DuckDB"));
    assert!(content.contains("5.44x mais rápida que o Apache Spark"));

    let _ = std::fs::remove_dir_all(&tmp_dir);
}
