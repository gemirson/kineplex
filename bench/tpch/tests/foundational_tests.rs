use kineplex_bench::config::*;
use kineplex_bench::datagen::TpchDataGenerator;
use kineplex_bench::metrics::*;
use kineplex_bench::perf_events::PerfEventCollector;

#[test]
fn test_config_parsing_and_defaults() {
    let cfg = BenchmarkRunConfig::default();
    assert_eq!(cfg.scale_factor, 1);
    assert_eq!(cfg.engine, BenchmarkEngine::KinePlex);
    assert_eq!(cfg.queries.len(), 22);

    let parsed_engine: BenchmarkEngine = "duckdb".parse().unwrap();
    assert_eq!(parsed_engine, BenchmarkEngine::DuckDb);
}

#[test]
fn test_metrics_statistical_calculations() {
    let raw_times = vec![12.0, 10.5, 11.2, 10.8, 14.0];
    let stats = calculate_statistics(&raw_times);

    assert_eq!(stats.min_ms, 10.5);
    assert_eq!(stats.max_ms, 14.0);
    assert_eq!(stats.median_ms, 11.2);
    assert!(stats.std_dev_ms > 0.0);

    let throughput = calculate_throughput_gb_s(1_000_000_000, 100.0); // 1 GB in 100 ms -> 10 GB/s
    assert!((throughput - 10.0).abs() < 1e-4);

    let geomean = compute_geometric_mean(&[2.0, 8.0]);
    assert!((geomean - 4.0).abs() < 1e-4);
}

#[test]
fn test_perf_events_collector() {
    let mut collector = PerfEventCollector::new(true);
    collector.start();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let (elapsed, telemetry) = collector.stop();

    assert!(elapsed >= 8.0);
    assert!(telemetry.is_some());
    let t = telemetry.unwrap();
    assert!(t.total_cycles > 0);
    assert!(t.instructions_per_cycle > 0.0);
}

#[test]
fn test_datagen_generation_parquet() {
    let dir = std::env::temp_dir().join(format!("tpch_test_{}", uuid::Uuid::new_v4()));
    let out_dir = dir.to_str().unwrap();

    let gen = TpchDataGenerator::new(1, out_dir, true);
    let nation = gen.generate_nation().unwrap();
    assert_eq!(nation.num_rows(), 25);

    let region = gen.generate_region().unwrap();
    assert_eq!(region.num_rows(), 5);

    let lineitem = gen.generate_lineitem().unwrap();
    assert!(lineitem.num_rows() > 0);

    let lineitem_file = dir.join("lineitem.parquet");
    assert!(lineitem_file.exists());

    let _ = std::fs::remove_dir_all(&dir);
}
