use crate::config::{BenchmarkEngine, BenchmarkQueryType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct HardwareTelemetrySnapshot {
    pub total_cycles: u64,
    pub instructions: u64,
    pub instructions_per_cycle: f64,
    pub l1_dcache_misses: u64,
    pub l1_miss_rate: f64,
    pub llc_misses: u64,
    pub branch_mispredictions: u64,
    pub branch_miss_rate: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryExecutionMetric {
    pub query: BenchmarkQueryType,
    pub engine: BenchmarkEngine,
    pub scale_factor: u64,
    pub raw_times_ms: Vec<f64>,
    pub median_time_ms: f64,
    pub std_dev_ms: f64,
    pub peak_rss_bytes: u64,
    pub total_rows_scanned: u64,
    pub throughput_gb_per_sec: f64,
    pub telemetry: Option<HardwareTelemetrySnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComparativeQuerySummary {
    pub query: BenchmarkQueryType,
    pub kineplex_time_ms: f64,
    pub duckdb_time_ms: f64,
    pub spark_time_ms: f64,
    pub speedup_vs_duckdb: f64,
    pub speedup_vs_spark: f64,
    pub kineplex_peak_rss_mb: f64,
    pub duckdb_peak_rss_mb: f64,
    pub spark_peak_rss_mb: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkSuiteSummary {
    pub timestamp: String,
    pub scale_factor: u64,
    pub hardware_cpu: String,
    pub total_cores: usize,
    pub total_ram_gb: f64,
    pub query_results: Vec<ComparativeQuerySummary>,
    pub geomean_kineplex_ms: f64,
    pub geomean_duckdb_ms: f64,
    pub geomean_spark_ms: f64,
    pub overall_speedup_vs_duckdb: f64,
    pub overall_speedup_vs_spark: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WhitepaperDocument {
    pub title: String,
    pub version: String,
    pub author: String,
    pub date: String,
    pub abstract_text: String,
    pub theoretical_foundations: Vec<String>,
    pub experimental_setup: String,
    pub benchmark_summary_1tb: BenchmarkSuiteSummary,
    pub benchmark_summary_10tb: Option<BenchmarkSuiteSummary>,
    pub microarchitectural_analysis: String,
    pub conclusion: String,
}
