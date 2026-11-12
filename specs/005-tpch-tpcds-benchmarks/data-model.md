# Data Model: TPC-H / TPC-DS Benchmarks & Whitepaper (FT-101)

## 1. Entidades de Configuração e Execução

### `BenchmarkEngine` (Enum)
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BenchmarkEngine {
    KinePlex,
    DuckDb,
    ApacheSpark,
}
```

### `BenchmarkQueryType` (Enum)
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BenchmarkQueryType {
    Tpch(u8),        // Q1 a Q22
    Tpcds(u8),       // Amostras TPC-DS: Q01, Q03, Q14, Q64, Q72, Q95
}
```

### `HardwareTelemetrySnapshot` (Struct)
```rust
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
```

### `BenchmarkRunConfig` (Struct)
```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkRunConfig {
    pub scale_factor: u64,                      // SF=1, SF=100, SF=1000 (1TB) ou SF=10000 (10TB)
    pub engine: BenchmarkEngine,
    pub queries: Vec<BenchmarkQueryType>,       // Lista de queries
    pub warmup_runs: usize,                     // Default: 1
    pub measurement_runs: usize,                // Default: 5
    pub num_threads: usize,
    pub data_directory: String,
    pub output_directory: String,
    pub enable_hardware_telemetry: bool,
}
```

### `QueryExecutionMetric` (Struct)
```rust
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
```

### `ComparativeQuerySummary` (Struct)
```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComparativeQuerySummary {
    pub query: BenchmarkQueryType,
    pub kineplex_time_ms: f64,
    pub duckdb_time_ms: f64,
    pub spark_time_ms: f64,
    pub speedup_vs_duckdb: f64,      // duckdb_time / kineplex_time
    pub speedup_vs_spark: f64,       // spark_time / kineplex_time
    pub kineplex_peak_rss_mb: f64,
    pub duckdb_peak_rss_mb: f64,
    pub spark_peak_rss_mb: f64,
}
```

### `BenchmarkSuiteSummary` (Struct)
```rust
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
```

### `WhitepaperDocument` (Struct)
```rust
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
```
