use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BenchmarkEngine {
    KinePlex,
    DuckDb,
    ApacheSpark,
}

impl std::str::FromStr for BenchmarkEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "kineplex" => Ok(BenchmarkEngine::KinePlex),
            "duckdb" => Ok(BenchmarkEngine::DuckDb),
            "spark" | "apachespark" | "apache-spark" => Ok(BenchmarkEngine::ApacheSpark),
            other => Err(format!("Unknown benchmark engine: {other}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BenchmarkQueryType {
    Tpch(u8),
    Tpcds(u8),
}

impl std::fmt::Display for BenchmarkQueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkQueryType::Tpch(id) => write!(f, "TPC-H Q{:02}", id),
            BenchmarkQueryType::Tpcds(id) => write!(f, "TPC-DS Q{:02}", id),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkRunConfig {
    pub scale_factor: u64,
    pub engine: BenchmarkEngine,
    pub queries: Vec<BenchmarkQueryType>,
    pub warmup_runs: usize,
    pub measurement_runs: usize,
    pub num_threads: usize,
    pub data_directory: String,
    pub output_directory: String,
    pub enable_hardware_telemetry: bool,
}

impl Default for BenchmarkRunConfig {
    fn default() -> Self {
        Self {
            scale_factor: 1,
            engine: BenchmarkEngine::KinePlex,
            queries: (1..=22).map(BenchmarkQueryType::Tpch).collect(),
            warmup_runs: 1,
            measurement_runs: 5,
            num_threads: num_cpus(),
            data_directory: "/tmp/tpch_data".to_string(),
            output_directory: "./reports".to_string(),
            enable_hardware_telemetry: true,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
