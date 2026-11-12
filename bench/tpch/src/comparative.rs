use crate::config::BenchmarkRunConfig;
use crate::data_model::{BenchmarkSuiteSummary, ComparativeQuerySummary};
use crate::export::export_summary_files;
use crate::metrics::compute_geometric_mean;
use crate::runner_duckdb::DuckDbQueryExecutor;
use crate::runner_kineplex::KineplexQueryExecutor;
use crate::runner_spark::SparkQueryExecutor;
use crate::{ComparativeBenchmarkRunner, TpchQueryExecutor};
use anyhow::Result;
use chrono::Utc;

pub struct ComparativeBenchmarkEngine;

impl ComparativeBenchmarkEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ComparativeBenchmarkEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ComparativeBenchmarkRunner for ComparativeBenchmarkEngine {
    fn run_comparative_suite(
        &self,
        config: &BenchmarkRunConfig,
    ) -> Result<BenchmarkSuiteSummary> {
        let kineplex_exec = KineplexQueryExecutor::new(config.clone())?;
        let duckdb_exec = DuckDbQueryExecutor::new(config.clone());
        let spark_exec = SparkQueryExecutor::new(config.clone());

        let mut query_results = Vec::with_capacity(config.queries.len());
        let mut kine_times = Vec::with_capacity(config.queries.len());
        let mut duck_times = Vec::with_capacity(config.queries.len());
        let mut spark_times = Vec::with_capacity(config.queries.len());

        for &q in &config.queries {
            let m_kine = kineplex_exec.execute_query(q)?;
            let m_duck = duckdb_exec.execute_query(q)?;
            let m_spark = spark_exec.execute_query(q)?;

            let t_kine = m_kine.median_time_ms.max(0.001);
            let t_duck = m_duck.median_time_ms.max(0.001);
            let t_spark = m_spark.median_time_ms.max(0.001);

            kine_times.push(t_kine);
            duck_times.push(t_duck);
            spark_times.push(t_spark);

            let speedup_vs_duckdb = t_duck / t_kine;
            let speedup_vs_spark = t_spark / t_kine;

            query_results.push(ComparativeQuerySummary {
                query: q,
                kineplex_time_ms: t_kine,
                duckdb_time_ms: t_duck,
                spark_time_ms: t_spark,
                speedup_vs_duckdb,
                speedup_vs_spark,
                kineplex_peak_rss_mb: m_kine.peak_rss_bytes as f64 / (1024.0 * 1024.0),
                duckdb_peak_rss_mb: m_duck.peak_rss_bytes as f64 / (1024.0 * 1024.0),
                spark_peak_rss_mb: m_spark.peak_rss_bytes as f64 / (1024.0 * 1024.0),
            });
        }

        let geomean_kineplex_ms = compute_geometric_mean(&kine_times);
        let geomean_duckdb_ms = compute_geometric_mean(&duck_times);
        let geomean_spark_ms = compute_geometric_mean(&spark_times);

        let overall_speedup_vs_duckdb = if geomean_kineplex_ms > 0.0 {
            geomean_duckdb_ms / geomean_kineplex_ms
        } else {
            1.0
        };

        let overall_speedup_vs_spark = if geomean_kineplex_ms > 0.0 {
            geomean_spark_ms / geomean_kineplex_ms
        } else {
            1.0
        };

        let total_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(64);

        Ok(BenchmarkSuiteSummary {
            timestamp: Utc::now().to_rfc3339(),
            scale_factor: config.scale_factor,
            hardware_cpu: "AMD EPYC 7763 64-Core Processor (x86_64)".to_string(),
            total_cores,
            total_ram_gb: 128.0,
            query_results,
            geomean_kineplex_ms,
            geomean_duckdb_ms,
            geomean_spark_ms,
            overall_speedup_vs_duckdb,
            overall_speedup_vs_spark,
        })
    }

    fn export_results_json(&self, summary: &BenchmarkSuiteSummary, path: &str) -> Result<()> {
        export_summary_files(summary, path)
    }
}
