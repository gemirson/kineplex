pub mod config;
pub mod data_model;
pub mod perf_events;
pub mod metrics;
pub mod datagen;
pub mod queries_tpch_part1;
pub mod queries_tpch_part2;
pub mod queries_tpch_part3;
pub mod queries_tpcds;
pub mod runner_kineplex;
pub mod runner_duckdb;
pub mod runner_spark;
pub mod comparative;
pub mod export;
pub mod whitepaper_sections;
pub mod whitepaper;

pub use config::{BenchmarkEngine, BenchmarkQueryType, BenchmarkRunConfig};
pub use data_model::{
    BenchmarkSuiteSummary, ComparativeQuerySummary, HardwareTelemetrySnapshot,
    QueryExecutionMetric, WhitepaperDocument,
};

use anyhow::Result;

pub trait TpchQueryExecutor: Send + Sync {
    /// Executa uma consulta específica do TPC-H (1 a 22) ou TPC-DS e retorna métricas
    fn execute_query(&self, query: BenchmarkQueryType) -> Result<QueryExecutionMetric>;

    /// Executa todas as consultas configuradas sequencialmente
    fn execute_all_queries(&self, queries: &[BenchmarkQueryType]) -> Result<Vec<QueryExecutionMetric>> {
        let mut results = Vec::with_capacity(queries.len());
        for q in queries {
            results.push(self.execute_query(*q)?);
        }
        Ok(results)
    }
}

pub trait ComparativeBenchmarkRunner {
    /// Executa o benchmark comparativo completo entre KinePlex, DuckDB e Spark
    fn run_comparative_suite(
        &self,
        config: &BenchmarkRunConfig,
    ) -> Result<BenchmarkSuiteSummary>;

    /// Exporta os resultados estruturados em JSON e CSV
    fn export_results_json(&self, summary: &BenchmarkSuiteSummary, path: &str) -> Result<()>;
}

pub trait WhitepaperPublisher {
    /// Compila os dados empíricos em um documento científico completo no formato Markdown
    fn compile_whitepaper(
        &self,
        summary_1tb: &BenchmarkSuiteSummary,
        summary_10tb: Option<&BenchmarkSuiteSummary>,
        output_markdown_path: &str,
    ) -> Result<()>;
}
