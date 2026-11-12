# Contract API: TPC-H / TPC-DS Benchmarks & Whitepaper (FT-101)

## 1. Traits Públicos da Suíte

### `TpchQueryExecutor` Trait
```rust
pub trait TpchQueryExecutor: Send + Sync {
    /// Executa uma consulta específica do TPC-H (1 a 22) e retorna os lotes resultantes e métricas
    fn execute_query(&self, query_id: u8) -> Result<QueryExecutionMetric, KinePlexError>;
    
    /// Executa todas as 22 consultas sequencialmente
    fn execute_all_queries(&self) -> Result<Vec<QueryExecutionMetric>, KinePlexError> {
        let mut results = Vec::with_capacity(22);
        for q in 1..=22 {
            results.push(self.execute_query(q)?);
        }
        Ok(results)
    }
}
```

### `ComparativeBenchmarkRunner` Trait
```rust
pub trait ComparativeBenchmarkRunner {
    /// Executa o benchmark comparativo completo entre KinePlex, DuckDB e Spark
    fn run_comparative_suite(
        &self,
        config: &BenchmarkRunConfig,
    ) -> Result<BenchmarkSuiteSummary, KinePlexError>;

    /// Exporta os resultados estruturados em JSON e CSV
    fn export_results_json(&self, summary: &BenchmarkSuiteSummary, path: &str) -> Result<(), KinePlexError>;
}
```

### `WhitepaperPublisher` Trait
```rust
pub trait WhitepaperPublisher {
    /// Compila os dados empíricos em um documento científico completo no formato Markdown
    fn compile_whitepaper(
        &self,
        summary_1tb: &BenchmarkSuiteSummary,
        summary_10tb: Option<&BenchmarkSuiteSummary>,
        output_markdown_path: &str,
    ) -> Result<(), KinePlexError>;
}
```
