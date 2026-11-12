use crate::config::{BenchmarkEngine, BenchmarkQueryType, BenchmarkRunConfig};
use crate::data_model::QueryExecutionMetric;
use crate::datagen::TpchDataGenerator;
use crate::metrics::{calculate_statistics, calculate_throughput_gb_s, read_process_rss_bytes};
use crate::perf_events::PerfEventCollector;
use crate::queries_tpcds::TpcdsExecutor;
use crate::queries_tpch_part1::TpchPart1Executor;
use crate::queries_tpch_part2::TpchPart2Executor;
use crate::queries_tpch_part3::TpchPart3Executor;
use crate::TpchQueryExecutor;
use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use std::collections::HashMap;

pub struct KineplexQueryExecutor {
    config: BenchmarkRunConfig,
    tables: HashMap<String, RecordBatch>,
}

impl KineplexQueryExecutor {
    pub fn new(config: BenchmarkRunConfig) -> Result<Self> {
        let gen = TpchDataGenerator::new(config.scale_factor, &config.data_directory, true);
        let mut tables = HashMap::new();

        tables.insert("nation".to_string(), gen.generate_nation()?);
        tables.insert("region".to_string(), gen.generate_region()?);
        tables.insert("customer".to_string(), gen.generate_customer()?);
        tables.insert("part".to_string(), gen.generate_part()?);
        tables.insert("supplier".to_string(), gen.generate_supplier()?);
        tables.insert("partsupp".to_string(), gen.generate_partsupp()?);
        tables.insert("orders".to_string(), gen.generate_orders()?);
        tables.insert("lineitem".to_string(), gen.generate_lineitem()?);

        Ok(Self { config, tables })
    }

    fn run_single_query(&self, query: BenchmarkQueryType) -> Result<usize> {
        match query {
            BenchmarkQueryType::Tpch(1) => TpchPart1Executor::execute_q1(&self.tables),
            BenchmarkQueryType::Tpch(2) => TpchPart1Executor::execute_q2(&self.tables),
            BenchmarkQueryType::Tpch(3) => TpchPart1Executor::execute_q3(&self.tables),
            BenchmarkQueryType::Tpch(4) => TpchPart1Executor::execute_q4(&self.tables),
            BenchmarkQueryType::Tpch(5) => TpchPart1Executor::execute_q5(&self.tables),
            BenchmarkQueryType::Tpch(6) => TpchPart1Executor::execute_q6(&self.tables),
            BenchmarkQueryType::Tpch(7) => TpchPart2Executor::execute_q7(&self.tables),
            BenchmarkQueryType::Tpch(8) => TpchPart2Executor::execute_q8(&self.tables),
            BenchmarkQueryType::Tpch(9) => TpchPart2Executor::execute_q9(&self.tables),
            BenchmarkQueryType::Tpch(10) => TpchPart2Executor::execute_q10(&self.tables),
            BenchmarkQueryType::Tpch(11) => TpchPart2Executor::execute_q11(&self.tables),
            BenchmarkQueryType::Tpch(12) => TpchPart2Executor::execute_q12(&self.tables),
            BenchmarkQueryType::Tpch(13) => TpchPart2Executor::execute_q13(&self.tables),
            BenchmarkQueryType::Tpch(14) => TpchPart2Executor::execute_q14(&self.tables),
            BenchmarkQueryType::Tpch(15) => TpchPart3Executor::execute_q15(&self.tables),
            BenchmarkQueryType::Tpch(16) => TpchPart3Executor::execute_q16(&self.tables),
            BenchmarkQueryType::Tpch(17) => TpchPart3Executor::execute_q17(&self.tables),
            BenchmarkQueryType::Tpch(18) => TpchPart3Executor::execute_q18(&self.tables),
            BenchmarkQueryType::Tpch(19) => TpchPart3Executor::execute_q19(&self.tables),
            BenchmarkQueryType::Tpch(20) => TpchPart3Executor::execute_q20(&self.tables),
            BenchmarkQueryType::Tpch(21) => TpchPart3Executor::execute_q21(&self.tables),
            BenchmarkQueryType::Tpch(22) => TpchPart3Executor::execute_q22(&self.tables),
            BenchmarkQueryType::Tpcds(1) => TpcdsExecutor::execute_ds_q01(&self.tables),
            BenchmarkQueryType::Tpcds(3) => TpcdsExecutor::execute_ds_q03(&self.tables),
            BenchmarkQueryType::Tpcds(14) => TpcdsExecutor::execute_ds_q14(&self.tables),
            BenchmarkQueryType::Tpcds(64) => TpcdsExecutor::execute_ds_q64(&self.tables),
            BenchmarkQueryType::Tpcds(72) => TpcdsExecutor::execute_ds_q72(&self.tables),
            BenchmarkQueryType::Tpcds(95) => TpcdsExecutor::execute_ds_q95(&self.tables),
            other => Err(anyhow!("Unsupported query: {:?}", other)),
        }
    }
}

impl TpchQueryExecutor for KineplexQueryExecutor {
    fn execute_query(&self, query: BenchmarkQueryType) -> Result<QueryExecutionMetric> {
        // 1. Warmup
        for _ in 0..self.config.warmup_runs {
            let _ = self.run_single_query(query)?;
        }

        // 2. Measurements
        let mut raw_times_ms = Vec::with_capacity(self.config.measurement_runs);
        let mut rows_scanned = 0;
        let mut collector = PerfEventCollector::new(self.config.enable_hardware_telemetry);
        let mut last_telemetry = None;

        for _ in 0..self.config.measurement_runs {
            collector.start();
            rows_scanned = self.run_single_query(query)?;
            let (elapsed_ms, telemetry) = collector.stop();
            raw_times_ms.push(elapsed_ms);
            last_telemetry = telemetry;
        }

        let stats = calculate_statistics(&raw_times_ms);
        let peak_rss_bytes = read_process_rss_bytes(None);
        let est_bytes = rows_scanned as u64 * 120; // ~120 bytes per row average
        let throughput = calculate_throughput_gb_s(est_bytes, stats.median_ms);

        Ok(QueryExecutionMetric {
            query,
            engine: BenchmarkEngine::KinePlex,
            scale_factor: self.config.scale_factor,
            raw_times_ms,
            median_time_ms: stats.median_ms,
            std_dev_ms: stats.std_dev_ms,
            peak_rss_bytes,
            total_rows_scanned: rows_scanned as u64,
            throughput_gb_per_sec: throughput,
            telemetry: last_telemetry,
        })
    }
}
