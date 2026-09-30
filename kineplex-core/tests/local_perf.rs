//! Local Performance Evaluation Test for KinePlex Data Plane (FT-081, FT-082)
//!
//! Evaluates:
//! 1. End-to-end distributed pipeline stages (Receptor -> Wasm -> Aggregation -> Terminal)
//! 2. Real Parquet output generation (part-0.parquet) and verification
//! 3. Arrow IPC serialization / deserialization throughput
//! 4. Latency percentiles (p50, p95, p99), standard deviation and variability

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kineplex_core::data::{
    AggregationStage, ArrowData, NodeId, ReceptorStage, StageExecutor, TerminalStage,
};

/// Helper to generate a realistic Arrow RecordBatch of `num_rows` rows.
fn generate_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric_value", DataType::Float64, false),
        Field::new("sensor_tag", DataType::Utf8, false),
    ]));

    let mut ids = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    let mut tags = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        ids.push(i as i64);
        values.push((i as f64) * 0.12345);
        tags.push(format!("sensor_{:04}", i % 50));
    }

    let id_array = Arc::new(Int64Array::from(ids)) as Arc<dyn Array>;
    let value_array = Arc::new(Float64Array::from(values)) as Arc<dyn Array>;
    let tag_array = Arc::new(StringArray::from(tags)) as Arc<dyn Array>;

    RecordBatch::try_new(schema, vec![id_array, value_array, tag_array])
        .expect("RecordBatch creation should succeed")
}

#[derive(Debug)]
#[allow(dead_code)]
struct Stats {
    count: usize,
    mean_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    min_ms: f64,
    max_ms: f64,
    std_dev_ms: f64,
    cv_percent: f64,
    throughput_rows_per_sec: f64,
    throughput_mb_per_sec: f64,
}

fn calculate_stats(mut samples: Vec<f64>, rows: usize, approx_bytes: usize) -> Stats {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples.len();
    let sum: f64 = samples.iter().sum();
    let mean = sum / (n as f64);

    let variance: f64 = samples.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / (n as f64);
    let std_dev = variance.sqrt();
    let cv = if mean > 0.0 { (std_dev / mean) * 100.0 } else { 0.0 };

    let p50 = samples[(n as f64 * 0.50) as usize];
    let p95 = samples[((n as f64 * 0.95) as usize).min(n - 1)];
    let p99 = samples[((n as f64 * 0.99) as usize).min(n - 1)];
    let min = samples[0];
    let max = samples[n - 1];

    let throughput_rows = if mean > 0.0 { (rows as f64) / (mean / 1000.0) } else { 0.0 };
    let throughput_mb = if mean > 0.0 {
        ((approx_bytes as f64) / (1024.0 * 1024.0)) / (mean / 1000.0)
    } else {
        0.0
    };

    Stats {
        count: n,
        mean_ms: mean,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        min_ms: min,
        max_ms: max,
        std_dev_ms: std_dev,
        cv_percent: cv,
        throughput_rows_per_sec: throughput_rows,
        throughput_mb_per_sec: throughput_mb,
    }
}

#[tokio::test]
async fn test_local_performance_data_plane_evaluation() {
    println!("\n================================================================================");
    println!("        KINEPLEX LOCAL PERFORMANCE EVALUATION (FT-081 & FT-082)");
    println!("================================================================================");
    println!("OS: {}", std::env::consts::OS);
    println!("Arch: {}", std::env::consts::ARCH);
    println!("Num CPUs: {}", std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1));
    println!("Timestamp: {}", chrono::Utc::now().to_rfc3339());
    println!("--------------------------------------------------------------------------------\n");

    let (temp_dir, should_cleanup) = if let Ok(custom_dir) = std::env::var("KINEPLEX_OUTPUT_DIR") {
        (PathBuf::from(custom_dir), false)
    } else {
        (std::env::temp_dir().join(format!("kineplex_perf_{}", uuid::Uuid::new_v4())), true)
    };
    fs::create_dir_all(&temp_dir).expect("Failed to create temporary perf directory");

    let node1 = NodeId::new("node-ingress-01");
    let node2 = NodeId::new("node-wasm-02");
    let node3 = NodeId::new("node-agg-03");
    let node4 = NodeId::new("node-terminal-04");

    let datasets = vec![
        ("small", 50_000, 3, 10),
        ("medium", 200_000, 2, 8),
        ("large", 500_000, 1, 5),
    ];

    let mut report_tables = Vec::new();

    for (name, rows, warmups, iterations) in datasets {
        println!(">>> Testing Dataset '{}' ({} rows)...", name, rows);
        let batch = generate_batch(rows);
        let approx_bytes = batch.get_array_memory_size();
        let arrow_data = ArrowData::new(batch);

        // 1. Arrow IPC Serialization / Deserialization benchmark
        let mut ipc_ser_times = Vec::new();
        let mut ipc_deser_times = Vec::new();
        let mut ipc_bytes_len = 0;

        for _ in 0..iterations {
            let t0 = Instant::now();
            let ipc_bytes = arrow_data.to_ipc().expect("to_ipc failed");
            ipc_ser_times.push(t0.elapsed().as_secs_f64() * 1000.0);
            ipc_bytes_len = ipc_bytes.len();

            let t1 = Instant::now();
            let _decoded = ArrowData::from_ipc(&ipc_bytes).expect("from_ipc failed");
            ipc_deser_times.push(t1.elapsed().as_secs_f64() * 1000.0);
        }

        let ipc_ser_stats = calculate_stats(ipc_ser_times, rows, ipc_bytes_len);
        let ipc_deser_stats = calculate_stats(ipc_deser_times, rows, ipc_bytes_len);

        println!(
            "  * Arrow IPC: size = {:.2} MB | Ser: {:.2} ms ({:.0} rows/s, {:.1} MB/s) | Deser: {:.2} ms ({:.0} rows/s, {:.1} MB/s)",
            (ipc_bytes_len as f64) / (1024.0 * 1024.0),
            ipc_ser_stats.mean_ms,
            ipc_ser_stats.throughput_rows_per_sec,
            ipc_ser_stats.throughput_mb_per_sec,
            ipc_deser_stats.mean_ms,
            ipc_deser_stats.throughput_rows_per_sec,
            ipc_deser_stats.throughput_mb_per_sec
        );

        // 2. Full Distributed Pipeline Stages benchmark
        let receptor = ReceptorStage;
        let wasm = kineplex_core::data::WasmStage::new(vec![0x00, 0x61, 0x73, 0x6d], 256, 1_000_000);
        let aggregation = AggregationStage;
        let dataset_output_dir = if should_cleanup {
            temp_dir.join(name)
        } else {
            temp_dir.clone()
        };
        let terminal = TerminalStage::new(dataset_output_dir.to_str().unwrap());

        // Warmup
        for _ in 0..warmups {
            let r_out = receptor.execute(arrow_data.clone(), &node1).await.unwrap();
            let w_out = wasm.execute(r_out, &node2).await.unwrap();
            let a_out = aggregation.execute(w_out, &node3).await.unwrap();
            let _t_out = terminal.execute(a_out, &node4).await.unwrap();
        }

        let mut pipeline_total_times = Vec::new();
        let mut receptor_times = Vec::new();
        let mut wasm_times = Vec::new();
        let mut agg_times = Vec::new();
        let mut terminal_times = Vec::new();
        let mut generated_parquet_path = String::new();

        for _ in 0..iterations {
            let total_start = Instant::now();

            let t_r0 = Instant::now();
            let r_out = receptor.execute(arrow_data.clone(), &node1).await.unwrap();
            receptor_times.push(t_r0.elapsed().as_secs_f64() * 1000.0);

            let t_w0 = Instant::now();
            let w_out = wasm.execute(r_out, &node2).await.unwrap();
            wasm_times.push(t_w0.elapsed().as_secs_f64() * 1000.0);

            let t_a0 = Instant::now();
            let a_out = aggregation.execute(w_out, &node3).await.unwrap();
            agg_times.push(t_a0.elapsed().as_secs_f64() * 1000.0);

            let t_t0 = Instant::now();
            let t_out = terminal.execute(a_out, &node4).await.unwrap();
            terminal_times.push(t_t0.elapsed().as_secs_f64() * 1000.0);

            pipeline_total_times.push(total_start.elapsed().as_secs_f64() * 1000.0);

            if generated_parquet_path.is_empty() {
                generated_parquet_path = t_out.metadata.get("parquet_path").cloned().unwrap_or_default();
            }
        }

        let total_stats = calculate_stats(pipeline_total_times, rows, approx_bytes);
        let rec_stats = calculate_stats(receptor_times, rows, approx_bytes);
        let wasm_stats = calculate_stats(wasm_times, rows, approx_bytes);
        let agg_stats = calculate_stats(agg_times, rows, approx_bytes);
        let term_stats = calculate_stats(terminal_times, rows, approx_bytes);

        // Verify Parquet file
        let parquet_file = PathBuf::from(&generated_parquet_path);
        let parquet_exists = parquet_file.exists();
        let parquet_size_bytes = if parquet_exists {
            fs::metadata(&parquet_file).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        println!(
            "  * E2E Pipeline Mean: {:.2} ms | p50: {:.2} ms | p95: {:.2} ms | StdDev: {:.2} ms (CV: {:.1}%)",
            total_stats.mean_ms, total_stats.p50_ms, total_stats.p95_ms, total_stats.std_dev_ms, total_stats.cv_percent
        );
        println!(
            "  * Throughput: {:.0} rows/s | {:.2} MB/s",
            total_stats.throughput_rows_per_sec, total_stats.throughput_mb_per_sec
        );
        println!(
            "  * Stage Latencies (ms): Receptor={:.2}, Wasm={:.2}, Aggregation={:.2}, Terminal(Parquet)={:.2}",
            rec_stats.mean_ms, wasm_stats.mean_ms, agg_stats.mean_ms, term_stats.mean_ms
        );
        println!(
            "  * Parquet Output: exists={}, path={}, size={:.2} KB\n",
            parquet_exists, generated_parquet_path, (parquet_size_bytes as f64) / 1024.0
        );

        assert!(parquet_exists, "Parquet file should have been materialized");
        assert!(parquet_size_bytes > 0, "Parquet file size must be > 0");

        report_tables.push((name, rows, total_stats, rec_stats, wasm_stats, agg_stats, term_stats, parquet_size_bytes));
    }

    // Print summary markdown table
    println!("================================================================================");
    println!("                 BENCHMARK SUMMARY REPORT (MARKDOWN TABLE)");
    println!("================================================================================");
    println!("| Dataset | Rows | E2E Mean (ms) | p50 (ms) | p95 (ms) | StdDev (ms) | CV (%) | Throughput (rows/s) | Parquet Size |");
    println!("|---|---|---|---|---|---|---|---|---|");
    for (name, rows, total, _, _, _, _, pq_size) in &report_tables {
        println!(
            "| **{}** | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.1}% | {:.0} | {:.1} KB |",
            name, rows, total.mean_ms, total.p50_ms, total.p95_ms, total.std_dev_ms, total.cv_percent, total.throughput_rows_per_sec, (*pq_size as f64) / 1024.0
        );
    }

    println!("\n### Latency Breakdown by Stage (Mean ms)");
    println!("| Dataset | Receptor | Wasm Stage | Aggregation | Terminal (Parquet) | Total |");
    println!("|---|---|---|---|---|---|");
    for (name, _, total, rec, wasm, agg, term, _) in &report_tables {
        println!(
            "| **{}** | {:.2} ({:.1}%) | {:.2} ({:.1}%) | {:.2} ({:.1}%) | {:.2} ({:.1}%) | {:.2} |",
            name,
            rec.mean_ms, (rec.mean_ms / total.mean_ms) * 100.0,
            wasm.mean_ms, (wasm.mean_ms / total.mean_ms) * 100.0,
            agg.mean_ms, (agg.mean_ms / total.mean_ms) * 100.0,
            term.mean_ms, (term.mean_ms / total.mean_ms) * 100.0,
            total.mean_ms
        );
    }
    println!("================================================================================\n");

    // Cleanup temp dir
    if should_cleanup {
        let _ = fs::remove_dir_all(&temp_dir);
    }
}

#[tokio::test]
async fn test_ollivier_ricci_geometric_flow_performance() {
    use kineplex_core::geometry::{
        AdaptiveStepController, GeoNodeId, GeometricWarmup, MetricTensorNormalizer,
        MetricTensorSystem, Simplex2Discoverer, SyntheticSpike, WarmupError,
    };

    println!("\n================================================================================");
    println!("     OLLIVIER-RICCI / RICCI FLOW GEOMETRIC ENGINE BENCHMARK (FT-089..FT-092)   ");
    println!("================================================================================");

    // 1. FT-089: Adaptive Step PID Controller for Ricci Flow Curvature Performance
    println!(">>> 1. Benchmarking Adaptive Step PID Controller (Ricci Flow Step)...");
    let mut controller = AdaptiveStepController::new(0.5);
    let pid_iterations = 1_000_000;
    let t0 = Instant::now();
    for i in 0..pid_iterations {
        // Vary curvature gradient dynamically
        let curvature_change = ((i % 100) as f64 - 50.0) * 0.002;
        let _ = controller.compute(curvature_change);
    }
    let pid_elapsed = t0.elapsed();
    let pid_ns_per_op = (pid_elapsed.as_nanos() as f64) / (pid_iterations as f64);
    let pid_ops_sec = (pid_iterations as f64) / pid_elapsed.as_secs_f64();
    println!(
        "  * PID Step: {:?} for {} iterations | {:.2} ns/op | {:.0} ops/sec (Epsilon converged to {:.4})",
        pid_elapsed, pid_iterations, pid_ns_per_op, pid_ops_sec, controller.epsilon()
    );

    // 2. FT-090: Metric Tensor Volume Normalization & Bare-Metal Overflow Hardening
    println!("\n>>> 2. Benchmarking Metric Tensor Volume Normalization (Overflow Prevention)...");
    let tensor_sizes = vec![10_000, 100_000, 1_000_000];
    for size in tensor_sizes {
        let normalizer = MetricTensorNormalizer::new(size as f64 * 10.0);
        let mut weights: Vec<f64> = (0..size).map(|i| (i % 256) as f64 * 1.5).collect();

        let t_norm = Instant::now();
        normalizer.normalize(&mut weights).expect("Normalization must succeed");
        let norm_elapsed = t_norm.elapsed();

        let elem_sec = (size as f64) / norm_elapsed.as_secs_f64();
        let mb_sec = ((size * 8) as f64 / (1024.0 * 1024.0)) / norm_elapsed.as_secs_f64();

        println!(
            "  * Tensor elements: {:>9} | Time: {:>8.3} ms | Throughput: {:>10.0} elem/s ({:>8.1} MB/s) | Volume stable: {}",
            size,
            norm_elapsed.as_secs_f64() * 1000.0,
            elem_sec,
            mb_sec,
            normalizer.is_volume_stable(&weights)
        );
    }

    // 3. FT-091: 2-Simplex Discovery (Faces) and Betti Number (Beta 1) Homology Health
    println!("\n>>> 3. Benchmarking 2-Simplex Discovery & Betti Number (Topological Holes)...");
    let mut discoverer = Simplex2Discoverer::new();
    let num_nodes = 150;
    // Build a mesh graph with known triangles
    for i in 0..num_nodes {
        let a = GeoNodeId(format!("node_{}", i));
        let b = GeoNodeId(format!("node_{}", (i + 1) % num_nodes));
        let c = GeoNodeId(format!("node_{}", (i + 2) % num_nodes));
        discoverer.add_edge(a.clone(), b.clone());
        discoverer.add_edge(b, c.clone());
        discoverer.add_edge(c, a);
    }

    let t_simplex = Instant::now();
    let triangles = discoverer.find_triangles();
    let betti = discoverer.betti_number();
    let simplex_elapsed = t_simplex.elapsed();

    println!(
        "  * Mesh Topology: {} nodes, {} edges | Discovered {} triangles (2-simplices) in {:.3} ms",
        num_nodes,
        discoverer.count_edges(),
        triangles.len(),
        simplex_elapsed.as_secs_f64() * 1000.0
    );
    println!("  * Betti-1 Invariant (Euler-Poincaré): beta_1 = {}", betti);

    // 4. FT-092: Topological Warm-up Phase
    println!("\n>>> 4. Benchmarking Topological Warm-up Convergence (Spike Propagation)...");
    struct FastMetricSystem {
        variance: f64,
        calls: u32,
    }
    impl MetricTensorSystem for FastMetricSystem {
        async fn process_spike(&mut self, _spike: SyntheticSpike) -> Result<(), WarmupError> {
            self.calls += 1;
            // Simulate convergence after 25 spikes
            if self.calls > 25 {
                self.variance = 0.005;
            } else {
                self.variance = 0.1 / (self.calls as f64);
            }
            Ok(())
        }
        fn metric_variance(&self) -> f64 {
            self.variance
        }
        fn metric_mean(&self) -> f64 {
            1.0
        }
    }

    let mut metric_system = FastMetricSystem { variance: 1.0, calls: 0 };
    let mut warmup = GeometricWarmup::new()
        .with_spikes(200)
        .with_threshold(0.01);

    let t_warmup = Instant::now();
    let warmup_res = warmup.warmup(&mut metric_system).await.expect("Warmup should converge");
    let warmup_elapsed = t_warmup.elapsed();

    println!(
        "  * Warmup: converged in {:.2} ms using {} synthetic spikes (Operational: {})",
        warmup_elapsed.as_secs_f64() * 1000.0,
        warmup_res.spikes_used,
        warmup.is_operational()
    );
    println!("================================================================================\n");
}

