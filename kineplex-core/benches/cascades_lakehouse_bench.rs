use std::time::Instant;
use kineplex_core::cbo::{GraphEdgeStatistics, KinePlexCascadesOptimizer};
use kineplex_core::lakehouse::{can_commute_concurrent_commit, UnifiedDataFileAction};
use kineplex_core::simd::compute_quote_mask_dispatch;

fn main() {
    println!("=== Benchmark: KinePlex Riemannian Cascades, Lakehouse & SIMD ===");

    // 1. Benchmark CBO: Curvatura de Ollivier-Ricci e Seleção de Plano
    println!("\n--- 1. CBO Ollivier-Ricci Decision Latency ---");
    let optimizer = KinePlexCascadesOptimizer::default();
    let stats = GraphEdgeStatistics::new(10.0, 3.0, 1_000_000, 1_000_000, true);

    let iterations_cbo = 100_000;
    let start_cbo = Instant::now();
    for _ in 0..iterations_cbo {
        let (plan, _cost) = optimizer.select_optimal_join_plan(&stats);
        std::hint::black_box(plan);
    }
    let elapsed_cbo = start_cbo.elapsed();
    let ns_per_cbo = elapsed_cbo.as_nanos() as f64 / iterations_cbo as f64;
    println!("Iterações: {}", iterations_cbo);
    println!("Latência média por aresta: {:.2} ns (Target: < 50 ns)", ns_per_cbo);
    assert!(ns_per_cbo < 50.0, "Latência do CBO deve ser inferior a 50 ns");

    // 2. Benchmark Lakehouse: Validação Comutativa SSI (Diamond Property)
    println!("\n--- 2. Lakehouse SSI Commutativity Validation ---");
    let mut actions = Vec::with_capacity(100);
    for i in 0..100 {
        actions.push(UnifiedDataFileAction::new(
            format!("file_{}.parquet", i),
            128_000_000,
            100_000,
            vec![(i * 100) as f64, 0.0],
            vec![((i + 1) * 100 - 1) as f64, 50.0],
            vec![0, 0],
            1700000000,
        ));
    }
    let query_min = vec![20_000.0, 60.0];
    let query_max = vec![30_000.0, 100.0];

    let iterations_ssi = 10_000;
    let start_ssi = Instant::now();
    for _ in 0..iterations_ssi {
        let ok = can_commute_concurrent_commit(&actions, &query_min, &query_max);
        std::hint::black_box(ok);
    }
    let elapsed_ssi = start_ssi.elapsed();
    let ns_per_ssi = elapsed_ssi.as_nanos() as f64 / iterations_ssi as f64;
    println!("Iterações: {}", iterations_ssi);
    println!("Latência média de validação SSI: {:.2} ns (Target: < 100 ns)", ns_per_ssi);

    // 3. Benchmark SIMD GF(2)[x] Dispatcher Throughput
    println!("\n--- 3. SIMD GF(2)[x] Dispatcher Throughput ---");
    let mut block = [b'A'; 64];
    block[0] = b'"';
    block[4] = b';';
    block[8] = b'"';
    block[12] = b';';
    block[63] = b'\n';

    let iterations_simd = 10_000_000;
    let start_simd = Instant::now();
    let mut state = 0u64;
    for _ in 0..iterations_simd {
        let mask = compute_quote_mask_dispatch(&block, b';', state);
        state = mask.next_prev_in_quote;
        std::hint::black_box(mask);
    }
    let elapsed_simd = start_simd.elapsed();

    let total_bytes = (iterations_simd * 64) as f64;
    let gb_processed = total_bytes / (1024.0 * 1024.0 * 1024.0);
    let throughput_gbs = gb_processed / elapsed_simd.as_secs_f64();
    println!("Blocos de 64B processados: {}", iterations_simd);
    println!("Tempo total: {:?}", elapsed_simd);
    println!("Throughput Medido SIMD: {:.2} GB/s", throughput_gbs);
    assert!(throughput_gbs >= 2.0, "Throughput SIMD deve ser superior a 2.0 GB/s");

    println!("\n=== Benchmark FT-100 Concluído com Sucesso ===");
}
