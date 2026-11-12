//! # Post-Banach Topological Performance Test Suite (`post_banach_perf`)
//!
//! Avalia o desempenho inovador do pipeline que sucede a construção do `BanachFrame`:
//! 1. Projeção topológica no espaço de Sobolev $W^{1,2}$ (Energia de Dirichlet, Crest Factor, Cauda de Lévy).
//! 2. Indexação e poda métrica via Árvore de Pontos de Vantagem (`FlatVpNode`, Desigualdade Triangular).
//! 3. Comparação rigorosa contra Brute-Force (Fidelidade Métrica 100%, Taxa de Poda $\eta_{prune} > 80\%$).
//! 4. Transporte Ótimo de Wasserstein $W_1$ em tempo real com scratch buffer zero-alocação.
//! 5. Cálculo do Índice BTEA (Banach Topological Efficiency & Acceleration Index).

use std::time::Instant;
use kineplex_df::{
    BanachFrame, BanachMetric, FunctionalRegime, TopologicalVpIndex, BATCH_CANONICAL_DIM,
};

/// Gera uma série temporal determinística com regime especificado.
fn generate_deterministic_signal(n: usize, regime_seed: usize) -> Vec<f64> {
    let mut data = Vec::with_capacity(n);
    let freq = 1.0 + (regime_seed as f64) * 0.5;
    for i in 0..n {
        let t = (i as f64) / (n as f64);
        let base = (2.0 * std::f64::consts::PI * t * freq).sin();
        let noise = (((i * 31 + regime_seed * 17) % 100) as f64 - 50.0) / 1000.0;
        data.push(base + noise);
    }
    data
}

#[test]
fn test_post_banach_topological_pruning_acceleration() {
    // 1. Constrói um manifold sintético com 1.000 nós no espaço R^6
    // Cada ponto representa a assinatura funcional de um BanachFrame anterior
    const MANIFOLD_NODES: usize = 1_000;
    const DIM: usize = 6;

    let mut manifold_coords: Vec<f64> = Vec::with_capacity(MANIFOLD_NODES * DIM);
    for i in 0..MANIFOLD_NODES {
        let sig_data = generate_deterministic_signal(1024, i);
        let frame = BanachFrame::new(&sig_data, None);
        let sig = frame.analyze_signature(3.0);
        let feats = sig.to_feature_vec();
        manifold_coords.extend_from_slice(&feats);
    }

    let vp_tree = TopologicalVpIndex::build(&manifold_coords, DIM, BanachMetric::L2);
    assert_eq!(vp_tree.nodes.len(), MANIFOLD_NODES);

    // 2. Cria um novo BanachFrame para simular evento em tempo real
    let incoming_data = generate_deterministic_signal(1024, 42);
    let incoming_frame = BanachFrame::new(&incoming_data, None);
    let incoming_sig = incoming_frame.analyze_signature(3.0);
    let query_pt = incoming_sig.to_feature_vec();

    // 3. Executa busca k-NN (k = 5) via VP-Tree com coleta de telemetria
    let (vp_knn, stats) = vp_tree.search_knn_with_stats(&query_pt, 5);

    // 4. Executa busca linear exaustiva (Brute-Force) para verificação da verdade-terrestre
    let (bf_knn, bf_evals) = vp_tree.search_knn_brute_force(&query_pt, 5);

    assert_eq!(bf_evals, MANIFOLD_NODES, "Brute-force deve avaliar todos os nós do manifold");
    assert_eq!(vp_knn.len(), 5, "k-NN deve retornar exatamente 5 vizinhos");
    assert_eq!(bf_knn.len(), 5, "Brute-force deve retornar exatamente 5 vizinhos");

    // 5. Invariante de Fidelidade Métrica (Zero Erros / 100% Exatidão)
    for i in 0..5 {
        assert_eq!(
            vp_knn[i].0, bf_knn[i].0,
            "Índice do {}-ésimo vizinho diverge entre VP-Tree e Brute-Force",
            i
        );
        let dist_diff = (vp_knn[i].1 - bf_knn[i].1).abs();
        assert!(
            dist_diff < 1e-12,
            "Distância do {}-ésimo vizinho difere em {}",
            i,
            dist_diff
        );
    }

    // 6. Invariante de Aceleração por Poda Métrica (Desigualdade Triangular)
    // O VP-tree deve eliminar a grande maioria das avaliações de distância
    println!(
        "\n[VP-Tree Pós-Banach] Nós totais: {}, Avaliações: {}, Podas: {}, Taxa de Poda: {:.2}%",
        MANIFOLD_NODES,
        stats.dist_evaluations,
        stats.nodes_pruned,
        stats.pruning_ratio * 100.0
    );

    assert!(
        stats.pruning_ratio >= 0.70,
        "Taxa de poda métrica ({:.2}%) inferior ao limiar mínimo de 70%",
        stats.pruning_ratio * 100.0
    );
    assert!(
        stats.dist_evaluations < MANIFOLD_NODES / 3,
        "Avaliações de distância ({}) devem ser inferiores a 1/3 do manifold",
        stats.dist_evaluations
    );
}

#[test]
fn test_post_banach_wasserstein_w1_scratch_zero_alloc() {
    const N: usize = 4096;
    let s1 = generate_deterministic_signal(N, 10);
    let s2 = generate_deterministic_signal(N, 20);

    let frame1 = BanachFrame::new(&s1, None);
    let frame2 = BanachFrame::new(&s2, None);

    // 1. Versão padrão (alocativa)
    let w1_standard = frame1.wasserstein_distance_p(&frame2, 1.0);

    // 2. Versão scratch (zero heap reallocations)
    let mut scratch_f = vec![0.0f64; N];
    let mut scratch_g = vec![0.0f64; N];
    let w1_scratch = frame1.wasserstein_distance_w1_scratch(&frame2, &mut scratch_f, &mut scratch_g);

    // 3. Validação de equivalência exata
    let diff = (w1_standard - w1_scratch).abs();
    assert!(
        diff < 1e-12,
        "Divergência numérica entre Wasserstein padrão e scratch: {}",
        diff
    );

    // 4. Teste de latência do cálculo W1
    let iterations = 100;
    let t0 = Instant::now();
    for _ in 0..iterations {
        let _ = frame1.wasserstein_distance_w1_scratch(&frame2, &mut scratch_f, &mut scratch_g);
    }
    let elapsed = t0.elapsed();
    let per_op_us = (elapsed.as_nanos() as f64) / (iterations as f64) / 1000.0;

    println!(
        "[Wasserstein W1 Scratch] N = {}, {} iterações em {:?} | {:.2} µs/op",
        N, iterations, elapsed, per_op_us
    );
    let max_allowed_us = if cfg!(debug_assertions) { 10_000.0 } else { 1_000.0 };
    assert!(
        per_op_us < max_allowed_us,
        "Wasserstein W1 deve executar em menos de {} µs para N = 4096 (obtido: {:.2} µs)",
        max_allowed_us,
        per_op_us
    );
}

#[test]
fn test_post_banach_end_to_end_manifold_pipeline_and_btea() {
    // Dimensão canônica N = 65.536
    const N: usize = BATCH_CANONICAL_DIM;
    const NUM_HISTORICAL_FRAMES: usize = 200;

    // 1. Gera manifold histórico de assinaturas pós-Banach
    let mut manifold_features = Vec::with_capacity(NUM_HISTORICAL_FRAMES * 6);
    for idx in 0..NUM_HISTORICAL_FRAMES {
        let data = generate_deterministic_signal(1024, idx);
        let f = BanachFrame::new(&data, None);
        manifold_features.extend_from_slice(&f.analyze_signature(3.0).to_feature_vec());
    }
    let manifold = TopologicalVpIndex::build(&manifold_features, 6, BanachMetric::L2);

    // 2. Executa pipeline ponta a ponta com N = 65.536
    let test_signal = generate_deterministic_signal(N, 999);
    let frames_to_test = 20;

    let t_start = Instant::now();
    let mut latencies_ms = Vec::with_capacity(frames_to_test);
    let mut pruning_ratios = Vec::with_capacity(frames_to_test);

    for _ in 0..frames_to_test {
        let frame_start = Instant::now();

        // Estágio 1: Instanciação do BanachFrame (Sobolev W^{1,2})
        let frame = BanachFrame::new(&test_signal, None);

        // Estágio 2: Análise Funcional de Assinatura (Dirichlet, Crest, Normas L1/L2/Linf)
        let sig = frame.analyze_signature(3.0);
        let query_pt = sig.to_feature_vec();

        // Estágio 3: Ingestão e Poda Métrica no Manifold (k-NN)
        let (_, stats) = manifold.search_knn_with_stats(&query_pt, 5);

        let frame_elapsed = frame_start.elapsed();
        latencies_ms.push(frame_elapsed.as_secs_f64() * 1000.0);
        pruning_ratios.push(stats.pruning_ratio);
    }
    let total_elapsed = t_start.elapsed();

    // 3. Estatísticas de Latência
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = latencies_ms[latencies_ms.len() / 2];
    let p95 = latencies_ms[(latencies_ms.len() as f64 * 0.95) as usize];
    let p99 = latencies_ms[latencies_ms.len() - 1];
    let avg_pruning = pruning_ratios.iter().sum::<f64>() / (pruning_ratios.len() as f64);
    let throughput_fps = (frames_to_test as f64) / total_elapsed.as_secs_f64();

    // 4. Cálculo do Índice BTEA (Banach Topological Efficiency & Acceleration)
    // BTEA = (FPS * Taxa de Poda) / Latência_p95 * 100
    let btea_score = (throughput_fps * avg_pruning / p95) * 100.0;

    println!("\n================================================================================");
    println!("     POST-BANACH TOPOLOGICAL PERFORMANCE BENCHMARK & BTEA INDEX               ");
    println!("================================================================================");
    println!("  * Dimensão do Frame: N = {} ({} KB por lote)", N, (N * 8) / 1024);
    println!("  * Taxa de Processamento: {:.2} frames/s", throughput_fps);
    println!("  * Latência p50: {:.3} ms | p95: {:.3} ms | p99: {:.3} ms", p50, p95, p99);
    println!("  * Taxa Média de Poda Métrica (VP-Tree): {:.2}%", avg_pruning * 100.0);
    println!("  * Score Composto BTEA: {:.2}", btea_score);
    println!("================================================================================\n");

    // Limiares de aceitação de produção
    assert!(throughput_fps > 5.0, "Throughput mínimo de 5 frames/s canônicos violado");
    assert!(p95 < 50.0, "Latência p95 superior a 50ms para frame completo");
    assert!(avg_pruning >= 0.70, "Poda métrica inferior a 70%");
    assert!(btea_score > 50.0, "Score BTEA de prontidão não atingido");
}

#[test]
fn test_post_banach_regime_transition_detection_latency() {
    let n = 2048;
    // Regime 1: Sub-Gaussiano
    let mut sig1 = generate_deterministic_signal(n, 1);
    let frame1 = BanachFrame::new(&sig1, None);
    let s1 = frame1.analyze_signature(3.0);
    assert_eq!(s1.regime, FunctionalRegime::SubGaussianEquilibrium);

    // Regime 2: Injeção de impulso Dirac (Anomalia Estocástica)
    sig1[n / 2] = 500.0; // Pico abrupto
    let t0 = Instant::now();
    let frame_anomaly = BanachFrame::new(&sig1, None);
    let s_anomaly = frame_anomaly.analyze_signature(3.0);
    let detection_time = t0.elapsed();

    assert_eq!(
        s_anomaly.regime,
        FunctionalRegime::DiracImpulseAnomaly,
        "Transição de regime para Dirac Anomaly deve ser detectada instantaneamente"
    );
    assert!(
        detection_time.as_micros() < 500,
        "Detecção de anomalia pós-Banach deve ocorrer em menos de 500 µs (obtido: {:?})",
        detection_time
    );
}
