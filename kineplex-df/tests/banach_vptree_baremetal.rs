//! Testes exaustivos de Análise Funcional, Topologia Métrica e Kernel Bare-Metal no KinePlex-DF.

use kineplex_df::{
    execute_native_vector_fma, process_with_l2_arena, BanachFrame, BanachMetric, FlatVpNode,
    FunctionalRegime, NativeBatchDescriptor, TopologicalVpIndex, BATCH_CANONICAL_DIM,
};

#[test]
fn test_vp_tree_node_size_and_metric_pruning() {
    // 1. Verificação de Alinhamento e Tamanho de Memória
    assert_eq!(
        std::mem::size_of::<FlatVpNode>(),
        32,
        "FlatVpNode deve ter exatamente 32 bytes para acomodar 2 nós por cacheline de 64 bytes"
    );

    // 2. Criação de Nuvem de Pontos em R^4
    let points: Vec<f64> = vec![
        0.0, 0.0, 0.0, 0.0, // pt 0
        1.0, 0.0, 0.0, 0.0, // pt 1 (dist = 1.0)
        0.0, 2.0, 0.0, 0.0, // pt 2 (dist = 2.0)
        5.0, 5.0, 5.0, 5.0, // pt 3 (dist = 10.0)
        0.0, 0.0, 0.5, 0.0, // pt 4 (dist = 0.5)
    ];

    let vp_index = TopologicalVpIndex::build(&points, 4, BanachMetric::L2);
    assert_eq!(vp_index.nodes.len(), 5);

    // 3. Busca por Bola Fechada com raio epsilon = 1.1 em torno da origem
    let query = [0.0, 0.0, 0.0, 0.0];
    let in_ball = vp_index.search_closed_ball(&query, 1.1);
    // Pontos dentro da bola de raio 1.1: pt 0 (0.0), pt 1 (1.0), pt 4 (0.5)
    assert!(in_ball.contains(&0));
    assert!(in_ball.contains(&1));
    assert!(in_ball.contains(&4));
    assert!(!in_ball.contains(&2)); // dist 2.0 > 1.1
    assert!(!in_ball.contains(&3)); // dist 10.0 > 1.1

    // 4. Busca k-NN (k = 3)
    let knn = vp_index.search_knn(&query, 3);
    assert_eq!(knn.len(), 3);
    assert_eq!(knn[0].0, 0); // dist 0.0
    assert_eq!(knn[1].0, 4); // dist 0.5
    assert_eq!(knn[2].0, 1); // dist 1.0

    // 5. Integridade de Checksum
    let chk = vp_index.compute_checksum();
    assert_ne!(chk, 0);
}

#[test]
fn test_banach_frame_sobolev_dirichlet_and_wavelets() {
    // 1. Criação de sinal financeiro simulado com dimensão canônica N = 65.536
    let n = BATCH_CANONICAL_DIM;
    let mut signal = Vec::with_capacity(n);
    for i in 0..n {
        let t = (i as f64) / (n as f64);
        // Sinal com componente de baixa frequência + ruído de alta frequência
        let val = (2.0 * std::f64::consts::PI * t * 4.0).sin() + 0.05 * ((i % 17) as f64 - 8.0);
        signal.push(val);
    }

    let frame = BanachFrame::new(&signal, None);
    assert_eq!(frame.len(), 65_536);

    // 2. Normas Lp e Análise Funcional
    let norm_l1 = frame.norm_l1();
    let norm_l2 = frame.norm_l2();
    let norm_linf = frame.norm_linf();
    assert!(norm_l1 > 0.0);
    assert!(norm_l2 > 0.0);
    assert!(norm_linf >= norm_l2);

    // 3. Energia de Dirichlet no Espaço de Sobolev W^{1,2}
    let dirichlet_e = frame.dirichlet_energy();
    assert!(dirichlet_e > 0.0);

    let sig = frame.analyze_signature(4.0);
    assert_eq!(sig.regime, FunctionalRegime::SubGaussianEquilibrium);

    // 4. Transformada Rápida de Wavelets de Haar (Isometria de Parseval)
    let wavelet_coeffs = frame.haar_wavelet_transform();
    assert_eq!(wavelet_coeffs.len(), n);

    // Verifica Isometria de Parseval: ||f||_L2 == ||Wavelet(f)||_L2
    let mut wavelet_energy = 0.0;
    for &c in wavelet_coeffs.as_slice() {
        wavelet_energy += c * c;
    }
    let wavelet_l2 = (wavelet_energy / (n as f64)).sqrt();
    let diff = (norm_l2 - wavelet_l2).abs();
    assert!(diff < 1e-10, "Isometria de Parseval violada: diff = {}", diff);

    // 5. Distância de Wasserstein W_1
    let frame_shifted = BanachFrame::new(
        &signal.iter().map(|&x| x + 2.0).collect::<Vec<f64>>(),
        None,
    );
    let w1 = frame.wasserstein_distance_p(&frame_shifted, 1.0);
    assert!(
        (w1 - 2.0).abs() < 1e-12,
        "Distância de Wasserstein de deslocamento constante deve ser exatamente o deslocamento"
    );
}

#[test]
fn test_gram_schmidt_orthogonal_decomposition() {
    let n = 1024;
    // Cria base ortonormal unidimensional (constante normalizada)
    let uniform = vec![1.0; n];
    let mut e1 = BanachFrame::new(&uniform, None);
    let norm = e1.norm_l2();
    for x in e1.data.as_mut_slice() {
        *x /= norm;
    }

    // Sinal de teste
    let mut test_signal = Vec::with_capacity(n);
    for i in 0..n {
        test_signal.push((i as f64) - 512.0); // média não-nula
    }
    let f = BanachFrame::new(&test_signal, None);

    // Decomposição ortogonal f = P_V(f) + f_perp
    let (p_v, f_perp) = f.project_orthogonal(&[e1]);

    // Teste de Ortogonalidade: <P_V(f), f_perp> == 0
    let cross_product = p_v.inner_product(&f_perp);
    assert!(
        cross_product.abs() < 1e-10,
        "Projeção ortogonal violada: produto interno = {}",
        cross_product
    );

    // Teste de Pitágoras: ||f||_2^2 == ||P_V||_2^2 + ||f_perp||_2^2
    let f_l2_sq = f.norm_l2().powi(2);
    let comp_l2_sq = p_v.norm_l2().powi(2) + f_perp.norm_l2().powi(2);
    assert!(
        (f_l2_sq - comp_l2_sq).abs() < 1e-10,
        "Teorema de Pitágoras em Hilbert violado: diff = {}",
        (f_l2_sq - comp_l2_sq).abs()
    );
}

#[test]
fn test_cauchy_fixed_point_contraction() {
    let n = 256;
    let init = vec![100.0; n];
    let mut frame = BanachFrame::new(&init, None);

    // Operador contraente T(x) = 0.5 * x + 2.0 com ponto fixo x* = 4.0
    let iters = frame.cauchy_fixed_point(
        |input, output| {
            for i in 0..input.len() {
                output[i] = 0.5 * input[i] + 2.0;
            }
        },
        0.5,
        1e-8,
        100,
    );

    assert!(iters < 100);
    for &val in frame.data.as_slice() {
        assert!(
            (val - 4.0).abs() < 1e-7,
            "Ponto fixo não convergiu para 4.0: val = {}",
            val
        );
    }
}

#[test]
fn test_baremetal_native_fma_and_bitboard_scanning() {
    let num_rows = 128;
    let mut col0 = vec![10.0f64; num_rows];
    let col1 = vec![2.0f64; num_rows];

    // Cria bitmap com linhas pares válidas e ímpares nulas
    let validity = vec![0x55u8; num_rows / 8]; // 01010101 em binário
    let accrual = 5.0f64;

    let desc = NativeBatchDescriptor {
        num_rows,
        values_col0_ptr: col0.as_mut_ptr(),
        validity_col0_ptr: validity.as_ptr(),
        values_col1_ptr: col1.as_ptr(),
        utf8_offsets_ptr: std::ptr::null(),
        utf8_data_ptr: std::ptr::null(),
        vptree_mmap_ptr: std::ptr::null(),
        vptree_mmap_len: 0,
    };

    let processed = unsafe { execute_native_vector_fma(&desc, accrual) };
    assert_eq!(processed, 64, "Deveriam ser processadas exatamente 64 linhas válidas");

    // Verifica resultados: linhas pares receberam col0 * col1 + accrual = 10 * 2 + 5 = 25
    for (i, val) in col0.iter().enumerate().take(num_rows) {
        if i % 2 == 0 {
            assert_eq!(*val, 25.0);
        } else {
            assert_eq!(*val, 10.0); // intacto
        }
    }
}

#[test]
fn test_l2_arena_zero_alloc_execution() {
    let result = process_with_l2_arena(|slice| {
        assert_eq!(slice.len(), BATCH_CANONICAL_DIM);
        // Preenche e computa sem alocação no heap
        slice[0] = 42.0;
        slice[BATCH_CANONICAL_DIM - 1] = 84.0;
        slice[0] + slice[BATCH_CANONICAL_DIM - 1]
    });

    assert_eq!(result, 126.0);
}

#[test]
fn test_spark_df_to_banach_frame_bridge() {
    use kineplex_df::SparkSession;

    let spark = SparkSession::builder().master("local[4]").get_or_create();
    let df = spark.range(0, 1024, 1, 4);

    let banach_frame = df.to_banach_frame("id").expect("Falha ao converter DataFrame para BanachFrame");
    assert_eq!(banach_frame.len(), 1024);

    let l2 = banach_frame.norm_l2();
    assert!(l2 > 0.0);

    let dirichlet_e = banach_frame.dirichlet_energy();
    assert!(dirichlet_e > 0.0);
}
