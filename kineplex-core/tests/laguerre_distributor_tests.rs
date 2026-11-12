//! Testes da Distribuição Ótima de Laguerre-Kantorovich (MK-EMD Anti-Small-Files)

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use kineplex_core::laguerre::{
    estimate_markov_zstd_mass, estimate_zstd_mass, KantorovichDistributionConfig,
    LaguerreOptimalDistributor,
};
use object_store::memory::InMemory;
use std::sync::Arc;

fn make_test_batch(values: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("metric", DataType::Float64, false)]));
    let array = Arc::new(Float64Array::from(values)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array]).expect("Falha ao criar RecordBatch")
}

#[test]
fn test_markov_zstd_mass_estimation_properties() {
    // 1. RLE contínuo
    let constant_batch = make_test_batch(vec![42.0; 2048]);
    let est_const = estimate_markov_zstd_mass(&constant_batch);
    let raw_const = constant_batch.get_array_memory_size() as u64;
    let ratio_const = (est_const as f64) / (raw_const as f64);
    assert!(ratio_const >= 0.05 && ratio_const <= 0.20, "Razão RLE deve ser próxima do piso 0.05: {}", ratio_const);

    // 2. Ruído de alta entropia
    let mut seed: u64 = 987654321;
    let random_values: Vec<f64> = (0..2048)
        .map(|_| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            f64::from_bits(seed)
        })
        .collect();
    let random_batch = make_test_batch(random_values);
    let est_rand = estimate_markov_zstd_mass(&random_batch);
    let raw_rand = random_batch.get_array_memory_size() as u64;
    let ratio_rand = (est_rand as f64) / (raw_rand as f64);
    assert!(ratio_rand >= 0.25, "Razão de alta entropia deve ser significativamente superior a RLE: {}", ratio_rand);
    assert!(ratio_rand > ratio_const * 2.0, "Ruído deve ter massa estimada > 2x a de RLE constante");

    // 3. Lote vazio
    let empty_schema = Arc::new(Schema::new(vec![Field::new("metric", DataType::Float64, false)]));
    let empty_batch = RecordBatch::try_new(empty_schema, vec![Arc::new(Float64Array::from(Vec::<f64>::new()))]).unwrap();
    assert_eq!(estimate_markov_zstd_mass(&empty_batch), 0);
    assert_eq!(estimate_zstd_mass(&empty_batch), 0);
}

#[tokio::test]
async fn test_welford_manifold_scale_and_dimensionless_cost() {
    let store = Arc::new(InMemory::new());
    let config = KantorovichDistributionConfig::default();
    let centroids = vec![[0.0, 0.0, 0.0, 0.0], [10.0, 10.0, 10.0, 10.0]];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/tests".to_string(),
        centroids,
        config,
    );

    // Avalia o custo de Laguerre antes e depois de atualizar a escala de Welford
    let sig = [8.0, 8.0, 8.0, 8.0];
    let cost0 = distributor.dimensionless_laguerre_cost(&sig, &distributor.cells()[0]);
    let cost1 = distributor.dimensionless_laguerre_cost(&sig, &distributor.cells()[1]);
    assert!(cost1 < cost0, "Célula 1 deve ter menor custo inicial para assinatura próxima: {} vs {}", cost1, cost0);

    // Atualiza escala de Welford e garante estabilidade numérica
    for _ in 0..100 {
        distributor.update_manifold_scale(&[1.0, 2.0, 3.0, 4.0]);
    }
    assert!(distributor.dimensionless_laguerre_cost(&sig, &distributor.cells()[0]) > 0.0);
}

#[tokio::test]
async fn test_sinkhorn_zero_sum_hyperplane_projection() {
    let store = Arc::new(InMemory::new());
    let config = KantorovichDistributionConfig::default();
    let centroids = vec![
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/tests".to_string(),
        centroids,
        config,
    );

    // Roteia lotes com desequilíbrio para alterar massas
    let batch = make_test_batch(vec![1.0; 500]);
    distributor.route_and_distribute(batch.clone(), [1.0, 0.0, 0.0, 0.0]).await.unwrap();
    distributor.route_and_distribute(batch.clone(), [1.0, 0.0, 0.0, 0.0]).await.unwrap();

    // Executa múltiplos passos de Log-Sinkhorn
    for _ in 0..10 {
        distributor.update_sinkhorn_kantorovich_potentials();
        let sum_psi: f64 = distributor.cells().iter().map(|c| c.kantorovich_psi).sum();
        assert!(
            sum_psi.abs() < 1e-12,
            "Invariante de soma zero deve ser preservada: sum = {}",
            sum_psi
        );
    }
}

#[tokio::test]
async fn test_lazy_promotion_and_arena_staged() {
    let store = Arc::new(InMemory::new());
    // M_min = 20_000 bytes, target = 100_000 bytes
    let config = KantorovichDistributionConfig::test_scale(20_000, 100_000, 200_000);

    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/tests".to_string(),
        vec![[0.0, 0.0, 0.0, 0.0]],
        config,
    );

    // Lote pequeno: tamanho estimado < M_min
    let small_batch = make_test_batch(vec![1.23; 50]);
    distributor.route_and_distribute(small_batch, [0.0, 0.0, 0.0, 0.0]).await.unwrap();

    // Deve estar na arena staged e NÃO deve ter aberto writer S3
    assert_eq!(distributor.cells()[0].staged_count(), 1);
    assert!(!distributor.cells()[0].is_promoted());
    assert_eq!(distributor.sealed_datafiles().len(), 0);
}

#[tokio::test]
async fn test_zero_residue_quantum_eof_consolidation() {
    let store = Arc::new(InMemory::new());
    // M_min = 50_000 bytes. Enviamos apenas batches pequenos (< M_min) divididos em 3 células.
    let config = KantorovichDistributionConfig::test_scale(50_000, 100_000, 200_000);

    let centroids = vec![
        [1.0, 1.0, 1.0, 1.0],
        [5.0, 5.0, 5.0, 5.0],
        [9.0, 9.0, 9.0, 9.0],
    ];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/zero_residue".to_string(),
        centroids,
        config,
    );

    distributor.route_and_distribute(make_test_batch(vec![1.0; 20]), [1.0, 1.0, 1.0, 1.0]).await.unwrap();
    distributor.route_and_distribute(make_test_batch(vec![5.0; 20]), [5.0, 5.0, 5.0, 5.0]).await.unwrap();
    distributor.route_and_distribute(make_test_batch(vec![9.0; 20]), [9.0, 9.0, 9.0, 9.0]).await.unwrap();

    // Nenhuma célula atingiu M_min, logo todas retêm lotes na arena
    for cell in distributor.cells() {
        assert!(!cell.is_promoted());
    }

    // Fechamento Quântico: Consolida todos os resíduos em exatamente 1 único arquivo!
    let files = distributor.finalize_optimal_distribution().await.unwrap();
    assert_eq!(files.len(), 1, "Deve consolidar em exatamente 1 único arquivo Parquet!");
    assert!(files[0].0.contains("consolidated_ward/kinehaus-coalesced-"));
}

#[tokio::test]
async fn test_zero_residue_quantum_eof_absorption_into_active_cell() {
    let store = Arc::new(InMemory::new());
    // M_min = 500 bytes, target = 500_000 bytes
    let config = KantorovichDistributionConfig::test_scale(500, 500_000, 1_000_000);

    let centroids = vec![[0.0, 0.0, 0.0, 0.0], [100.0, 100.0, 100.0, 100.0]];
    let mut distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/absorption".to_string(),
        centroids,
        config,
    );

    // Alimenta a Célula 0 com dados suficientes para promover (passar de 500 bytes)
    for _ in 0..15 {
        distributor
            .route_and_distribute(make_test_batch(vec![0.1; 100]), [0.0, 0.0, 0.0, 0.0])
            .await
            .unwrap();
    }
    assert!(distributor.cells()[0].is_promoted(), "Célula 0 deve ter sido promovida");

    // Envia um lote isolado na Célula 1 (< 500 bytes, retido em RAM)
    distributor
        .route_and_distribute(make_test_batch(vec![99.9; 10]), [100.0, 100.0, 100.0, 100.0])
        .await
        .unwrap();
    assert!(!distributor.cells()[1].is_promoted(), "Célula 1 não deve estar promovida");

    // Fechamento Quântico: A Célula 1 é absorvida pela Célula 0 ativa via Ward-Steiner!
    let files = distributor.finalize_optimal_distribution().await.unwrap();
    // Apenas 1 arquivo deve ter sido gerado (o da Célula 0 que absorveu os órfãos)
    assert_eq!(files.len(), 1);
    assert!(files[0].0.contains("laguerre_cell_00"));
}

#[test]
fn test_banach_signature_extraction() {
    let store = Arc::new(InMemory::new());
    let config = KantorovichDistributionConfig::default();
    let distributor = LaguerreOptimalDistributor::new(
        store,
        "test-bucket".to_string(),
        "lakehouse/sig".to_string(),
        vec![[0.0, 0.0, 0.0, 0.0]],
        config,
    );

    let batch = make_test_batch(vec![2.0, -4.0, 6.0, -8.0]);
    let sig = distributor.extract_banach_signature(&batch);

    // L1 normalizada = (2 + 4 + 6 + 8) / 4 = 5.0
    assert!((sig[0] - 5.0).abs() < 1e-4);
    // Linf = 8.0
    assert!((sig[3] - 8.0).abs() < 1e-4);
    assert!(sig[1] > 0.0);
    assert!(sig[2] > 0.0);
}
