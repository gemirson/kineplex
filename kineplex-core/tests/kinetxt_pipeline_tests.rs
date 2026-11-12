use kineplex_core::kinetxt::driver::{KineTxtDriver, TxtColumnSpec};
use kineplex_core::laguerre::config::KantorovichDistributionConfig;
use kineplex_core::laguerre::distributor::LaguerreOptimalDistributor;
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::test]
async fn test_kinetxt_to_laguerre_pipeline() {
    // 1. Gera buffer texto delimitado
    let mut raw_txt = Vec::new();
    for i in 0..1000 {
        let line = format!("{};\"ITEM_{:04}\";{:.2}\n", i, i, (i as f64) * 1.5);
        raw_txt.extend_from_slice(line.as_bytes());
    }

    let specs = vec![
        TxtColumnSpec::Int64 {
            name: "id".to_string(),
            col_index: 0,
            fixed_range: None,
        },
        TxtColumnSpec::Utf8 {
            name: "descricao".to_string(),
            col_index: 1,
            fixed_range: None,
        },
        TxtColumnSpec::Float64 {
            name: "preco".to_string(),
            col_index: 2,
            fixed_range: None,
            scale: 1.0,
        },
    ];

    let driver = KineTxtDriver::new(b';', specs);
    let batch = driver.parse_delimited_chunk_to_arrow(&raw_txt).unwrap();

    assert_eq!(batch.num_rows(), 1000);
    assert_eq!(batch.num_columns(), 3);

    // 2. Conecta ao Distribuidor de Laguerre
    let store = Arc::new(InMemory::new());
    let seeds = vec![[0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0, 1.0]];
    let mut distributor = LaguerreOptimalDistributor::new(
        store.clone(),
        "bucket-text".to_string(),
        "parquet-output".to_string(),
        seeds,
        KantorovichDistributionConfig::default(),
    );

    let mass = LaguerreOptimalDistributor::estimate_markov_zstd_mass(&batch);
    assert!(mass > 0);

    // Roteia o batch no espaço de Banach
    distributor
        .route_and_distribute(batch, [0.1, 0.2, 0.3, 0.4])
        .await
        .unwrap();

    // Finaliza distribuição (Zero-Residue EOF)
    let sealed = distributor.finalize_optimal_distribution().await.unwrap();
    assert_eq!(sealed.len(), 1, "Job diminuto deve consolidar em 1 arquivo único");

    println!(
        "Pipeline KineTxt -> Laguerre finalizou com sucesso: arquivo gerado {}",
        sealed[0].0
    );
}
