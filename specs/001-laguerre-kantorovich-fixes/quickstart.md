# Quickstart: Distribuidor de Laguerre-Kantorovich (MK-EMD)

**Feature**: `001-laguerre-kantorovich-fixes` (FT-097)  
**Target**: Desenvolvedores e Engenheiros de Dados do KinePlex  

---

## 1. Exemplo de Uso Prático

```rust
use std::sync::Arc;
use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use object_store::memory::InMemory;
use kineplex_core::laguerre::{KantorovichDistributionConfig, LaguerreOptimalDistributor};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Configurar o ObjectStore (InMemory para testes/local, S3 em produção)
    let store = Arc::new(InMemory::new());
    let bucket = "kineplex-lakehouse".to_string();
    let prefix = "tables/sensor_events".to_string();

    // 2. Definir baricentros iniciais da variedade (K = 4 células de Laguerre)
    let seeds = vec![
        [0.1, 0.2, 0.1, 0.5],
        [0.8, 0.9, 0.7, 0.9],
        [0.4, 0.5, 0.3, 0.4],
        [0.2, 0.7, 0.6, 0.8],
    ];

    // 3. Inicializar hiperparâmetros de Monge-Kantorovich
    let config = KantorovichDistributionConfig::default();
    let mut distributor = LaguerreOptimalDistributor::new(store, bucket, prefix, seeds, config);

    // 4. Criar schema e lote Arrow de exemplo
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, false),
    ]));
    let values = vec![42.0; 8192];
    let array: ArrayRef = Arc::new(Float64Array::from(values));
    let batch = RecordBatch::try_new(schema, vec![array])?;

    // 5. Roteamento e distribuição com assinatura topológica Banach L^p
    let signature = [0.12, 0.18, 0.11, 0.48];
    distributor.route_and_distribute(batch, signature).await?;

    // 6. Fechamento terminal com Coalescência de Ward-Steiner (Zero-Residue)
    let sealed_files = distributor.finalize_optimal_distribution().await?;
    println!("Arquivos Parquet selados sem small files: {}", sealed_files.len());

    Ok(())
}
```

---

## 2. Como Executar os Testes de Validação

Para executar a suíte completa de verificação matemática e integração de Laguerre-Kantorovich:

```bash
cd /home/james/Documentos/KinePlex/kineplex
cargo test --test laguerre_distributor_tests -- --nocapture
```
