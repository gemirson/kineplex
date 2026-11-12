//! Distribuição Ótima de Laguerre-Kantorovich (MK-EMD Anti-Small-Files)
//!
//! Este módulo implementa o particionamento dinâmico e o roteamento de lotes
//! Arrow para arquivos Parquet no Object Storage / S3 / Iceberg sem geração
//! de arquivos pequenos (Small Files Problem).
//!
//! # Fundamentos Matemáticos:
//! - **Transporte Ótimo Semi-Discreto de Monge-Kantorovich**: Minimização da funcional de energia livre de transporte-massa.
//! - **Diagramas de Potência de Laguerre (CCVT)**: Células convexas orientadas por potenciais duais \psi_k.
//! - **Dinâmica de Subgradiente Hidrostático / Log-Sinkhorn**: Rebalanceamento contínuo da atração/repulsão gravitacional para convergir em M^* = 128 MB.
//! - **Estimador Entrópico de Shannon-Markov ZSTD**: Medição em O(1) da taxa condicional de Markov para estimar tamanho comprimido.
//! - **Promoção Preguiçosa & Coalescência de Ward-Steiner EOF (Zero-Residue)**: Absorção estrita de resíduos < M_min.

pub mod buf_writer;
pub mod cell;
pub mod config;
pub mod distributor;
pub mod entropy;
pub mod sink;

pub use buf_writer::BufWriter;
pub use cell::{LaguerreCell, SemiDiscreteLaguerreCell};
pub use config::{KantorovichDistributionConfig, OptimalDistributionConfig};
pub use distributor::LaguerreOptimalDistributor;
pub use entropy::{estimate_markov_zstd_mass, estimate_zstd_mass};
pub use sink::{
    default_writer_properties, ActiveWriterHandle, FileParquetSink, InMemoryParquetSink,
    LaguerreParquetSink,
};
