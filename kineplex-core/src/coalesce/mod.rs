//! # Operador KineCoalesce ($\mathcal{C}$)
//!
//! Unificação de três vertentes da Matemática Pura e Aplicada para o processamento colunar:
//!
//! 1. **[`sheaf`]:** `SheafCoalesce` - Fusão de $M$ colunas com nulos via Teoria de Feixes,
//!    Homotopia de Calibre nas interseções, Partições da Unidade e Extensão Harmônica de Dirichlet ($\Delta_d f = 0$).
//! 2. **[`smoluchowski`]:** `SmoluchowskiCoalesce` - Coagulação métrica de arquivos Parquet e partições físicas
//!    via equação cinética de Smoluchowski, minimizando a dilatação de caixas mortas no Apache Iceberg.
//! 3. **[`quotient`]:** `QuotientCoalesce` - Condensação de rajadas de micro-eventos no espaço quociente topológico $X / \sim$,
//!    colapsando variações dentro da bola $\overline{B}_\varepsilon$ em classes de equivalência baricêntricas.

pub mod quotient;
pub mod sheaf;
pub mod smoluchowski;

pub use quotient::quotient_topological_coalesce;
pub use sheaf::{bitwise_sheaf_coalesce_pair, sheaf_harmonic_coalesce};
pub use smoluchowski::{smoluchowski_partition_coalesce, PartitionMetricDescriptor};
