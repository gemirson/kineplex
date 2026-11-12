//! # kineplex-df: Columnar DataFrame + Spark SQL-style engine
//!
//! Engine colunar em Rust puro (zero dependências externas) para o KinePlex:
//! DSL SQL fluente, pipeline Catalyst/Whole-Stage e Kernel Financeiro B3/BACEN $O(1)$.
//!
//! Adaptado de **Quimera-Ingnes** (`quimera-ingnes` v3.1.0, Quimera Labs / DAC-UNICAMP)
//! — apenas o subconjunto `sql` + `ingnes_df` e suas dependências internas.
//!
//! ## Principais Módulos:
//! - [`memory`]: Gerenciamento de memória com alinhamento estrito a linhas de cache de 64 bytes (`AlignedBuffer<T>`),
//!   eliminando *false sharing* e *split-line loads*.
//! - [`decimal`]: Aritmética Radicial Decimal de 128-bits (`QDec128`, `QFactor64`) via 2-Cociclo de Schreier
//!   com branchless carry e produto no monoide B3 (Zero Heap Allocation, zero `BigDecimal`/`BigInt`).
//! - [`kernel`]: Kernel Bare-Metal B3/BACEN $O(1)$ com Disjoint Sparse Table (DST), Momentos de Taylor
//!   de Kahan-Babuška-Neumaier e Fibração de Grothendieck co-localizados em registradores AVX2.
//! - [`catalyst`]: Sistema colunar de tipos, expressões, operadores e Whole-Stage Pipeline Codegen.
//! - [`sql`]: DSL fluente para DataFrames Spark SQL (`select`, `with_column`, `with_b3_factor`, `filter`, `agg`).
//! - [`ingnes_df`]: DataFrame colunar com catamorfismos sobre monoides, hashing de Fibonacci, CRT e DAG zero-copy.
//! - [`banach`]: Análise Funcional de Sobolev $W^{1,2}$, Energia de Dirichlet, Haar Wavelets e Transporte Ótimo $W_p$ no `BanachFrame` ($N = 65.536$).
//! - [`vptree`]: Poda topológica métrica zero-copy via Desigualdade Triangular com nós compactos de 32 bytes (`FlatVpNode`).
//! - [`baremetal`]: Kernel vetorial SIMD com FMA, Bitboard BLSR/TZCNT e execução desacoplada sem runtimes externos.

pub mod banach;
pub mod baremetal;
pub mod catalyst;
pub mod concurrency;
pub mod decimal;
pub mod ingnes_df;
pub mod kernel;
pub mod memory;
pub mod sql;
pub mod vptree;

pub use banach::{
    BanachFrame, FunctionalRegime, FunctionalSignature, BATCH_CANONICAL_DIM,
};
pub use baremetal::{
    execute_native_vector_fma, fast_normalize_scale16, process_with_l2_arena,
    NativeBatchDescriptor,
};
pub use catalyst::{ColumnVector, ColumnarBatch, DataType, Expression, ScalarValue};
pub use concurrency::{global_thread_pool, MicroThreadPool};
pub use decimal::{
    add_exact, mul_exact, sub_exact, QDec128, QDec128ColumnarVector, QFactor64,
    QuimeraCategoricalDecimal,
};
pub use ingnes_df::{
    catamorphism, ChineseRemainderEngine, ColumnBitmask, CommutativeMonoid, FlowGraph,
    IngnesColumn, IngnesDataFrame, JoinType, MaxF64Monoid, MinF64Monoid, QDec128SumMonoid,
    SumI64Monoid,
};
pub use kernel::{
    EvaluatedFactor, FinancialCategoricalKernelV2, FinancialContract, FlatBroadcastEngine,
    GrothendieckCell, QuimeraNativeMath, TaylorJet4, ANNIV_DAYS, DAY_MASK, DAY_SHIFT, DST_LEVELS,
    IDX_CDI_PCT, IDX_CDI_PURE, IDX_IGPM, IDX_IPCA, MAX_DAYS, MAX_MONTHS,
};
pub use memory::{with_thread_arena, AlignedBuffer, VectorArena, CACHE_LINE_BYTES, L1D_OPTIMAL_BATCH_SIZE};
pub use sql::{
    avg, col, count, lit, round, sum, when, Column, DataFrame, Row, SparkSession,
};
pub use vptree::{
    BanachMetric, FlatVpNode, TopologicalVpIndex, VpIndexHeader, VpSearchStats, NULL_NODE, VP_MAGIC,
};
