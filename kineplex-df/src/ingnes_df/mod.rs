//! # Módulo IngnesDataFrame (`ingnes_df`)
//!
//! Replicador do conceito de DataFrame orientado por:
//! - **Simpatia Mecânica:** Buffer colunar contíguo alinhado a 64 bytes (`AlignedBuffer<T>`), micro-lotes L1d.
//! - **Teoria das Categorias:** Esquemas como Objetos, Functors, Morfismos e Catamorfismos sobre Monoides Comutativos.
//! - **Teoria dos Números:** Fibonacci Hashing $O(1)$, Chinese Remainder Theorem (CRT) e Crivos Aritméticos.
//! - **Grafos:** Grafo Direcionado Acíclico (DAG) de fluxo de memória zero-copy.

pub mod category;
pub mod dataframe;
pub mod graph;
pub mod number_theory;

pub use category::{
    catamorphism, B3FactorMulMonoid, CommutativeMonoid, ComposedMorphism, MaxF64Monoid,
    MinF64Monoid, QDec128SumMonoid, SumI64Monoid,
};
pub use dataframe::{IngnesColumn, IngnesDataFrame, JoinType};
pub use graph::{FlowGraph, NodeId, NodeKind};
pub use number_theory::{
    extended_gcd, fibonacci_hash_64, mod_inverse, ChineseRemainderEngine, ColumnBitmask,
    COPRIME_M1, COPRIME_M2, FIBONACCI_HASH_PRIME_64,
};
