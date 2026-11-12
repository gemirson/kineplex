//! # Catalyst Optimizer & Vectorized Whole-Stage Engine (`catalyst`)
//!
//! Reimplementação bare-metal em Rust com **Simpatia Mecânica de Hardware** do
//! Apache Spark Catalyst & Tungsten Engine:
//!
//! 1. **Substituição do `UnsafeRow` por `ColumnarBatch` SoA:**
//!    Em vez de armazenar tuplas desnormalizadas de 8 bytes na memória, organizamos
//!    os dados em vetores de colunas puras (`AlignedBuffer<T>`) alinhados a **64 bytes**
//!    (Cache Line Boundary), garantindo vetorização SIMD AVX2/AVX-512 e eliminando
//!    completamente o *false sharing* e *split-line loads*.
//!
//! 2. **Vectorized Micro-Batching (L1d Optimal Window):**
//!    Processamento em lotes colunares de 2048 elementos (`L1D_OPTIMAL_BATCH_SIZE`),
//!    garantindo que todas as colunas de um estágio residam 100% na Cache L1d (32 KiB)
//!    do núcleo da CPU durante a execução dos kernels.
//!
//! 3. **Whole-Stage Pipeline Fusion (Zero JVM Janino Codegen Overhead):**
//!    Compilação estática de pipelines `Scan -> Project -> Filter -> Aggregate`
//!    com monomorfização via Rust `inline(always)`, eliminando *virtual call dispatch*
//!    e permitindo que o compilador LLVM faça unroll de loops e emita instruções FMA e CMOV.
//!
//! 4. **Expressão Nativa `QuimeraFactorExpression`:**
//!    Executada diretamente no loop colunar de cada partição, com acesso direto via ponteiro
//!    às tabelas de broadcast O(1) da Disjoint Sparse Table e superfícies de Grothendieck.

use crate::decimal::QDec128;
use crate::kernel::{FlatBroadcastEngine, QuimeraNativeMath, DAY_MASK};
use crate::memory::AlignedBuffer;
use std::collections::HashSet;
use std::sync::Arc;

pub mod algebra;
pub mod fusion;
pub mod optimizer;

pub use fusion::*;
pub use optimizer::*;

/// Tipos de Dados suportados pelo sistema de tipos colunar Catalyst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Byte,
    Short,
    Int,
    Long,
    Double,
    Boolean,
    Decimal128,
}

/// Coluna de dados contígua com alinhamento estrito de 64 bytes.
#[derive(Clone, Debug)]
pub enum ColumnVector {
    Byte(AlignedBuffer<i8>),
    Short(AlignedBuffer<i16>),
    Int(AlignedBuffer<i32>),
    Long(AlignedBuffer<i64>),
    Double(AlignedBuffer<f64>),
    Boolean(AlignedBuffer<bool>),
    Decimal128(AlignedBuffer<QDec128>),
}

impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Byte(b) => b.len(),
            ColumnVector::Short(b) => b.len(),
            ColumnVector::Int(b) => b.len(),
            ColumnVector::Long(b) => b.len(),
            ColumnVector::Double(b) => b.len(),
            ColumnVector::Boolean(b) => b.len(),
            ColumnVector::Decimal128(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnVector::Byte(_) => DataType::Byte,
            ColumnVector::Short(_) => DataType::Short,
            ColumnVector::Int(_) => DataType::Int,
            ColumnVector::Long(_) => DataType::Long,
            ColumnVector::Double(_) => DataType::Double,
            ColumnVector::Boolean(_) => DataType::Boolean,
            ColumnVector::Decimal128(_) => DataType::Decimal128,
        }
    }

    #[inline(always)]
    pub fn as_i64_slice(&self) -> &[i64] {
        match self {
            ColumnVector::Long(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Long"),
        }
    }

    #[inline(always)]
    pub fn as_i16_slice(&self) -> &[i16] {
        match self {
            ColumnVector::Short(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Short"),
        }
    }

    #[inline(always)]
    pub fn as_i8_slice(&self) -> &[i8] {
        match self {
            ColumnVector::Byte(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Byte"),
        }
    }

    #[inline(always)]
    pub fn as_f64_slice(&self) -> &[f64] {
        match self {
            ColumnVector::Double(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Double"),
        }
    }

    #[inline(always)]
    pub fn as_qdec128_slice(&self) -> &[QDec128] {
        match self {
            ColumnVector::Decimal128(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Decimal128"),
        }
    }

    #[inline(always)]
    pub fn as_int_slice(&self) -> &[i32] {
        match self {
            ColumnVector::Int(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Int"),
        }
    }

    #[inline(always)]
    pub fn as_bool_slice(&self) -> &[bool] {
        match self {
            ColumnVector::Boolean(b) => b.as_slice(),
            _ => panic!("Esperado ColumnVector::Boolean"),
        }
    }

    #[inline(always)]
    pub fn get_i64_key(&self, idx: usize) -> i64 {
        match self {
            ColumnVector::Long(b) => b[idx],
            ColumnVector::Int(b) => b[idx] as i64,
            ColumnVector::Short(b) => b[idx] as i64,
            ColumnVector::Byte(b) => b[idx] as i64,
            ColumnVector::Double(b) => b[idx] as i64,
            ColumnVector::Boolean(b) => b[idx] as i64,
            ColumnVector::Decimal128(b) => b[idx].q,
        }
    }

    /// Projeta índices (Gather) com suporte a null branchless em Outer Joins.
    pub fn gather_indices(&self, indices: &[Option<usize>]) -> ColumnVector {
        let n = indices.len();
        match self {
            ColumnVector::Long(b) => {
                let mut out = AlignedBuffer::from_elem(0_i64, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Long(out)
            }
            ColumnVector::Int(b) => {
                let mut out = AlignedBuffer::from_elem(0_i32, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Int(out)
            }
            ColumnVector::Short(b) => {
                let mut out = AlignedBuffer::from_elem(0_i16, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Short(out)
            }
            ColumnVector::Byte(b) => {
                let mut out = AlignedBuffer::from_elem(0_i8, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Byte(out)
            }
            ColumnVector::Double(b) => {
                let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Double(out)
            }
            ColumnVector::Boolean(b) => {
                let mut out = AlignedBuffer::from_elem(false, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Boolean(out)
            }
            ColumnVector::Decimal128(b) => {
                let mut out = AlignedBuffer::from_elem(QDec128::ZERO, n);
                for (i, opt) in indices.iter().enumerate() {
                    if let Some(src) = opt {
                        out[i] = b[*src];
                    }
                }
                ColumnVector::Decimal128(out)
            }
        }
    }

    /// Concatenação contígua com alinhamento a 64 bytes.
    pub fn concat(&self, other: &ColumnVector) -> ColumnVector {
        match (self, other) {
            (ColumnVector::Long(b1), ColumnVector::Long(b2)) => {
                let mut buf = AlignedBuffer::from_elem(0_i64, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Long(buf)
            }
            (ColumnVector::Int(b1), ColumnVector::Int(b2)) => {
                let mut buf = AlignedBuffer::from_elem(0_i32, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Int(buf)
            }
            (ColumnVector::Short(b1), ColumnVector::Short(b2)) => {
                let mut buf = AlignedBuffer::from_elem(0_i16, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Short(buf)
            }
            (ColumnVector::Byte(b1), ColumnVector::Byte(b2)) => {
                let mut buf = AlignedBuffer::from_elem(0_i8, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Byte(buf)
            }
            (ColumnVector::Double(b1), ColumnVector::Double(b2)) => {
                let mut buf = AlignedBuffer::from_elem(0.0_f64, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Double(buf)
            }
            (ColumnVector::Boolean(b1), ColumnVector::Boolean(b2)) => {
                let mut buf = AlignedBuffer::from_elem(false, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Boolean(buf)
            }
            (ColumnVector::Decimal128(b1), ColumnVector::Decimal128(b2)) => {
                let mut buf = AlignedBuffer::from_elem(QDec128::ZERO, b1.len() + b2.len());
                buf.as_mut_slice()[..b1.len()].copy_from_slice(b1.as_slice());
                buf.as_mut_slice()[b1.len()..].copy_from_slice(b2.as_slice());
                ColumnVector::Decimal128(buf)
            }
            _ => panic!("Tipos incompativeis para concatenacao de ColumnVector"),
        }
    }

    pub fn zeros_like(&self, len: usize) -> ColumnVector {
        match self {
            ColumnVector::Long(_) => ColumnVector::Long(AlignedBuffer::from_elem(0_i64, len)),
            ColumnVector::Int(_) => ColumnVector::Int(AlignedBuffer::from_elem(0_i32, len)),
            ColumnVector::Short(_) => ColumnVector::Short(AlignedBuffer::from_elem(0_i16, len)),
            ColumnVector::Byte(_) => ColumnVector::Byte(AlignedBuffer::from_elem(0_i8, len)),
            ColumnVector::Double(_) => ColumnVector::Double(AlignedBuffer::from_elem(0.0_f64, len)),
            ColumnVector::Boolean(_) => ColumnVector::Boolean(AlignedBuffer::from_elem(false, len)),
            ColumnVector::Decimal128(_) => {
                ColumnVector::Decimal128(AlignedBuffer::from_elem(QDec128::ZERO, len))
            }
        }
    }
}

/// Lote Colunar de Dados (SoA) que substitui as matrizes de `UnsafeRow` do Apache Spark Tungsten.
#[derive(Clone, Debug)]
pub struct ColumnarBatch {
    pub names: Vec<String>,
    pub columns: Vec<ColumnVector>,
    pub num_rows: usize,
}

impl ColumnarBatch {
    pub fn new(names: Vec<String>, columns: Vec<ColumnVector>) -> Self {
        let num_rows = if columns.is_empty() {
            0
        } else {
            columns[0].len()
        };
        for col in &columns {
            assert_eq!(
                col.len(),
                num_rows,
                "Todas as colunas do ColumnarBatch devem ter o mesmo comprimento"
            );
        }
        Self {
            names,
            columns,
            num_rows,
        }
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }

    pub fn column(&self, name: &str) -> &ColumnVector {
        let idx = self.column_index(name).unwrap_or_else(|| {
            panic!(
                "Coluna '{}' nao encontrada no ColumnarBatch. Colunas disponiveis: {:?}",
                name, self.names
            )
        });
        &self.columns[idx]
    }

    /// Concatenação de dois lotes colunares com o mesmo esquema.
    pub fn concat(&self, other: &ColumnarBatch) -> ColumnarBatch {
        assert_eq!(self.names, other.names, "Esquemas devem coincidir para concat");
        let mut cols = Vec::with_capacity(self.columns.len());
        for (c1, c2) in self.columns.iter().zip(&other.columns) {
            cols.push(c1.concat(c2));
        }
        ColumnarBatch::new(self.names.clone(), cols)
    }

    /// Concatena uma fatia de lotes colunares com o mesmo esquema.
    pub fn concat_batches(batches: &[ColumnarBatch]) -> ColumnarBatch {
        if batches.is_empty() {
            return ColumnarBatch::new(Vec::new(), Vec::new());
        }
        if batches.len() == 1 {
            return batches[0].clone();
        }
        let mut accum = batches[0].clone();
        for b in &batches[1..] {
            accum = accum.concat(b);
        }
        accum
    }

    /// Deduplicação de linhas em L1d via hashing de Fibonacci.
    pub fn deduplicate(&self) -> ColumnarBatch {
        if self.num_rows <= 1 || self.columns.is_empty() {
            return self.clone();
        }
        let hash_bits = 16;
        let num_buckets = 1usize << hash_bits;
        let mut head = vec![u32::MAX; num_buckets];
        let mut next = vec![u32::MAX; self.num_rows];
        let mut kept_indices = Vec::with_capacity(self.num_rows);

        for row in 0..self.num_rows {
            let mut h = 0_u64;
            for (i, col) in self.columns.iter().enumerate() {
                let val = col.get_i64_key(row) as u64;
                let prime = match i % 3 {
                    0 => crate::ingnes_df::number_theory::FIBONACCI_HASH_PRIME_64,
                    1 => 0x9e3779b97f4a7c15,
                    _ => 0xbf58476d1ce4e5b9,
                };
                h = h.wrapping_add(val.wrapping_mul(prime));
                h = (h << 13) | (h >> 51);
            }
            let bucket = crate::ingnes_df::fibonacci_hash_64(h as i64, hash_bits);
            let mut curr = head[bucket];
            let mut found = false;
            while curr != u32::MAX {
                let r_prev = curr as usize;
                let mut all_match = true;
                for col in &self.columns {
                    if col.get_i64_key(r_prev) != col.get_i64_key(row) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    found = true;
                    break;
                }
                curr = next[r_prev];
            }
            if !found {
                kept_indices.push(row);
                next[row] = head[bucket];
                head[bucket] = row as u32;
            }
        }

        let optional_indices: Vec<Option<usize>> = kept_indices.into_iter().map(Some).collect();
        let mut cols = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            cols.push(col.gather_indices(&optional_indices));
        }
        ColumnarBatch::new(self.names.clone(), cols)
    }

    /// Junção Bare-Metal de alta velocidade via Crivo L1d e Hashing de Fibonacci.
    pub fn join(
        &self,
        right: &ColumnarBatch,
        left_key: &str,
        right_key: &str,
        join_type: crate::ingnes_df::JoinType,
    ) -> ColumnarBatch {
        let left_col = self.column(left_key);
        let right_col = right.column(right_key);
        let n_r = right.num_rows;
        let n_l = self.num_rows;

        let sieve_bits = 18;
        let mut sieve = crate::ingnes_df::ColumnBitmask::all_zeros(1 << sieve_bits);

        let hash_bits = 16;
        let num_buckets = 1usize << hash_bits;
        let mut head = vec![u32::MAX; num_buckets];
        let mut next = vec![u32::MAX; n_r];

        for r_idx in 0..n_r {
            let k = right_col.get_i64_key(r_idx);
            let s_idx = crate::ingnes_df::fibonacci_hash_64(k, sieve_bits);
            sieve.set(s_idx, true);

            let bucket = crate::ingnes_df::fibonacci_hash_64(k, hash_bits);
            next[r_idx] = head[bucket];
            head[bucket] = r_idx as u32;
        }

        let mut matched_left = Vec::with_capacity(n_l.max(n_r));
        let mut matched_right = Vec::with_capacity(n_l.max(n_r));
        let mut right_visited = if join_type == crate::ingnes_df::JoinType::RightOuter {
            crate::ingnes_df::ColumnBitmask::all_zeros(n_r)
        } else {
            crate::ingnes_df::ColumnBitmask::all_zeros(0)
        };

        for l_idx in 0..n_l {
            let k = left_col.get_i64_key(l_idx);
            let s_idx = crate::ingnes_df::fibonacci_hash_64(k, sieve_bits);

            if !sieve.get(s_idx) {
                match join_type {
                    crate::ingnes_df::JoinType::LeftAnti => {
                        matched_left.push(Some(l_idx));
                        matched_right.push(None);
                    }
                    crate::ingnes_df::JoinType::LeftOuter => {
                        matched_left.push(Some(l_idx));
                        matched_right.push(None);
                    }
                    crate::ingnes_df::JoinType::Inner
                    | crate::ingnes_df::JoinType::LeftSemi
                    | crate::ingnes_df::JoinType::RightOuter => {}
                }
                continue;
            }

            let bucket = crate::ingnes_df::fibonacci_hash_64(k, hash_bits);
            let mut curr = head[bucket];
            let mut match_found = false;

            while curr != u32::MAX {
                let r_idx = curr as usize;
                if right_col.get_i64_key(r_idx) == k {
                    match_found = true;
                    match join_type {
                        crate::ingnes_df::JoinType::LeftSemi => {
                            matched_left.push(Some(l_idx));
                            matched_right.push(None);
                            break;
                        }
                        crate::ingnes_df::JoinType::LeftAnti => {
                            break;
                        }
                        crate::ingnes_df::JoinType::Inner | crate::ingnes_df::JoinType::LeftOuter => {
                            matched_left.push(Some(l_idx));
                            matched_right.push(Some(r_idx));
                        }
                        crate::ingnes_df::JoinType::RightOuter => {
                            matched_left.push(Some(l_idx));
                            matched_right.push(Some(r_idx));
                            right_visited.set(r_idx, true);
                        }
                    }
                }
                curr = next[r_idx];
            }

            if !match_found {
                match join_type {
                    crate::ingnes_df::JoinType::LeftOuter | crate::ingnes_df::JoinType::LeftAnti => {
                        matched_left.push(Some(l_idx));
                        matched_right.push(None);
                    }
                    _ => {}
                }
            }
        }

        if join_type == crate::ingnes_df::JoinType::RightOuter {
            for r_idx in 0..n_r {
                if !right_visited.get(r_idx) {
                    matched_left.push(None);
                    matched_right.push(Some(r_idx));
                }
            }
        }

        let mut out_names = Vec::new();
        let mut out_cols = Vec::new();

        if join_type == crate::ingnes_df::JoinType::LeftSemi
            || join_type == crate::ingnes_df::JoinType::LeftAnti
        {
            for (idx, name) in self.names.iter().enumerate() {
                out_names.push(name.clone());
                out_cols.push(self.columns[idx].gather_indices(&matched_left));
            }
        } else {
            for (idx, name) in self.names.iter().enumerate() {
                out_names.push(name.clone());
                out_cols.push(self.columns[idx].gather_indices(&matched_left));
            }

            for (idx, rname) in right.names.iter().enumerate() {
                if rname == right_key {
                    continue;
                }
                out_names.push(format!("right_{}", rname));
                out_cols.push(right.columns[idx].gather_indices(&matched_right));
            }
        }

        ColumnarBatch::new(out_names, out_cols)
    }
}

/// Valor Escalar para Expressões do Catalyst.
#[derive(Clone, Debug)]
pub enum ScalarValue {
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Double(f64),
    Boolean(bool),
    Decimal128(QDec128),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarValue::Byte(a), ScalarValue::Byte(b)) => a == b,
            (ScalarValue::Short(a), ScalarValue::Short(b)) => a == b,
            (ScalarValue::Int(a), ScalarValue::Int(b)) => a == b,
            (ScalarValue::Long(a), ScalarValue::Long(b)) => a == b,
            (ScalarValue::Double(a), ScalarValue::Double(b)) => a.to_bits() == b.to_bits(),
            (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a == b,
            (ScalarValue::Decimal128(a), ScalarValue::Decimal128(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Byte(b) => b.hash(state),
            ScalarValue::Short(s) => s.hash(state),
            ScalarValue::Int(i) => i.hash(state),
            ScalarValue::Long(l) => l.hash(state),
            ScalarValue::Double(d) => d.to_bits().hash(state),
            ScalarValue::Boolean(b) => b.hash(state),
            ScalarValue::Decimal128(d) => {
                d.q.hash(state);
                d.r.hash(state);
            }
        }
    }
}

impl ScalarValue {
    #[inline(always)]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ScalarValue::Byte(v) => Some(*v as i64),
            ScalarValue::Short(v) => Some(*v as i64),
            ScalarValue::Int(v) => Some(*v as i64),
            ScalarValue::Long(v) => Some(*v),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Double(v) => Some(*v),
            ScalarValue::Byte(v) => Some(*v as f64),
            ScalarValue::Short(v) => Some(*v as f64),
            ScalarValue::Int(v) => Some(*v as f64),
            ScalarValue::Long(v) => Some(*v as f64),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        match self {
            ScalarValue::Byte(v) => *v == 0,
            ScalarValue::Short(v) => *v == 0,
            ScalarValue::Int(v) => *v == 0,
            ScalarValue::Long(v) => *v == 0,
            ScalarValue::Double(v) => *v == 0.0,
            ScalarValue::Boolean(b) => !*b,
            ScalarValue::Decimal128(d) => d.q == 0 && d.r == 0,
        }
    }

    #[inline(always)]
    pub fn is_one(&self) -> bool {
        match self {
            ScalarValue::Byte(v) => *v == 1,
            ScalarValue::Short(v) => *v == 1,
            ScalarValue::Int(v) => *v == 1,
            ScalarValue::Long(v) => *v == 1,
            ScalarValue::Double(v) => *v == 1.0,
            ScalarValue::Boolean(b) => *b,
            ScalarValue::Decimal128(d) => d.q == 1 && d.r == 0,
        }
    }

    /// Se o valor for uma potência de dois positiva (2, 4, 8, 16, 32, ...), retorna o expoente k (ex: 2^k).
    #[inline(always)]
    pub fn is_power_of_two(&self) -> Option<u32> {
        if let Some(val) = self.as_i64() {
            if val > 0 && (val & (val - 1)) == 0 {
                return Some(val.trailing_zeros());
            }
        }
        None
    }
}

/// Expressões Nativas do Catalyst Engine.
#[derive(Clone, Debug)]
pub enum Expression {
    Literal(ScalarValue),
    Column(String),
    Add(Box<Expression>, Box<Expression>),
    Sub(Box<Expression>, Box<Expression>),
    Mul(Box<Expression>, Box<Expression>),
    Div(Box<Expression>, Box<Expression>),
    Mod(Box<Expression>, Box<Expression>),
    BitAnd(Box<Expression>, Box<Expression>),
    BitOr(Box<Expression>, Box<Expression>),
    BitXor(Box<Expression>, Box<Expression>),
    BitShiftLeft(Box<Expression>, Box<Expression>),
    BitShiftRight(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Round(Box<Expression>, usize),
    WhenOtherwise {
        condition: Box<Expression>,
        if_true: Box<Expression>,
        if_false: Box<Expression>,
    },
    Eq(Box<Expression>, Box<Expression>),
    Neq(Box<Expression>, Box<Expression>),
    Lt(Box<Expression>, Box<Expression>),
    Lte(Box<Expression>, Box<Expression>),
    Gt(Box<Expression>, Box<Expression>),
    Gte(Box<Expression>, Box<Expression>),
    Cast {
        expr: Box<Expression>,
        target_type: DataType,
    },
    /// Expressão Nativa Quimera B3/BACEN Catalyst
    QuimeraFactor {
        start_day: Box<Expression>,
        end_day: Box<Expression>,
        indexer_code: Box<Expression>,
        anniv_day: Box<Expression>,
        pct_indexer: Box<Expression>,
        spread_aa: Box<Expression>,
        broadcast_engine: Arc<FlatBroadcastEngine>,
    },
}

impl PartialEq for Expression {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Expression::Literal(a), Expression::Literal(b)) => a == b,
            (Expression::Column(a), Expression::Column(b)) => a == b,
            (Expression::Add(a1, a2), Expression::Add(b1, b2))
            | (Expression::Sub(a1, a2), Expression::Sub(b1, b2))
            | (Expression::Mul(a1, a2), Expression::Mul(b1, b2))
            | (Expression::Div(a1, a2), Expression::Div(b1, b2))
            | (Expression::Mod(a1, a2), Expression::Mod(b1, b2))
            | (Expression::BitAnd(a1, a2), Expression::BitAnd(b1, b2))
            | (Expression::BitOr(a1, a2), Expression::BitOr(b1, b2))
            | (Expression::BitXor(a1, a2), Expression::BitXor(b1, b2))
            | (Expression::BitShiftLeft(a1, a2), Expression::BitShiftLeft(b1, b2))
            | (Expression::BitShiftRight(a1, a2), Expression::BitShiftRight(b1, b2))
            | (Expression::And(a1, a2), Expression::And(b1, b2))
            | (Expression::Or(a1, a2), Expression::Or(b1, b2))
            | (Expression::Eq(a1, a2), Expression::Eq(b1, b2))
            | (Expression::Neq(a1, a2), Expression::Neq(b1, b2))
            | (Expression::Lt(a1, a2), Expression::Lt(b1, b2))
            | (Expression::Lte(a1, a2), Expression::Lte(b1, b2))
            | (Expression::Gt(a1, a2), Expression::Gt(b1, b2))
            | (Expression::Gte(a1, a2), Expression::Gte(b1, b2)) => a1 == b1 && a2 == b2,
            (Expression::Not(a), Expression::Not(b)) => a == b,
            (Expression::Round(a, da), Expression::Round(b, db)) => da == db && a == b,
            (
                Expression::WhenOtherwise {
                    condition: c1,
                    if_true: t1,
                    if_false: f1,
                },
                Expression::WhenOtherwise {
                    condition: c2,
                    if_true: t2,
                    if_false: f2,
                },
            ) => c1 == c2 && t1 == t2 && f1 == f2,
            (
                Expression::Cast {
                    expr: e1,
                    target_type: t1,
                },
                Expression::Cast {
                    expr: e2,
                    target_type: t2,
                },
            ) => t1 == t2 && e1 == e2,
            (
                Expression::QuimeraFactor {
                    start_day: s1,
                    end_day: e1,
                    indexer_code: i1,
                    anniv_day: a1,
                    pct_indexer: p1,
                    spread_aa: sp1,
                    broadcast_engine: b1,
                },
                Expression::QuimeraFactor {
                    start_day: s2,
                    end_day: e2,
                    indexer_code: i2,
                    anniv_day: a2,
                    pct_indexer: p2,
                    spread_aa: sp2,
                    broadcast_engine: b2,
                },
            ) => {
                s1 == s2
                    && e1 == e2
                    && i1 == i2
                    && a1 == a2
                    && p1 == p2
                    && sp1 == sp2
                    && Arc::ptr_eq(b1, b2)
            }
            _ => false,
        }
    }
}

impl Eq for Expression {}

impl std::hash::Hash for Expression {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Expression::Literal(lit) => lit.hash(state),
            Expression::Column(col) => col.hash(state),
            Expression::Add(l, r)
            | Expression::Sub(l, r)
            | Expression::Mul(l, r)
            | Expression::Div(l, r)
            | Expression::Mod(l, r)
            | Expression::BitAnd(l, r)
            | Expression::BitOr(l, r)
            | Expression::BitXor(l, r)
            | Expression::BitShiftLeft(l, r)
            | Expression::BitShiftRight(l, r)
            | Expression::And(l, r)
            | Expression::Or(l, r)
            | Expression::Eq(l, r)
            | Expression::Neq(l, r)
            | Expression::Lt(l, r)
            | Expression::Lte(l, r)
            | Expression::Gt(l, r)
            | Expression::Gte(l, r) => {
                l.hash(state);
                r.hash(state);
            }
            Expression::Not(e) => e.hash(state),
            Expression::Round(e, d) => {
                e.hash(state);
                d.hash(state);
            }
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => {
                condition.hash(state);
                if_true.hash(state);
                if_false.hash(state);
            }
            Expression::Cast { expr, target_type } => {
                expr.hash(state);
                (*target_type as u8).hash(state);
            }
            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                broadcast_engine,
            } => {
                start_day.hash(state);
                end_day.hash(state);
                indexer_code.hash(state);
                anniv_day.hash(state);
                pct_indexer.hash(state);
                spread_aa.hash(state);
                (Arc::as_ptr(broadcast_engine) as usize).hash(state);
            }
        }
    }
}

impl Expression {
    /// Coleta todas as colunas referenciadas nesta expressão.
    pub fn collect_columns(&self, out: &mut HashSet<String>) {
        match self {
            Expression::Literal(_) => {}
            Expression::Column(name) => {
                out.insert(name.clone());
            }
            Expression::Add(l, r)
            | Expression::Sub(l, r)
            | Expression::Mul(l, r)
            | Expression::Div(l, r)
            | Expression::Mod(l, r)
            | Expression::BitAnd(l, r)
            | Expression::BitOr(l, r)
            | Expression::BitXor(l, r)
            | Expression::BitShiftLeft(l, r)
            | Expression::BitShiftRight(l, r)
            | Expression::And(l, r)
            | Expression::Or(l, r)
            | Expression::Eq(l, r)
            | Expression::Neq(l, r)
            | Expression::Lt(l, r)
            | Expression::Lte(l, r)
            | Expression::Gt(l, r)
            | Expression::Gte(l, r) => {
                l.collect_columns(out);
                r.collect_columns(out);
            }
            Expression::Not(e) | Expression::Round(e, _) | Expression::Cast { expr: e, .. } => {
                e.collect_columns(out);
            }
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => {
                condition.collect_columns(out);
                if_true.collect_columns(out);
                if_false.collect_columns(out);
            }
            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                ..
            } => {
                start_day.collect_columns(out);
                end_day.collect_columns(out);
                indexer_code.collect_columns(out);
                anniv_day.collect_columns(out);
                pct_indexer.collect_columns(out);
                spread_aa.collect_columns(out);
            }
        }
    }

    /// Substitui recursivamente um sub-árvore de expressão por outra.
    pub fn replace_expression(&self, target: &Expression, replacement: &Expression) -> Expression {
        if self == target {
            return replacement.clone();
        }
        match self {
            Expression::Literal(_) | Expression::Column(_) => self.clone(),
            Expression::Add(l, r) => Expression::Add(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Sub(l, r) => Expression::Sub(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Mul(l, r) => Expression::Mul(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Div(l, r) => Expression::Div(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Mod(l, r) => Expression::Mod(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::BitAnd(l, r) => Expression::BitAnd(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::BitOr(l, r) => Expression::BitOr(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::BitXor(l, r) => Expression::BitXor(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::BitShiftLeft(l, r) => Expression::BitShiftLeft(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::BitShiftRight(l, r) => Expression::BitShiftRight(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::And(l, r) => Expression::And(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Or(l, r) => Expression::Or(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Not(e) => {
                Expression::Not(Box::new(e.replace_expression(target, replacement)))
            }
            Expression::Round(e, d) => {
                Expression::Round(Box::new(e.replace_expression(target, replacement)), *d)
            }
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => Expression::WhenOtherwise {
                condition: Box::new(condition.replace_expression(target, replacement)),
                if_true: Box::new(if_true.replace_expression(target, replacement)),
                if_false: Box::new(if_false.replace_expression(target, replacement)),
            },
            Expression::Eq(l, r) => Expression::Eq(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Neq(l, r) => Expression::Neq(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Lt(l, r) => Expression::Lt(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Lte(l, r) => Expression::Lte(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Gt(l, r) => Expression::Gt(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Gte(l, r) => Expression::Gte(
                Box::new(l.replace_expression(target, replacement)),
                Box::new(r.replace_expression(target, replacement)),
            ),
            Expression::Cast { expr, target_type } => Expression::Cast {
                expr: Box::new(expr.replace_expression(target, replacement)),
                target_type: *target_type,
            },
            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                broadcast_engine,
            } => Expression::QuimeraFactor {
                start_day: Box::new(start_day.replace_expression(target, replacement)),
                end_day: Box::new(end_day.replace_expression(target, replacement)),
                indexer_code: Box::new(indexer_code.replace_expression(target, replacement)),
                anniv_day: Box::new(anniv_day.replace_expression(target, replacement)),
                pct_indexer: Box::new(pct_indexer.replace_expression(target, replacement)),
                spread_aa: Box::new(spread_aa.replace_expression(target, replacement)),
                broadcast_engine: broadcast_engine.clone(),
            },
        }
    }

    /// Retorna a profundidade da árvore da expressão.
    pub fn depth(&self) -> usize {
        match self {
            Expression::Literal(_) | Expression::Column(_) => 1,
            Expression::Not(e) | Expression::Round(e, _) | Expression::Cast { expr: e, .. } => {
                1 + e.depth()
            }
            Expression::Add(l, r)
            | Expression::Sub(l, r)
            | Expression::Mul(l, r)
            | Expression::Div(l, r)
            | Expression::Mod(l, r)
            | Expression::BitAnd(l, r)
            | Expression::BitOr(l, r)
            | Expression::BitXor(l, r)
            | Expression::BitShiftLeft(l, r)
            | Expression::BitShiftRight(l, r)
            | Expression::And(l, r)
            | Expression::Or(l, r)
            | Expression::Eq(l, r)
            | Expression::Neq(l, r)
            | Expression::Lt(l, r)
            | Expression::Lte(l, r)
            | Expression::Gt(l, r)
            | Expression::Gte(l, r) => 1 + l.depth().max(r.depth()),
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => 1 + condition.depth().max(if_true.depth()).max(if_false.depth()),
            Expression::QuimeraFactor { .. } => 2,
        }
    }

    /// Retorna o número de nós na árvore da expressão.
    pub fn size(&self) -> usize {
        match self {
            Expression::Literal(_) | Expression::Column(_) => 1,
            Expression::Not(e) | Expression::Round(e, _) | Expression::Cast { expr: e, .. } => {
                1 + e.size()
            }
            Expression::Add(l, r)
            | Expression::Sub(l, r)
            | Expression::Mul(l, r)
            | Expression::Div(l, r)
            | Expression::Mod(l, r)
            | Expression::BitAnd(l, r)
            | Expression::BitOr(l, r)
            | Expression::BitXor(l, r)
            | Expression::BitShiftLeft(l, r)
            | Expression::BitShiftRight(l, r)
            | Expression::And(l, r)
            | Expression::Or(l, r)
            | Expression::Eq(l, r)
            | Expression::Neq(l, r)
            | Expression::Lt(l, r)
            | Expression::Lte(l, r)
            | Expression::Gt(l, r)
            | Expression::Gte(l, r) => 1 + l.size() + r.size(),
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => 1 + condition.size() + if_true.size() + if_false.size(),
            Expression::QuimeraFactor { .. } => 7,
        }
    }
}

impl Expression {
    /// Avalia a expressão sobre um `ColumnarBatch`, retornando um novo `ColumnVector` contíguo.
    pub fn eval(&self, batch: &ColumnarBatch) -> ColumnVector {
        let n = batch.num_rows;
        match self {
            Expression::Literal(val) => match val {
                ScalarValue::Byte(v) => ColumnVector::Byte(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Short(v) => ColumnVector::Short(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Int(v) => ColumnVector::Int(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Long(v) => ColumnVector::Long(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Double(v) => ColumnVector::Double(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Boolean(v) => ColumnVector::Boolean(AlignedBuffer::from_elem(*v, n)),
                ScalarValue::Decimal128(v) => ColumnVector::Decimal128(AlignedBuffer::from_elem(*v, n)),
            },
            Expression::Column(name) => batch.column(name).clone(),
            Expression::Add(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = lv[i] + rv[i];
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] + rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] + (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = (lv[i] as i64) + rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    _ => panic!("Add nao suportado para os tipos: {:?} + {:?}", l.data_type(), r.data_type()),
                }
            }
            Expression::Sub(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = lv[i] - rv[i];
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] - rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    _ => panic!("Sub nao suportado para os tipos fornecidos"),
                }
            }
            Expression::Mul(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = lv[i] * rv[i];
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] * rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] * (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = (lv[i] as i64) * rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Double(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = lv[i] * (rv[i] as f64);
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Double(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = (lv[i] as f64) * rv[i];
                        }
                        ColumnVector::Double(out)
                    }
                    _ => panic!("Mul nao suportado para os tipos: {:?} * {:?}", l.data_type(), r.data_type()),
                }
            }
            Expression::Div(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = lv[i] / rv[i];
                        }
                        ColumnVector::Double(out)
                    }
                    _ => panic!("Div nao suportado"),
                }
            }
            Expression::Mod(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] % rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] % (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    _ => panic!("Mod suportado apenas para tipos inteiros"),
                }
            }
            Expression::BitAnd(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] & rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] & (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i32, n);
                        for i in 0..n {
                            out[i] = lv[i] & rv[i];
                        }
                        ColumnVector::Int(out)
                    }
                    _ => panic!("BitAnd suportado apenas para tipos inteiros"),
                }
            }
            Expression::BitOr(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] | rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] | (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i32, n);
                        for i in 0..n {
                            out[i] = lv[i] | rv[i];
                        }
                        ColumnVector::Int(out)
                    }
                    _ => panic!("BitOr suportado apenas para tipos inteiros"),
                }
            }
            Expression::BitXor(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] ^ rv[i];
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] ^ (rv[i] as i64);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i32, n);
                        for i in 0..n {
                            out[i] = lv[i] ^ rv[i];
                        }
                        ColumnVector::Int(out)
                    }
                    _ => panic!("BitXor suportado apenas para tipos inteiros"),
                }
            }
            Expression::BitShiftLeft(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] << (rv[i] as u32);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] << (rv[i] as u32);
                        }
                        ColumnVector::Long(out)
                    }
                    _ => panic!("BitShiftLeft suportado para inteiros"),
                }
            }
            Expression::BitShiftRight(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] >> (rv[i] as u32);
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = lv[i] >> (rv[i] as u32);
                        }
                        ColumnVector::Long(out)
                    }
                    _ => panic!("BitShiftRight suportado para inteiros"),
                }
            }
            Expression::And(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Boolean(lv), ColumnVector::Boolean(rv)) => {
                        let mut out = AlignedBuffer::from_elem(false, n);
                        for i in 0..n {
                            out[i] = lv[i] && rv[i];
                        }
                        ColumnVector::Boolean(out)
                    }
                    _ => panic!("And requer operandos Boolean"),
                }
            }
            Expression::Or(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                match (&l, &r) {
                    (ColumnVector::Boolean(lv), ColumnVector::Boolean(rv)) => {
                        let mut out = AlignedBuffer::from_elem(false, n);
                        for i in 0..n {
                            out[i] = lv[i] || rv[i];
                        }
                        ColumnVector::Boolean(out)
                    }
                    _ => panic!("Or requer operandos Boolean"),
                }
            }
            Expression::Not(inner) => {
                let v = inner.eval(batch);
                match &v {
                    ColumnVector::Boolean(bv) => {
                        let mut out = AlignedBuffer::from_elem(false, n);
                        for i in 0..n {
                            out[i] = !bv[i];
                        }
                        ColumnVector::Boolean(out)
                    }
                    _ => panic!("Not requer operando Boolean"),
                }
            }
            Expression::Round(expr, decimals) => {
                let col = expr.eval(batch);
                match col {
                    ColumnVector::Double(v) => {
                        let factor = 10_f64.powi(*decimals as i32);
                        let inv_factor = 1.0 / factor;
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = (v[i] * factor).round() * inv_factor;
                        }
                        ColumnVector::Double(out)
                    }
                    _ => panic!("Round suportado apenas para Double"),
                }
            }
            Expression::WhenOtherwise {
                condition,
                if_true,
                if_false,
            } => {
                let cond_col = condition.eval(batch);
                let cond_slice = match &cond_col {
                    ColumnVector::Boolean(b) => b.as_slice(),
                    _ => panic!("Condition deve ser Boolean"),
                };
                let t_col = if_true.eval(batch);
                let f_col = if_false.eval(batch);

                match (&t_col, &f_col) {
                    (ColumnVector::Double(tv), ColumnVector::Double(fv)) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Long(tv), ColumnVector::Long(fv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i64, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Long(out)
                    }
                    (ColumnVector::Short(tv), ColumnVector::Short(fv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i16, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Short(out)
                    }
                    (ColumnVector::Byte(tv), ColumnVector::Byte(fv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i8, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Byte(out)
                    }
                    (ColumnVector::Int(tv), ColumnVector::Int(fv)) => {
                        let mut out = AlignedBuffer::from_elem(0_i32, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Int(out)
                    }
                    (ColumnVector::Boolean(tv), ColumnVector::Boolean(fv)) => {
                        let mut out = AlignedBuffer::from_elem(false, n);
                        for i in 0..n {
                            out[i] = if cond_slice[i] { tv[i] } else { fv[i] };
                        }
                        ColumnVector::Boolean(out)
                    }
                    _ => panic!("Tipos incompativeis em WhenOtherwise"),
                }
            }
            Expression::Eq(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] == rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] == (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) == rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] == rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] == rv[i];
                        }
                    }
                    _ => panic!("Eq nao suportado para estes tipos"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Neq(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] != rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] != (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) != rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] != rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] != rv[i];
                        }
                    }
                    _ => panic!("Neq nao suportado"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Lt(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] < rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] < (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) < rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] < rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] < rv[i];
                        }
                    }
                    _ => panic!("Lt nao suportado"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Lte(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] <= rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] <= (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) <= rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] <= rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] <= rv[i];
                        }
                    }
                    _ => panic!("Lte nao suportado"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Gt(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] > rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] > (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) > rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] > rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] > rv[i];
                        }
                    }
                    _ => panic!("Gt nao suportado"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Gte(lhs, rhs) => {
                let l = lhs.eval(batch);
                let r = rhs.eval(batch);
                let mut out = AlignedBuffer::from_elem(false, n);
                match (&l, &r) {
                    (ColumnVector::Long(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] >= rv[i];
                        }
                    }
                    (ColumnVector::Long(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] >= (rv[i] as i64);
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Long(rv)) => {
                        for i in 0..n {
                            out[i] = (lv[i] as i64) >= rv[i];
                        }
                    }
                    (ColumnVector::Int(lv), ColumnVector::Int(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] >= rv[i];
                        }
                    }
                    (ColumnVector::Double(lv), ColumnVector::Double(rv)) => {
                        for i in 0..n {
                            out[i] = lv[i] >= rv[i];
                        }
                    }
                    _ => panic!("Gte nao suportado"),
                }
                ColumnVector::Boolean(out)
            }
            Expression::Cast { expr, target_type } => {
                let src = expr.eval(batch);
                match (src, target_type) {
                    (ColumnVector::Long(v), DataType::Short) => {
                        let mut out = AlignedBuffer::from_elem(0_i16, n);
                        for i in 0..n {
                            out[i] = v[i] as i16;
                        }
                        ColumnVector::Short(out)
                    }
                    (ColumnVector::Long(v), DataType::Byte) => {
                        let mut out = AlignedBuffer::from_elem(0_i8, n);
                        for i in 0..n {
                            out[i] = v[i] as i8;
                        }
                        ColumnVector::Byte(out)
                    }
                    (ColumnVector::Long(v), DataType::Double) => {
                        let mut out = AlignedBuffer::from_elem(0.0_f64, n);
                        for i in 0..n {
                            out[i] = v[i] as f64;
                        }
                        ColumnVector::Double(out)
                    }
                    (ColumnVector::Int(v), DataType::Short) => {
                        let mut out = AlignedBuffer::from_elem(0_i16, n);
                        for i in 0..n {
                            out[i] = v[i] as i16;
                        }
                        ColumnVector::Short(out)
                    }
                    (ColumnVector::Int(v), DataType::Byte) => {
                        let mut out = AlignedBuffer::from_elem(0_i8, n);
                        for i in 0..n {
                            out[i] = v[i] as i8;
                        }
                        ColumnVector::Byte(out)
                    }
                    (other, target) => {
                        if other.data_type() == *target {
                            other
                        } else {
                            panic!("Conversao de tipo {:?} para {:?} nao suportada", other.data_type(), target)
                        }
                    }
                }
            }
            Expression::QuimeraFactor {
                start_day,
                end_day,
                indexer_code,
                anniv_day,
                pct_indexer,
                spread_aa,
                broadcast_engine,
            } => {
                let u_col = start_day.eval(batch);
                let v_col = end_day.eval(batch);
                let idx_col = indexer_code.eval(batch);
                let anniv_col = anniv_day.eval(batch);
                let pct_col = pct_indexer.eval(batch);
                let spread_col = spread_aa.eval(batch);

                let mut out = AlignedBuffer::from_elem(1.0_f64, n);
                let eng = broadcast_engine.as_ref();

                let u_slice = match &u_col {
                    ColumnVector::Short(s) => s.as_slice(),
                    _ => panic!("start_day deve ser Short"),
                };
                let v_slice = match &v_col {
                    ColumnVector::Short(s) => s.as_slice(),
                    _ => panic!("end_day deve ser Short"),
                };
                let idx_slice = match &idx_col {
                    ColumnVector::Byte(b) => b.as_slice(),
                    _ => panic!("indexer_code deve ser Byte"),
                };
                let anniv_slice = match &anniv_col {
                    ColumnVector::Byte(b) => b.as_slice(),
                    _ => panic!("anniv_day deve ser Byte"),
                };
                let pct_slice = match &pct_col {
                    ColumnVector::Double(d) => d.as_slice(),
                    _ => panic!("pct_indexer deve ser Double"),
                };
                let spread_slice = match &spread_col {
                    ColumnVector::Double(d) => d.as_slice(),
                    _ => panic!("spread_aa deve ser Double"),
                };

                // Hot loop vetorizado executando o Whole-Stage Codegen em Rust nativo
                for i in 0..n {
                    let u = (u_slice[i] as usize) & DAY_MASK;
                    let v = (v_slice[i] as usize) & DAY_MASK;
                    if v > u {
                        out[i] = QuimeraNativeMath::eval_row(
                            eng,
                            u,
                            v,
                            idx_slice[i],
                            anniv_slice[i],
                            pct_slice[i],
                            spread_slice[i],
                        );
                    }
                }
                ColumnVector::Double(out)
            }
        }
    }
}
