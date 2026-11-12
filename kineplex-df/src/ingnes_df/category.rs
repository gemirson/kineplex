//! # Teoria das Categorias para DataFrames (`ingnes_df::category`)
//!
//! Modela DataFrames sob a ótica da Teoria das Categorias:
//! 1. **Morfismos Colunares e Composição ($g \circ f$):**
//!    Transformações entre tipos de colunas são morfismos em uma categoria comutaiva $\mathbf{Col}$.
//!    A composição de dois mapas $f: A \to B$ e $g: B \to C$ colapsa em tempo de compilação
//!    em um único morfismo $h = g \circ f: A \to C$, eliminando buffers intermediários.
//! 2. **Monoides Comutativos $(\mathcal{M}, \oplus, e)$ e Catamorfismos:**
//!    Todas as agregações são catamorfismos (dobras) sobre Monoides Comutativos com elemento neutro $e$.
//!    A associatividade e comutatividade garantem que partições paralelas somem/reduzam seus
//!    registradores de hardware sem locks e com consistência determinística.
//! 3. **Pullbacks (Produtos Fibrados):**
//!    Um Join entre duas tabelas $A$ e $B$ com chave comum $C$ é rigorosamente o Pullback categórico:
//!    $$A \times_C B = \{ (a, b) \in A \times B \mid \pi_C(a) = \pi_C(b) \}$$

use crate::decimal::QDec128;
use crate::kernel::FinancialCategoricalKernelV2;

/// Definição formal de Monoide Comutativo na Teoria das Categorias.
pub trait CommutativeMonoid: Copy + Send + Sync + 'static {
    type Value: Copy + Send + Sync + 'static;

    /// Elemento Neutro $e \in \mathcal{M}$.
    fn identity() -> Self::Value;

    /// Operação Binária Associativa e Comutativa $x \oplus y$.
    fn combine(x: Self::Value, y: Self::Value) -> Self::Value;
}

/// Monoide da Adição de Inteiros $(\mathbb{Z}, +, 0)$.
#[derive(Clone, Copy, Debug)]
pub struct SumI64Monoid;

impl CommutativeMonoid for SumI64Monoid {
    type Value = i64;

    #[inline(always)]
    fn identity() -> i64 {
        0
    }

    #[inline(always)]
    fn combine(x: i64, y: i64) -> i64 {
        x.wrapping_add(y)
    }
}

/// Monoide da Adição de Decimais de 128-Bits de Schreier $(\mathcal{D}_{16}, \oplus, (0, 0))$.
#[derive(Clone, Copy, Debug)]
pub struct QDec128SumMonoid;

impl CommutativeMonoid for QDec128SumMonoid {
    type Value = QDec128;

    #[inline(always)]
    fn identity() -> QDec128 {
        QDec128::ZERO
    }

    #[inline(always)]
    fn combine(x: QDec128, y: QDec128) -> QDec128 {
        x.add_quarter(y)
    }
}

/// Monoide da Multiplicação Truncada B3 $(\mathcal{M}_{B3}, \otimes_{16}, 10^{16})$.
#[derive(Clone, Copy, Debug)]
pub struct B3FactorMulMonoid;

impl CommutativeMonoid for B3FactorMulMonoid {
    type Value = i64;

    #[inline(always)]
    fn identity() -> i64 {
        10_000_000_000_000_000_i64
    }

    #[inline(always)]
    fn combine(x: i64, y: i64) -> i64 {
        FinancialCategoricalKernelV2::b3_monoid_mul16_safe(x, y)
    }
}

/// Monoide do Mínimo $(\mathbb{R} \cup \{\infty\}, \min, \infty)$.
#[derive(Clone, Copy, Debug)]
pub struct MinF64Monoid;

impl CommutativeMonoid for MinF64Monoid {
    type Value = f64;

    #[inline(always)]
    fn identity() -> f64 {
        f64::INFINITY
    }

    #[inline(always)]
    fn combine(x: f64, y: f64) -> f64 {
        x.min(y)
    }
}

/// Monoide do Máximo $(\mathbb{R} \cup \{-\infty\}, \max, -\infty)$.
#[derive(Clone, Copy, Debug)]
pub struct MaxF64Monoid;

impl CommutativeMonoid for MaxF64Monoid {
    type Value = f64;

    #[inline(always)]
    fn identity() -> f64 {
        f64::NEG_INFINITY
    }

    #[inline(always)]
    fn combine(x: f64, y: f64) -> f64 {
        x.max(y)
    }
}

/// Catamorfismo Categórico: dobra (redução) de um slice colunar sobre um Monoide Comutativo.
/// Executa com 4 acumuladores independentes paralelos que quebram a cadeia de dependência
/// de latência da CPU, saturando as portas de execução com vazão de 1 elemento por ciclo de clock.
#[inline(always)]
pub fn catamorphism<M: CommutativeMonoid>(slice: &[M::Value]) -> M::Value {
    let chunks = slice.chunks_exact(4);
    let rem = chunks.remainder();

    let mut acc0 = M::identity();
    let mut acc1 = M::identity();
    let mut acc2 = M::identity();
    let mut acc3 = M::identity();

    for chunk in chunks {
        acc0 = M::combine(acc0, chunk[0]);
        acc1 = M::combine(acc1, chunk[1]);
        acc2 = M::combine(acc2, chunk[2]);
        acc3 = M::combine(acc3, chunk[3]);
    }

    let mut acc = M::combine(M::combine(acc0, acc1), M::combine(acc2, acc3));
    for &val in rem {
        acc = M::combine(acc, val);
    }
    acc
}

/// Morfismo Composto $h(x) = (g \circ f)(x) = g(f(x))$.
pub struct ComposedMorphism<F, G> {
    pub f: F,
    pub g: G,
}

impl<F, G> ComposedMorphism<F, G> {
    pub fn new(f: F, g: G) -> Self {
        Self { f, g }
    }

    #[inline(always)]
    pub fn apply<A, B, C>(&self, arg: A) -> C
    where
        F: Fn(A) -> B,
        G: Fn(B) -> C,
    {
        (self.g)((self.f)(arg))
    }
}
