//! # Quimera Algebraic Categorical Decimal (`QFactor64` & `QDec128`)
//!
//! Substituto Bare-Metal para `BigInt` e `BigDecimal` com **Zero Heap Allocation** e
//! **Simpatia Mecânica de Hardware (Registradores RDX:RAX + BMI2 MULX + Branchless CMOV)**.
//!
//! ## Formalização via Teoria das Categorias e Extensões de Schreier
//! Modelamos o espaço de números decimais de precisão estendida como uma Extensão Central
//! de Monoides Comutativos (Sequência Exata Curta de Schreier) na base radicial $B = 10^{16}$:
//!
//! $$0 \longrightarrow (\mathcal{M}_B, +) \longrightarrow (\mathcal{E}_{128}, \oplus, \otimes) \longrightarrow (\mathbb{Z}, +) \longrightarrow 0$$
//!
//! - **Fibra Fracionária (Mantissa):** $r \in \mathcal{M}_B = [0, 10^{16} - 1]$ (54 bits $< 63$ bits).
//! - **Base Inteira (Quociente):** $q \in \mathbb{Z} = [-2^{63}, 2^{63} - 1]$ (64 bits).
//! - **Invariante Canônico de Fibra:** Todo elemento $X \in \mathcal{D}_{16}$ é representado
//!   unicamente pelo par $(q, r)$ em dois registradores de 64 bits (`i64, i64`), satisfazendo
//!   $X = q \cdot 10^{16} + r$ com $0 \le r < 10^{16}$ (mesmo para números negativos).

use crate::kernel::FinancialCategoricalKernelV2;
use crate::memory::AlignedBuffer;
use std::fmt;
use std::ops::{Add, Sub};

pub const BASE_16: i64 = 10_000_000_000_000_000; // 10^16 (Fibra Fracionária B3)
pub const BASE_8: i64 = 100_000_000; // 10^8  (Sub-fibra Isomórfica para Produto)
pub const SCALE_8_TO_16: i64 = 100_000_000; // 10^(16 - 8)
pub const SCALE_2_TO_16: i64 = 100_000_000_000_000; // 10^(16 - 2) para Centavos BACEN
pub const SCALE_6_TO_16: i64 = 10_000_000_000; // 10^(16 - 6) para VNA NTN-B

/// Constantes de Montgomery / Granlund-Montgomery para divisão invariante por 10^8 e 10^16 em 2 ciclos:
pub const MONTGOMERY_M_BASE_8: u64 = 0xabcc77118461d;
pub const MONTGOMERY_S_BASE_8: u32 = 14;

pub const MONTGOMERY_M_BASE_16: u64 = 0x39a5652fb11379;
pub const MONTGOMERY_S_BASE_16: u32 = 43;

/// Divisão e resto rápidos por 10^8 para inteiros não-negativos em 2 ciclos de CPU (zero idiv).
#[inline(always)]
pub fn fast_div_mod_base8(x: i64) -> (i64, i64) {
    if x >= 0 && x < (1_i64 << 54) {
        let x_u = x as u64;
        let hi = ((x_u as u128 * MONTGOMERY_M_BASE_8 as u128) >> (64 + MONTGOMERY_S_BASE_8)) as i64;
        let r = x - hi * BASE_8;
        (hi, r)
    } else {
        (x / BASE_8, x % BASE_8)
    }
}

/// Divisão e resto rápidos por 10^16 para inteiros não-negativos em 2 ciclos de CPU (zero idiv).
#[inline(always)]
pub fn fast_div_mod_base16(x: i64) -> (i64, i64) {
    if x >= 0 {
        let x_u = x as u64;
        let hi = ((x_u as u128 * MONTGOMERY_M_BASE_16 as u128) >> (64 + MONTGOMERY_S_BASE_16)) as i64;
        let r = x - hi * BASE_16;
        (hi, r)
    } else {
        (x / BASE_16, x % BASE_16)
    }
}

/// Erro aritmético explícito disparado quando a flag de hardware `JO` (Jump on Overflow)
/// detecta extrapolação do limite de 64 bits (`i64::MAX` / `i64::MIN`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArithmeticOverflowError(pub &'static str);

impl fmt::Display for ArithmeticOverflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArithmeticOverflowException: {}", self.0)
    }
}

impl std::error::Error for ArithmeticOverflowError {}

/// Helper equivalente ao `Math.addExact` da JVM: dispara pânico determinístico (capturável)
/// exatamente na fronteira de overflow de 64 bits (instrução `JO`).
#[inline(always)]
pub fn add_exact(a: i64, b: i64) -> i64 {
    a.checked_add(b)
        .expect("ArithmeticException: integer overflow in add_exact")
}

/// Helper equivalente ao `Math.subtractExact` da JVM.
#[inline(always)]
pub fn sub_exact(a: i64, b: i64) -> i64 {
    a.checked_sub(b)
        .expect("ArithmeticException: integer overflow in sub_exact")
}

/// Helper equivalente ao `Math.multiplyExact` da JVM.
#[inline(always)]
pub fn mul_exact(a: i64, b: i64) -> i64 {
    a.checked_mul(b)
        .expect("ArithmeticException: integer overflow in mul_exact")
}

/// Representação Unboxed (`#[repr(transparent)]`) de 64 bits para Fatores Compactos na Fibra $\mathcal{D}_{16}$
/// quando $q < 922$ (1 único registrador de 64 bits, Zero Boxing).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct QFactor64(pub i64);

impl QFactor64 {
    #[inline(always)]
    pub const fn new(raw16: i64) -> Self {
        Self(raw16)
    }

    #[inline(always)]
    pub const fn q(self) -> i64 {
        self.0 / BASE_16
    }

    #[inline(always)]
    pub const fn r(self) -> i64 {
        self.0 - (self.q() * BASE_16)
    }

    /// Multiplicação truncada no Monoide B3 ($\otimes_{16}$) imune a overflow intermediário.
    #[inline(always)]
    pub fn mul_b3(self, that: QFactor64) -> QFactor64 {
        QFactor64(FinancialCategoricalKernelV2::b3_monoid_mul16_safe(
            self.0, that.0,
        ))
    }

    #[inline(always)]
    pub fn trunc8(self) -> f64 {
        ((self.0 / BASE_8) as f64) * 1e-8
    }

    #[inline(always)]
    pub fn to_f64(self) -> f64 {
        (self.q() as f64) + ((self.r() as f64) * 1e-16)
    }
}

impl Add for QFactor64 {
    type Output = QFactor64;
    #[inline(always)]
    fn add(self, rhs: QFactor64) -> QFactor64 {
        QFactor64(add_exact(self.0, rhs.0))
    }
}

impl Sub for QFactor64 {
    type Output = QFactor64;
    #[inline(always)]
    fn sub(self, rhs: QFactor64) -> QFactor64 {
        QFactor64(sub_exact(self.0, rhs.0))
    }
}

/// Elemento de 128-bits na Extensão Central de Schreier $(q, r) \in \mathbb{Z} \times \mathcal{M}_{10^{16}}$.
/// Alinhado a 16 bytes para caber em um par de registradores `RDX:RAX` ou um registrador `XMM`.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct QDec128 {
    pub q: i64,
    pub r: i64,
}

impl QDec128 {
    pub const ZERO: QDec128 = QDec128 { q: 0, r: 0 };
    pub const ONE: QDec128 = QDec128 { q: 1, r: 0 };

    #[inline(always)]
    pub fn new(q: i64, r: i64) -> Self {
        let (qn, rn) = QuimeraCategoricalDecimal::normalize(q, r);
        Self { q: qn, r: rn }
    }

    #[inline(always)]
    pub const fn from_canonical_unchecked(q: i64, r: i64) -> Self {
        Self { q, r }
    }

    #[inline(always)]
    pub fn from_f64_lossy(val: f64) -> Self {
        let q = val.floor() as i64;
        let rem = ((val - (q as f64)) * (BASE_16 as f64)).round() as i64;
        Self::new(q, rem)
    }

    #[inline(always)]
    pub fn to_f64(self) -> f64 {
        (self.q as f64) + ((self.r as f64) * 1e-16)
    }

    #[inline(always)]
    pub fn add_quarter(self, rhs: QDec128) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::add128_quarter(self.q, self.r, rhs.q, rhs.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn sub_quarter(self, rhs: QDec128) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::sub128_quarter(self.q, self.r, rhs.q, rhs.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn mul_trunc16(self, rhs: QDec128) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::mul128_trunc16(self.q, self.r, rhs.q, rhs.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn project_trunc8(self) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::project_trunc8(self.q, self.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn project_round8_half_up(self) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::project_round8_half_up(self.q, self.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn project_trunc6_vna(self) -> QDec128 {
        let (q, r) = QuimeraCategoricalDecimal::project_trunc6_vna(self.q, self.r);
        QDec128 { q, r }
    }

    #[inline(always)]
    pub fn project_cents_trunc2(self) -> i64 {
        QuimeraCategoricalDecimal::project_cents_trunc2(self.q, self.r)
    }

    #[inline(always)]
    pub fn project_cents_round2_half_up(self) -> i64 {
        QuimeraCategoricalDecimal::project_cents_round2_half_up(self.q, self.r)
    }
}

impl fmt::Display for QDec128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            QuimeraCategoricalDecimal::format_exact16(self.q, self.r)
        )
    }
}

/// Motor de Aritmética de 128 bits em Base Decimal Radicial $(q: \text{i64}, r: \text{i64})$.
pub struct QuimeraCategoricalDecimal;

impl QuimeraCategoricalDecimal {
    pub const BASE_16: i64 = BASE_16;
    pub const BASE_8: i64 = BASE_8;
    pub const SCALE_8_TO_16: i64 = SCALE_8_TO_16;
    pub const SCALE_2_TO_16: i64 = SCALE_2_TO_16;
    pub const SCALE_6_TO_16: i64 = SCALE_6_TO_16;

    /// Normaliza um par $(q, r)$ qualquer para a forma canônica $0 \le r < 10^{16}$.
    #[inline(always)]
    pub fn normalize(q_in: i64, r_in: i64) -> (i64, i64) {
        let mut q = q_in;
        let mut r = r_in;
        if r >= BASE_16 {
            let (carry, rem) = fast_div_mod_base16(r);
            q = add_exact(q, carry);
            r = rem;
        } else if r < 0 {
            let pos_r_minus_1 = -r - 1;
            let (borrow_div, _) = fast_div_mod_base16(pos_r_minus_1);
            let borrow = borrow_div + 1;
            q = sub_exact(q, borrow);
            r += borrow * BASE_16;
        }
        (q, r)
    }

    /// 1. ADIÇÃO EXATA EM 128-BITS VIA 2-COCICLO DE SCHREIER (Zero Divisão, Branchless Carry `CMOV`)
    /// Complexidade: 3 instruções de CPU (`ADD`, `CMP/CMOV`, `SUB`).
    #[inline(always)]
    pub fn add128_quarter(q1: i64, r1: i64, q2: i64, r2: i64) -> (i64, i64) {
        let r_sum = r1 + r2; // Seguro: r1, r2 < 10^16 => r_sum < 2 * 10^16 << i64::MAX
        let carry = (r_sum >= BASE_16) as i64; // Branchless 2-cociclo C(r1, r2) in {0, 1}
        let q_out = add_exact(add_exact(q1, q2), carry);
        let r_out = r_sum - (carry * BASE_16);
        (q_out, r_out)
    }

    /// 2. SUBTRAÇÃO EXATA EM 128-BITS VIA 2-COCICLO DE SCHREIER INVERSO (Zero Divisão, Branchless Borrow)
    #[inline(always)]
    pub fn sub128_quarter(q1: i64, r1: i64, q2: i64, r2: i64) -> (i64, i64) {
        let r_diff = r1 - r2;
        let borrow = (r_diff < 0) as i64;
        let q_out = sub_exact(sub_exact(q1, q2), borrow);
        let r_out = r_diff + (borrow * BASE_16);
        (q_out, r_out)
    }

    /// 3. PRODUTO TENSORIAL DE 128-BITS COM TRUNCAMENTO B3 NA 16ª CASA DECIMAL:
    /// Calcula $(q_1 \cdot 10^{16} + r_1) \otimes (q_2 \cdot 10^{16} + r_2)$ em $\mathcal{D}_{16}$
    /// para saldos bilionários/trilionários onde $q_1 \cdot q_2$ pode chegar a $9.22 \times 10^{18}$.
    ///
    /// Decomposição Bilinear na Fibra:
    /// $$(q_1 + r_1 10^{-16})(q_2 + r_2 10^{-16}) = q_1 q_2 + (q_1 r_2 + q_2 r_1)10^{-16} + \lfloor (r_1 r_2) / 10^{16} \rfloor 10^{-16}$$
    #[inline(always)]
    pub fn mul128_trunc16(q1: i64, r1: i64, q2: i64, r2: i64) -> (i64, i64) {
        // 3.1. Termo puramente fracionário: floor((r1 * r2) / 10^16) in [0, 10^16 - 1]
        // Em Rust x86_64 com BMI2, o produto de dois i64 em i128 compila para uma única
        // instrução `MULX` e a divisão por constante 10^16 usa multiplicação recíproca sem `DIV`!
        // Mantemos também paridade algébrica exata com a decomposição em base 10^8.
        let r1_hi = r1 / BASE_8;
        let r1_lo = r1 - (r1_hi * BASE_8);
        let r2_hi = r2 / BASE_8;
        let r2_lo = r2 - (r2_hi * BASE_8);

        let prod_lo = r1_lo * r2_lo;
        let prod_mid = (r1_hi * r2_lo) + (r1_lo * r2_hi) + (prod_lo / BASE_8);
        let frac_carry = (r1_hi * r2_hi) + (prod_mid / BASE_8); // < 10^16

        // 3.2. Termo cruzado 1: q1 * r2 = q1 * (r2_hi * 10^8 + r2_lo)
        let q1_hi = q1 / BASE_8;
        let q1_lo = q1 - (q1_hi * BASE_8);

        let c1_mid = add_exact(mul_exact(q1_hi, r2_lo), mul_exact(q1_lo, r2_hi));
        let c1_mid_quo = c1_mid / BASE_8;
        let c1_mid_rem = c1_mid - (c1_mid_quo * BASE_8);
        let c1_int_part = add_exact(mul_exact(q1_hi, r2_hi), c1_mid_quo);
        let c1_frac_raw = (c1_mid_rem * BASE_8) + (q1_lo * r2_lo);

        // 3.3. Termo cruzado 2: q2 * r1 = q2 * (r1_hi * 10^8 + r1_lo)
        let q2_hi = q2 / BASE_8;
        let q2_lo = q2 - (q2_hi * BASE_8);

        let c2_mid = add_exact(mul_exact(q2_hi, r1_lo), mul_exact(q2_lo, r1_hi));
        let c2_mid_quo = c2_mid / BASE_8;
        let c2_mid_rem = c2_mid - (c2_mid_quo * BASE_8);
        let c2_int_part = add_exact(mul_exact(q2_hi, r1_hi), c2_mid_quo);
        let c2_frac_raw = (c2_mid_rem * BASE_8) + (q2_lo * r1_lo);

        // 3.4. Consolida todas as partes inteiras e fracionárias via normalização de Schreier:
        let total_int_base = add_exact(mul_exact(q1, q2), add_exact(c1_int_part, c2_int_part));

        let (q_step1, r_step1) = Self::normalize(total_int_base, c1_frac_raw);
        let (q_step2, r_step2) = Self::normalize(q_step1, r_step1 + c2_frac_raw);
        Self::normalize(q_step2, r_step2 + frac_carry)
    }

    /// 3b. [MELHORIA HARDWARE RUST] Produto Tensorial 128-bits acelerado por instrução `MULX` (`i128` ALU nativa).
    /// Opera os termos cruzados e o termo fracionário usando registradores de 128 bits nativos do x86_64,
    /// reduzindo de 8 divisões escalares para 3 multiplicações recíprocas em hardware!
    #[inline(always)]
    pub fn mul128_trunc16_hw(q1: i64, r1: i64, q2: i64, r2: i64) -> (i64, i64) {
        const B16: i128 = 10_000_000_000_000_000_i128;
        let frac_carry = ((r1 as i128) * (r2 as i128) / B16) as i64;

        let cross1 = (q1 as i128) * (r2 as i128);
        let c1_int = i64::try_from(cross1 / B16)
            .expect("ArithmeticException: overflow in cross1 integer part");
        let c1_rem = (cross1 % B16) as i64;

        let cross2 = (q2 as i128) * (r1 as i128);
        let c2_int = i64::try_from(cross2 / B16)
            .expect("ArithmeticException: overflow in cross2 integer part");
        let c2_rem = (cross2 % B16) as i64;

        let total_int = add_exact(mul_exact(q1, q2), add_exact(c1_int, c2_int));
        let (q_s1, r_s1) = Self::normalize(total_int, c1_rem);
        let (q_s2, r_s2) = Self::normalize(q_s1, r_s1 + c2_rem);
        Self::normalize(q_s2, r_s2 + frac_carry)
    }

    // ===================================================================================
    // 4. PROJETORES REGULATÓRIOS (FUNCTORES DE ESQUECIMENTO D_16 -> D_8, D_6, D_2)
    // ===================================================================================

    /// Truncamento B3 na 8ª casa decimal (Fatores de Indexador).
    #[inline(always)]
    pub fn project_trunc8(q: i64, r: i64) -> (i64, i64) {
        let r8 = (r / SCALE_8_TO_16) * SCALE_8_TO_16;
        (q, r8)
    }

    /// Arredondamento ABNT/BACEN Half-Up na 8ª casa decimal.
    #[inline(always)]
    pub fn project_round8_half_up(q: i64, r: i64) -> (i64, i64) {
        let half8: i64 = 50_000_000; // 0.5 * 10^8
        let r_rounded = ((r + half8) / SCALE_8_TO_16) * SCALE_8_TO_16;
        if r_rounded >= BASE_16 {
            (add_exact(q, 1), 0)
        } else {
            (q, r_rounded)
        }
    }

    /// Truncamento ANBIMA na 6ª casa decimal (VNA de NTN-B / IPCA / IGP-M).
    #[inline(always)]
    pub fn project_trunc6_vna(q: i64, r: i64) -> (i64, i64) {
        let r6 = (r / SCALE_6_TO_16) * SCALE_6_TO_16;
        (q, r6)
    }

    /// Truncamento Contábil BACEN na 2ª casa decimal (retorna centavos inteiros de Real).
    #[inline(always)]
    pub fn project_cents_trunc2(q: i64, r: i64) -> i64 {
        add_exact(mul_exact(q, 100), r / SCALE_2_TO_16)
    }

    /// Arredondamento Contábil BACEN Half-Up na 2ª casa decimal (retorna centavos inteiros de Real).
    #[inline(always)]
    pub fn project_cents_round2_half_up(q: i64, r: i64) -> i64 {
        let half_cent: i64 = 50_000_000_000_000; // 0.5 * 10^14
        let cents_frac = (r + half_cent) / SCALE_2_TO_16;
        add_exact(mul_exact(q, 100), cents_frac)
    }

    /// Formata `(q, r)` para String Decimal Exata de 16 casas (Auditoria / Logs).
    pub fn format_exact16(q: i64, r: i64) -> String {
        let (qn, rn) = Self::normalize(q, r);
        if qn < 0 && rn > 0 {
            let abs_q = -(qn + 1);
            let abs_r = BASE_16 - rn;
            format!("-{}.{:016}", abs_q, abs_r)
        } else {
            format!("{}.{:016}", qn, rn)
        }
    }
}

/// Estrutura Colunar Desempacotada (SoA) para Vetores de 128-bits (`QDec128ColumnarVector`).
/// Aloca `q_arr` e `r_arr` em `AlignedBuffer<i64>` alinhados a linhas de cache de **64 bytes**,
/// permitindo vetorização SIMD AVX2/AVX-512 sem um único ponteiro de objeto.
#[derive(Clone, Debug)]
pub struct QDec128ColumnarVector {
    pub size: usize,
    pub q_arr: AlignedBuffer<i64>,
    pub r_arr: AlignedBuffer<i64>,
}

impl QDec128ColumnarVector {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            q_arr: AlignedBuffer::from_elem(0, size),
            r_arr: AlignedBuffer::from_elem(0, size),
        }
    }

    #[inline(always)]
    pub fn set(&mut self, idx: usize, q: i64, r: i64) {
        self.q_arr[idx] = q;
        self.r_arr[idx] = r;
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> QDec128 {
        QDec128 {
            q: self.q_arr[idx],
            r: self.r_arr[idx],
        }
    }

    #[inline(always)]
    pub fn add_in_place(&mut self, idx: usize, dq: i64, dr: i64) {
        let (nq, nr) =
            QuimeraCategoricalDecimal::add128_quarter(self.q_arr[idx], self.r_arr[idx], dq, dr);
        self.q_arr[idx] = nq;
        self.r_arr[idx] = nr;
    }

    #[inline(always)]
    pub fn mul_factor_in_place(&mut self, idx: usize, factor_q: i64, factor_r: i64) {
        let (nq, nr) = QuimeraCategoricalDecimal::mul128_trunc16(
            self.q_arr[idx],
            self.r_arr[idx],
            factor_q,
            factor_r,
        );
        self.q_arr[idx] = nq;
        self.r_arr[idx] = nr;
    }

    /// [MELHORIA SIMD/SoA RUST] Soma vetorizada in-place de outro `QDec128ColumnarVector`
    /// sobre arrays alinhados a 64 bytes (sem ramificações no cálculo do 2-cociclo de Schreier).
    pub fn add_vector_in_place(&mut self, rhs: &QDec128ColumnarVector) {
        assert_eq!(self.size, rhs.size);
        let q_dst = self.q_arr.as_mut_slice();
        let r_dst = self.r_arr.as_mut_slice();
        let q_src = rhs.q_arr.as_slice();
        let r_src = rhs.r_arr.as_slice();

        for i in 0..self.size {
            let r_sum = r_dst[i] + r_src[i];
            let carry = (r_sum >= BASE_16) as i64;
            q_dst[i] = q_dst[i] + q_src[i] + carry;
            r_dst[i] = r_sum - carry * BASE_16;
        }
    }

    /// Redução exata de todos os saldos do vetor colunar em um único `QDec128` sem perda de 1 ULP ($10^{-16}$).
    pub fn sum_exact(&self) -> QDec128 {
        let mut acc_q = 0_i64;
        let mut acc_r = 0_i64;
        let q_slice = self.q_arr.as_slice();
        let r_slice = self.r_arr.as_slice();
        for i in 0..self.size {
            let (nq, nr) =
                QuimeraCategoricalDecimal::add128_quarter(acc_q, acc_r, q_slice[i], r_slice[i]);
            acc_q = nq;
            acc_r = nr;
        }
        QDec128 { q: acc_q, r: acc_r }
    }
}
