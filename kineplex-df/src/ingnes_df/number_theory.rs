//! # Teoria dos Números para Simpatia Mecânica (`ingnes_df::number_theory`)
//!
//! Ferramentas de Teoria dos Números aplicadas à computação de alto desempenho:
//! 1. **Hashing Multiplicativo de Fibonacci (Knuth):**
//!    Usa o primo dourado $A = \lfloor 2^{64} / \phi \rfloor = 11400714819323198485$ para distribuir
//!    chaves de forma perfeitamente uniforme nas linhas de cache L1d com apenas 1 ciclo (`IMUL`).
//! 2. **Particionamento pelo Teorema Chinês dos Restos (CRT):**
//!    Permite particionar chaves compostas $(k_1, k_2)$ com módulos coprimos $m_1, m_2$ com $\gcd(m_1, m_2) = 1$
//!    em um único índice unidimensional compacto com reconciliação isomórfica $\mathbb{Z}/(m_1 m_2)\mathbb{Z} \cong \mathbb{Z}/m_1\mathbb{Z} \times \mathbb{Z}/m_2\mathbb{Z}$.
//! 3. **Crivos Aritméticos e Bitboards Branchless:**
//!    Avaliação de condições de filtro na forma de máscaras de bits de 64 bits (`u64`),
//!    eliminando completamente branch mispredictions nos pipelines colunares.

/// Constante de Fibonacci para Hashing Multiplicativo de 64 bits ($2^{64} / \phi$).
pub const FIBONACCI_HASH_PRIME_64: u64 = 11_400_714_819_323_198_485;

/// Primos pequenos para particionamento coprimo CRT na Cache L1d.
pub const COPRIME_M1: u64 = 257; // 2^8 + 1 (Primo de Fermat)
pub const COPRIME_M2: u64 = 263; // Primo adjacente, gcd(257, 263) = 1

/// Hashing de Fibonacci de 1 ciclo de instrução `IMUL`.
/// Mapeia qualquer `i64` para o intervalo $[0, 2^k - 1]$ com dispersão ótima na cache.
#[inline(always)]
pub fn fibonacci_hash_64(key: i64, bits: u32) -> usize {
    debug_assert!(bits <= 64);
    let shift = 64 - bits;
    let h = (key as u64).wrapping_mul(FIBONACCI_HASH_PRIME_64);
    (h >> shift) as usize
}

/// Algoritmo de Euclides Estendido: calcula $g, x, y$ tais que $a \cdot x + b \cdot y = g = \gcd(a, b)$.
pub fn extended_gcd(a: i64, b: i64) -> (i64, i64, i64) {
    if a == 0 {
        (b, 0, 1)
    } else {
        let (g, x1, y1) = extended_gcd(b % a, a);
        let x = y1 - (b / a) * x1;
        let y = x1;
        (g, x, y)
    }
}

/// Inverso Modular $a^{-1} \pmod m$.
pub fn mod_inverse(a: i64, m: i64) -> Option<i64> {
    let (g, x, _) = extended_gcd(a, m);
    if g == 1 || g == -1 {
        Some((x % m + m) % m)
    } else {
        None
    }
}

/// Solucionador do Teorema Chinês dos Restos (CRT) para 2 módulos coprimos.
/// Dado $x \equiv r_1 \pmod{m_1}$ e $x \equiv r_2 \pmod{m_2}$ com $\gcd(m_1, m_2) = 1$,
/// encontra a única solução $x \pmod{M}$ onde $M = m_1 \cdot m_2$.
pub struct ChineseRemainderEngine {
    pub m1: i64,
    pub m2: i64,
    pub m_total: i64,
    m1_inv_m2: i64,
    m2_inv_m1: i64,
}

impl ChineseRemainderEngine {
    pub fn new(m1: i64, m2: i64) -> Self {
        let (g, _, _) = extended_gcd(m1, m2);
        assert_eq!(g.abs(), 1, "Modulos para CRT devem ser coprimos (gcd = 1)");
        let m_total = m1 * m2;
        let m1_inv_m2 = mod_inverse(m1, m2).expect("Inverso modular falhou");
        let m2_inv_m1 = mod_inverse(m2, m1).expect("Inverso modular falhou");
        Self {
            m1,
            m2,
            m_total,
            m1_inv_m2,
            m2_inv_m1,
        }
    }

    /// Codifica um par de partições residuais $(r_1, r_2)$ em um índice único canônico $X \in [0, M-1]$.
    #[inline(always)]
    pub fn encode(&self, r1: i64, r2: i64) -> i64 {
        let t1 = (r1 * self.m2) % self.m_total;
        let term1 = (t1 * self.m2_inv_m1) % self.m_total;

        let t2 = (r2 * self.m1) % self.m_total;
        let term2 = (t2 * self.m1_inv_m2) % self.m_total;

        (term1 + term2) % self.m_total
    }

    /// Decodifica o índice global $X$ de volta para o par de coordenadas de partição $(r_1, r_2)$ em 2 ciclos de CPU.
    #[inline(always)]
    pub fn decode(&self, x: i64) -> (i64, i64) {
        (x % self.m1, x % self.m2)
    }
}

/// Bitboard colunar para filtragem branchless de 64 linhas simultâneas por palavra de CPU.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnBitmask {
    pub words: Vec<u64>,
    pub len: usize,
}

impl ColumnBitmask {
    pub fn all_ones(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        let mut words = vec![!0_u64; num_words];
        let rem = len % 64;
        if rem != 0 && num_words > 0 {
            words[num_words - 1] = (1_u64 << rem) - 1;
        }
        Self { words, len }
    }

    pub fn all_zeros(len: usize) -> Self {
        let num_words = (len + 63) / 64;
        Self {
            words: vec![0_u64; num_words],
            len,
        }
    }

    #[inline(always)]
    pub fn set(&mut self, idx: usize, val: bool) {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        if val {
            self.words[word_idx] |= 1_u64 << bit_idx;
        } else {
            self.words[word_idx] &= !(1_u64 << bit_idx);
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> bool {
        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        (self.words[word_idx] & (1_u64 << bit_idx)) != 0
    }

    /// Operação vetorial AND bit a bit em registradores de 64 bits.
    #[inline(always)]
    pub fn and_inplace(&mut self, rhs: &ColumnBitmask) {
        assert_eq!(self.len, rhs.len);
        for i in 0..self.words.len() {
            self.words[i] &= rhs.words[i];
        }
    }

    /// Operação vetorial OR bit a bit.
    #[inline(always)]
    pub fn or_inplace(&mut self, rhs: &ColumnBitmask) {
        assert_eq!(self.len, rhs.len);
        for i in 0..self.words.len() {
            self.words[i] |= rhs.words[i];
        }
    }

    /// Contagem populacional (`POPCNT`) de linhas aprovadas em 1 ciclo de hardware.
    #[inline(always)]
    pub fn count_ones(&self) -> usize {
        let mut sum = 0usize;
        for &w in &self.words {
            sum += w.count_ones() as usize;
        }
        sum
    }
}
