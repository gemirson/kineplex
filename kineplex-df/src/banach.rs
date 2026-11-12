//! # Análise Funcional e Álgebra em Espaços de Banach/Hilbert (`banach`)
//!
//! Implementação rigorosa do `BanachFrame` e `HilbertBatch` de dimensão canônica
//! $N = 65.536 = 2^{16}$ para processamento funcional de séries e matrizes financeiras.

use crate::memory::AlignedBuffer;

/// Dimensão canônica de um lote funcional: $N = 65.536 = 2^{16}$.
pub const BATCH_CANONICAL_DIM: usize = 65_536;

/// Categoria topológica do sinal financeiro no espaço de Banach.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FunctionalRegime {
    SubGaussianEquilibrium,
    LevyHeavyTail,
    DiracImpulseAnomaly,
}

/// Assinatura funcional contendo invariantes topológicos e espectrais.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FunctionalSignature {
    pub norm_l1: f64,
    pub norm_l2: f64,
    pub norm_linf: f64,
    pub norm_lp: f64,
    pub p_order: f64,
    pub crest_factor_kappa: f64,
    pub heavy_tail_lambda: f64,
    pub dirichlet_energy: f64,
    pub poincare_ratio: f64,
    pub regime: FunctionalRegime,
}

impl FunctionalSignature {
    pub fn to_feature_vec(&self) -> [f64; 6] {
        [
            self.norm_l1,
            self.norm_l2,
            self.crest_factor_kappa,
            self.heavy_tail_lambda,
            self.dirichlet_energy,
            self.poincare_ratio,
        ]
    }
}

/// `BanachFrame`: Vetor funcional com pesos e operadores funcionais em $L^p$ e $\mathcal{H}$.
pub struct BanachFrame {
    pub data: AlignedBuffer<f64>,
    pub weights: AlignedBuffer<f64>,
}

impl BanachFrame {
    /// Cria um novo BanachFrame a partir de dados brutos e pesos opcionais (se None, peso uniforme $1/N$).
    pub fn new(slice: &[f64], custom_weights: Option<&[f64]>) -> Self {
        let n = slice.len();
        let data = AlignedBuffer::<f64>::from_slice(slice);

        let weights = if let Some(w) = custom_weights {
            assert_eq!(w.len(), n, "Dimensão dos pesos incompatível com dados");
            AlignedBuffer::<f64>::from_slice(w)
        } else {
            let uniform_w = 1.0 / (n as f64);
            AlignedBuffer::<f64>::from_elem(uniform_w, n)
        };

        Self { data, weights }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Calcula a norma $L^1(\mu) = \sum_{i=0}^{N-1} |f(i)| \mu(i)$.
    pub fn norm_l1(&self) -> f64 {
        let d = self.data.as_slice();
        let w = self.weights.as_slice();
        let n = d.len();
        let chunks = n / 4;
        let mut acc0 = 0.0;
        let mut acc1 = 0.0;
        let mut acc2 = 0.0;
        let mut acc3 = 0.0;

        for c in 0..chunks {
            let base = c * 4;
            acc0 += d[base].abs() * w[base];
            acc1 += d[base + 1].abs() * w[base + 1];
            acc2 += d[base + 2].abs() * w[base + 2];
            acc3 += d[base + 3].abs() * w[base + 3];
        }
        let mut sum = (acc0 + acc1) + (acc2 + acc3);
        for i in (chunks * 4)..n {
            sum += d[i].abs() * w[i];
        }
        sum
    }

    /// Calcula a norma de Hilbert $L^2(\mu) = \sqrt{\sum_{i=0}^{N-1} f(i)^2 \mu(i)}$.
    pub fn norm_l2(&self) -> f64 {
        let d = self.data.as_slice();
        let w = self.weights.as_slice();
        let n = d.len();
        let chunks = n / 4;
        let mut acc0 = 0.0;
        let mut acc1 = 0.0;
        let mut acc2 = 0.0;
        let mut acc3 = 0.0;

        for c in 0..chunks {
            let base = c * 4;
            acc0 += d[base] * d[base] * w[base];
            acc1 += d[base + 1] * d[base + 1] * w[base + 1];
            acc2 += d[base + 2] * d[base + 2] * w[base + 2];
            acc3 += d[base + 3] * d[base + 3] * w[base + 3];
        }
        let mut sum_sq = (acc0 + acc1) + (acc2 + acc3);
        for i in (chunks * 4)..n {
            sum_sq += d[i] * d[i] * w[i];
        }
        sum_sq.sqrt()
    }

    /// Calcula a norma essencial $L^\infty = \max_{i} |f(i)|$.
    pub fn norm_linf(&self) -> f64 {
        let mut max_val = 0.0f64;
        for &x in self.data.as_slice() {
            let abs_x = x.abs();
            if abs_x > max_val {
                max_val = abs_x;
            }
        }
        max_val
    }

    /// Calcula a norma $L^p(\mu) = \left( \sum_{i=0}^{N-1} |f(i)|^p \mu(i) \right)^{1/p}$.
    pub fn norm_lp(&self, p: f64) -> f64 {
        assert!(p >= 1.0, "Ordem p deve ser >= 1.0");
        let d = self.data.as_slice();
        let w = self.weights.as_slice();
        let mut sum = 0.0;
        for i in 0..d.len() {
            sum += d[i].abs().powf(p) * w[i];
        }
        sum.powf(1.0 / p)
    }

    /// Produto interno no Espaço de Hilbert: $\langle f, g \rangle_\mu = \sum_{i=0}^{N-1} f(i) g(i) \mu(i)$.
    pub fn inner_product(&self, other: &BanachFrame) -> f64 {
        let n = self.len().min(other.len());
        let d1 = self.data.as_slice();
        let d2 = other.data.as_slice();
        let w = self.weights.as_slice();

        let chunks = n / 4;
        let mut acc0 = 0.0;
        let mut acc1 = 0.0;
        let mut acc2 = 0.0;
        let mut acc3 = 0.0;

        for c in 0..chunks {
            let base = c * 4;
            acc0 += d1[base] * d2[base] * w[base];
            acc1 += d1[base + 1] * d2[base + 1] * w[base + 1];
            acc2 += d1[base + 2] * d2[base + 2] * w[base + 2];
            acc3 += d1[base + 3] * d2[base + 3] * w[base + 3];
        }
        let mut sum = (acc0 + acc1) + (acc2 + acc3);
        for i in (chunks * 4)..n {
            sum += d1[i] * d2[i] * w[i];
        }
        sum
    }

    /// Funcional de Energia de Dirichlet no Espaço de Sobolev $W^{1,2}(\Omega, \mu)$:
    /// $\mathcal{E}_{\text{Dirichlet}}(f) = \frac{1}{2} \sum_{i=0}^{N-2} \mu(i) \left( f(i+1) - f(i) \right)^2$.
    pub fn dirichlet_energy(&self) -> f64 {
        let n = self.len();
        if n < 2 {
            return 0.0;
        }
        let d = self.data.as_slice();
        let w = self.weights.as_slice();

        let mut energy = 0.0;
        for i in 0..(n - 1) {
            let diff = d[i + 1] - d[i];
            energy += w[i] * (diff * diff);
        }
        0.5 * energy
    }

    /// Avalia a assinatura e categoriza o regime funcional do lote.
    pub fn analyze_signature(&self, p_order: f64) -> FunctionalSignature {
        let norm_l1 = self.norm_l1();
        let norm_l2 = self.norm_l2();
        let norm_linf = self.norm_linf();
        let norm_lp = self.norm_lp(p_order);
        let dirichlet_energy = self.dirichlet_energy();

        let l2_safe = if norm_l2 < 1e-12 { 1e-12 } else { norm_l2 };
        let crest_factor_kappa = norm_linf / l2_safe;
        let heavy_tail_lambda = norm_lp / l2_safe;

        let poincare_ratio = if dirichlet_energy < 1e-12 {
            1.0
        } else {
            (norm_l2 * norm_l2) / dirichlet_energy
        };

        let regime = if crest_factor_kappa > 8.0 {
            FunctionalRegime::DiracImpulseAnomaly
        } else if heavy_tail_lambda > 2.5 {
            FunctionalRegime::LevyHeavyTail
        } else {
            FunctionalRegime::SubGaussianEquilibrium
        };

        FunctionalSignature {
            norm_l1,
            norm_l2,
            norm_linf,
            norm_lp,
            p_order,
            crest_factor_kappa,
            heavy_tail_lambda,
            dirichlet_energy,
            poincare_ratio,
            regime,
        }
    }

    /// Ortogonalização por Gram-Schmidt Modificado: Decompõe $f = P_V(f) \oplus f^\perp$
    /// contra uma base ortonormal $\{e_1, \dots, e_M\}$.
    pub fn project_orthogonal(&self, basis: &[BanachFrame]) -> (BanachFrame, BanachFrame) {
        let mut proj = AlignedBuffer::<f64>::from_elem(0.0, self.len());

        for e in basis {
            let coeff = self.inner_product(e);
            let e_slice = e.data.as_slice();
            let p_slice = proj.as_mut_slice();
            for i in 0..self.len() {
                p_slice[i] += coeff * e_slice[i];
            }
        }

        let mut perp = AlignedBuffer::<f64>::from_elem(0.0, self.len());
        let d = self.data.as_slice();
        let p = proj.as_slice();
        let perp_mut = perp.as_mut_slice();
        for i in 0..self.len() {
            perp_mut[i] = d[i] - p[i];
        }

        let frame_proj = BanachFrame { data: proj, weights: self.weights.clone() };
        let frame_perp = BanachFrame { data: perp, weights: self.weights.clone() };
        (frame_proj, frame_perp)
    }

    /// Transformada Rápida de Wavelets de Haar (FWT) 1D com preservação exata de energia (Parseval).
    /// Requer $N = 2^J$ (ex: $65.536 = 2^{16}$).
    pub fn haar_wavelet_transform(&self) -> AlignedBuffer<f64> {
        let n = self.len();
        assert!(n.is_power_of_two(), "Comprimento deve ser potência de 2");

        let mut coeffs = AlignedBuffer::<f64>::from_slice(self.data.as_slice());

        let inv_sqrt2 = std::f64::consts::FRAC_1_SQRT_2;
        let mut len = n;
        let mut temp = AlignedBuffer::<f64>::from_elem(0.0, n);

        while len > 1 {
            let half = len / 2;
            let c = coeffs.as_slice();
            let t = temp.as_mut_slice();

            for i in 0..half {
                let a = c[2 * i];
                let b = c[2 * i + 1];
                t[i] = (a + b) * inv_sqrt2;          // Coeficiente de aproximação
                t[half + i] = (a - b) * inv_sqrt2;   // Coeficiente de detalhe
            }

            coeffs.as_mut_slice()[0..len].copy_from_slice(&t[0..len]);
            len = half;
        }

        coeffs
    }

    /// Distância de Wasserstein de ordem $p$ ($W_p$) entre distribuições empíricas via integração de quantis inversos.
    pub fn wasserstein_distance_p(&self, other: &BanachFrame, p: f64) -> f64 {
        assert_eq!(self.len(), other.len(), "Dimensões incompatíveis");
        let mut f_sorted: Vec<f64> = self.data.as_slice().to_vec();
        let mut g_sorted: Vec<f64> = other.data.as_slice().to_vec();

        f_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        g_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = f_sorted.len();
        let mut sum = 0.0;
        for i in 0..n {
            let diff = (f_sorted[i] - g_sorted[i]).abs();
            sum += diff.powf(p);
        }
        (sum / (n as f64)).powf(1.0 / p)
    }

    /// Distância de Wasserstein $W_1$ de ultra-alta performance utilizando buffers scratch pré-alocados.
    /// Elimina 100% das alocações na heap durante medições contínuas de fluxo topológico.
    pub fn wasserstein_distance_w1_scratch(
        &self,
        other: &BanachFrame,
        scratch_f: &mut [f64],
        scratch_g: &mut [f64],
    ) -> f64 {
        let n = self.len();
        assert_eq!(n, other.len(), "Dimensões incompatíveis");
        assert!(scratch_f.len() >= n && scratch_g.len() >= n, "Buffers scratch insuficientes");

        scratch_f[..n].copy_from_slice(self.data.as_slice());
        scratch_g[..n].copy_from_slice(other.data.as_slice());

        scratch_f[..n].sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        scratch_g[..n].sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let mut sum = 0.0;
        let chunks = n / 4;
        let mut acc0 = 0.0;
        let mut acc1 = 0.0;
        let mut acc2 = 0.0;
        let mut acc3 = 0.0;

        for c in 0..chunks {
            let base = c * 4;
            acc0 += (scratch_f[base] - scratch_g[base]).abs();
            acc1 += (scratch_f[base + 1] - scratch_g[base + 1]).abs();
            acc2 += (scratch_f[base + 2] - scratch_g[base + 2]).abs();
            acc3 += (scratch_f[base + 3] - scratch_g[base + 3]).abs();
        }
        sum += (acc0 + acc1) + (acc2 + acc3);
        for i in (chunks * 4)..n {
            sum += (scratch_f[i] - scratch_g[i]).abs();
        }
        sum / (n as f64)
    }

    /// Ponto Fixo de Cauchy via Iteração de Banach-Picard: $x_{k+1} = T(x_k)$.
    /// Converge geometricamente com $\|T(x) - T(y)\|_{L^2} \le \alpha \|x - y\|_{L^2}$ para $\alpha < 1$.
    pub fn cauchy_fixed_point<F>(&mut self, operator: F, alpha: f64, epsilon: f64, max_iters: usize) -> usize
    where
        F: Fn(&[f64], &mut [f64]),
    {
        assert!(alpha > 0.0 && alpha < 1.0, "Operador deve ser contração estrita (alpha < 1)");
        let n = self.len();
        let mut next_buf = AlignedBuffer::<f64>::from_elem(0.0, n);

        for iter in 0..max_iters {
            operator(self.data.as_slice(), next_buf.as_mut_slice());

            // Calcula erro em L2
            let mut diff_sq: f64 = 0.0;
            let d = self.data.as_slice();
            let nx = next_buf.as_slice();
            let w = self.weights.as_slice();

            for i in 0..n {
                let diff = nx[i] - d[i];
                diff_sq += diff * diff * w[i];
            }
            let err = diff_sq.sqrt();

            self.data.as_mut_slice().copy_from_slice(nx);

            if err < epsilon {
                return iter + 1;
            }
        }
        max_iters
    }
}
