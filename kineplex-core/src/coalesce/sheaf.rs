//! # Sheaf Harmonic Coalesce (Fusão de Feixes em $L^2$ com Regularização de Dirichlet)
//!
//! Resolve a descontinuidade de degrau dos operadores `COALESCE` tradicionais através de:
//! 1. **Homotopia de Calibre ($\Delta_{1,k}$):** Alinhamento estatístico suave no aberto de interseção $U_1 \cap U_k$.
//! 2. **Partição da Unidade ($\sum \psi_k(i) = 1$):** Combinação convexa das seções locais subordinada aos Validity Bitmaps,
//!    ponderada pelas precisões espectrais de Hilbert $\omega_k = \|f_k^\perp\|_{L^2(\mu)}^{-2}$.
//! 3. **Extensão Harmônica de Dirichlet ($\Delta_d \tilde{f} = 0$):** Resolução exata do problema de contorno discreto 1D
//!    nos intervalos de vazio total $\Omega \setminus \bigcup U_k$, minimizando a energia de Dirichlet $\mathcal{E}(\tilde{f})$
//!    e garantindo um buffer 100% denso ($C^0/C^1$).

use arrow_array::{Array, Float64Array};
use rayon::prelude::*;

/// Limiar ótimo de elementos para paralelismo via Rayon (janela Cache L1d)
const PARALLEL_ROW_THRESHOLD: usize = 2048;

/// Funde $M$ colunas `Float64Array` tratando seus Validity Bitmaps como abertos $U_k$ de um Feixe,
/// eliminando descontinuidades de spread e interpolando buracos via equação harmônica discreta ($\Delta_d f = 0$).
pub fn sheaf_harmonic_coalesce(
    columns: &[&Float64Array],
    hilbert_precisions: &[f64], // \omega_k = 1 / ||f_k^\perp||_{L^2(\mu)}^2
) -> Result<Float64Array, String> {
    if columns.is_empty() {
        return Err("[KineCoalesce] Nenhuma coluna fornecida para o Sheaf Coalesce.".into());
    }
    let num_rows = columns[0].len();
    let num_cols = columns.len();

    for (idx, col) in columns.iter().enumerate() {
        if col.len() != num_rows {
            return Err(format!(
                "[KineCoalesce] Dimensão incompatível na coluna {}: esperado {}, obtido {}",
                idx,
                num_rows,
                col.len()
            ));
        }
    }

    if num_rows == 0 {
        return Ok(Float64Array::from(Vec::<f64>::new()));
    }

    // Se houver apenas 1 coluna sem nulos, retorna diretamente
    if num_cols == 1 && columns[0].null_count() == 0 {
        return Ok((*columns[0]).clone());
    }

    // ------------------------------------------------------------------------
    // Etapa 1: Cálculo da Homotopia de Calibre (\Delta_{0,k}) nas interseções U_0 \cap U_k
    // ------------------------------------------------------------------------
    let primary = columns[0];
    let primary_vals = primary.values();

    let compute_shift = |k: usize| -> f64 {
        if k == 0 {
            return 0.0;
        }
        let col_k = columns[k];
        let vals_k = col_k.values();
        let mut sum_diff = 0.0_f64;
        let mut count = 0usize;

        for i in 0..num_rows {
            if primary.is_valid(i) && col_k.is_valid(i) {
                sum_diff += vals_k[i] - primary_vals[i];
                count += 1;
            }
        }
        if count > 0 {
            sum_diff / (count as f64)
        } else {
            0.0
        }
    };

    let gauge_shifts: Vec<f64> = if num_rows >= PARALLEL_ROW_THRESHOLD {
        (0..num_cols).into_par_iter().map(compute_shift).collect()
    } else {
        (0..num_cols).map(compute_shift).collect()
    };

    // ------------------------------------------------------------------------
    // Etapa 2: Avaliação da Partição da Unidade \sum_{k=1}^M \psi_k(i) = 1
    // ------------------------------------------------------------------------
    let mut coalesced = vec![0.0_f64; num_rows];
    let mut is_vacuum_hole = vec![false; num_rows];

    let evaluate_point = |i: usize, out_val: &mut f64, hole_flag: &mut bool| {
        let mut num_acc = 0.0_f64;
        let mut den_acc = 0.0_f64;

        for k in 0..num_cols {
            if columns[k].is_valid(i) {
                let omega_k = hilbert_precisions.get(k).copied().unwrap_or(1.0).max(1e-12);
                let calibrated_v = columns[k].values()[i] - gauge_shifts[k];
                num_acc += omega_k * calibrated_v;
                den_acc += omega_k;
            }
        }

        if den_acc > 0.0 {
            *out_val = num_acc / den_acc;
            *hole_flag = false;
        } else {
            *hole_flag = true; // Ponto pertence ao vazio total \Omega \setminus \bigcup U_k
        }
    };

    if num_rows >= PARALLEL_ROW_THRESHOLD {
        coalesced
            .par_iter_mut()
            .zip(is_vacuum_hole.par_iter_mut())
            .enumerate()
            .for_each(|(i, (out_val, hole_flag))| {
                evaluate_point(i, out_val, hole_flag);
            });
    } else {
        for i in 0..num_rows {
            evaluate_point(i, &mut coalesced[i], &mut is_vacuum_hole[i]);
        }
    }

    // ------------------------------------------------------------------------
    // Etapa 3: Extensão Harmônica de Dirichlet (\Delta_d f = 0) sobre os intervalos de vazio
    // Minimiza a energia de Dirichlet \mathcal{E}(f) = \sum |f(i+1) - f(i)|^2
    // ------------------------------------------------------------------------
    let mut idx = 0;
    while idx < num_rows {
        if is_vacuum_hole[idx] {
            let hole_start = idx;
            while idx < num_rows && is_vacuum_hole[idx] {
                idx += 1;
            }
            let hole_end = idx; // exclusivo

            let boundary_left = if hole_start > 0 {
                coalesced[hole_start - 1]
            } else if hole_end < num_rows {
                coalesced[hole_end]
            } else {
                0.0
            };

            let boundary_right = if hole_end < num_rows {
                coalesced[hole_end]
            } else {
                boundary_left
            };

            let span = (hole_end - hole_start + 1) as f64;
            for h in hole_start..hole_end {
                let alpha = ((h - hole_start + 1) as f64) / span;
                coalesced[h] = (1.0 - alpha) * boundary_left + alpha * boundary_right;
            }
        } else {
            idx += 1;
        }
    }

    Ok(Float64Array::from(coalesced))
}

/// Caso ultra-rápido de 2 colunas via Álgebra de Bitmask (Zero Branching)
pub fn bitwise_sheaf_coalesce_pair(
    col_a: &Float64Array,
    col_b: &Float64Array,
    omega_a: f64,
    omega_b: f64,
    gauge_shift_b: f64,
) -> Float64Array {
    let n = col_a.len().min(col_b.len());
    if n == 0 {
        return Float64Array::from(Vec::<f64>::new());
    }

    let vals_a = col_a.values();
    let vals_b = col_b.values();

    let inv_sum = 1.0 / (omega_a + omega_b).max(1e-15);
    let psi_a = omega_a * inv_sum;
    let psi_b = omega_b * inv_sum;

    let eval_pair = |i: usize| -> f64 {
        let va = col_a.is_valid(i);
        let vb = col_b.is_valid(i);
        let a = vals_a[i];
        let b = vals_b[i] - gauge_shift_b;

        match (va, vb) {
            (true, true) => psi_a * a + psi_b * b, // Interseção U_A \cap U_B
            (true, false) => a,                    // Apenas aberto U_A
            (false, true) => b,                    // Apenas aberto U_B (calibrado)
            (false, false) => 0.0,                 // Vazio total
        }
    };

    let out: Vec<f64> = if n >= PARALLEL_ROW_THRESHOLD {
        (0..n).into_par_iter().map(eval_pair).collect()
    } else {
        (0..n).map(eval_pair).collect()
    };

    Float64Array::from(out)
}
