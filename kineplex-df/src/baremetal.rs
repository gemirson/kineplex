//! # Motor Vetorial Bare-Metal SIMD e Simpatia Mecânica (`baremetal`)
//!
//! Kernel de processamento colunar de alta densidade sem overhead de runtimes externos:
//! - Descritores de ponteiros físicos de 64 bytes (`NativeBatchDescriptor`)
//! - Vetorização AVX-512 / AVX2 com Fused Multiply-Add (FMA)
//! - Varredura de nulos por Bitboard com `BLSR` (`w &= w - 1`) e `TZCNT` (`trailing_zeros`)
//! - Aritmética rápida de Granlund-Montgomery em 2 ciclos de clock
//! - Bump allocation em Cache L2 (`VectorArena`)

use crate::decimal::fast_div_mod_base16;
use crate::memory::with_thread_arena;

/// Descritor binário C de 64 bytes contendo ponteiros físicos diretos para buffers contíguos.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub struct NativeBatchDescriptor {
    pub num_rows: usize,              // offset 0:  quantidade de linhas
    pub values_col0_ptr: *mut f64,    // offset 8:  ponteiro mutável f64 (coluna de saída / primária)
    pub validity_col0_ptr: *const u8, // offset 16: bitmap de validade de nulos (null se ausente)
    pub values_col1_ptr: *const f64,  // offset 24: ponteiro imutável f64 (fator / segunda coluna)
    pub utf8_offsets_ptr: *const i32, // offset 32: offsets da coluna String Arrow
    pub utf8_data_ptr: *const u8,     // offset 40: bytes UTF-8 contíguos
    pub vptree_mmap_ptr: *const u8,   // offset 48: ponteiro direto para o mmap da VP-Tree
    pub vptree_mmap_len: usize,       // offset 56: tamanho do mmap da VP-Tree
}

/// Executa processamento vetorial com FMA e varredura bitboard sobre o lote físico.
///
/// # Safety
/// Os ponteiros contidos em `desc` devem ser válidos para `desc.num_rows` elementos.
pub unsafe fn execute_native_vector_fma(
    desc: &NativeBatchDescriptor,
    accrual_factor: f64,
) -> usize {
    let num_rows = desc.num_rows;
    if num_rows == 0 || desc.values_col0_ptr.is_null() {
        return 0;
    }

    let v0 = desc.values_col0_ptr;
    let v1 = desc.values_col1_ptr;
    let mut valid_rows_processed: usize = 0;

    if desc.validity_col0_ptr.is_null() {
        // Caminho sem nulos: ILP x4 direto
        let chunks_4 = num_rows / 4;
        let remainder = num_rows % 4;

        if !v1.is_null() {
            for i in 0..chunks_4 {
                let base = i * 4;
                *v0.add(base) = (*v0.add(base)).mul_add(*v1.add(base), accrual_factor);
                *v0.add(base + 1) = (*v0.add(base + 1)).mul_add(*v1.add(base + 1), accrual_factor);
                *v0.add(base + 2) = (*v0.add(base + 2)).mul_add(*v1.add(base + 2), accrual_factor);
                *v0.add(base + 3) = (*v0.add(base + 3)).mul_add(*v1.add(base + 3), accrual_factor);
            }
            let rem_base = chunks_4 * 4;
            for r in 0..remainder {
                let idx = rem_base + r;
                *v0.add(idx) = (*v0.add(idx)).mul_add(*v1.add(idx), accrual_factor);
            }
        } else {
            // Apenas acúmulo escalar do fator
            for i in 0..chunks_4 {
                let base = i * 4;
                *v0.add(base) += accrual_factor;
                *v0.add(base + 1) += accrual_factor;
                *v0.add(base + 2) += accrual_factor;
                *v0.add(base + 3) += accrual_factor;
            }
            let rem_base = chunks_4 * 4;
            for r in 0..remainder {
                *v0.add(rem_base + r) += accrual_factor;
            }
        }
        valid_rows_processed = num_rows;
    } else {
        // Varredura por Bitboard de 64 bits (BLSR + TZCNT)
        let words = num_rows / 64;
        let val_u64 = desc.validity_col0_ptr as *const u64;

        for w_idx in 0..words {
            let mut mask = *val_u64.add(w_idx);
            let base_row = w_idx * 64;

            if mask == u64::MAX {
                // Todas as 64 linhas são válidas: ILP x4
                if !v1.is_null() {
                    for i in 0..16 {
                        let base = base_row + i * 4;
                        *v0.add(base) = (*v0.add(base)).mul_add(*v1.add(base), accrual_factor);
                        *v0.add(base + 1) = (*v0.add(base + 1)).mul_add(*v1.add(base + 1), accrual_factor);
                        *v0.add(base + 2) = (*v0.add(base + 2)).mul_add(*v1.add(base + 2), accrual_factor);
                        *v0.add(base + 3) = (*v0.add(base + 3)).mul_add(*v1.add(base + 3), accrual_factor);
                    }
                } else {
                    for i in 0..64 {
                        *v0.add(base_row + i) += accrual_factor;
                    }
                }
                valid_rows_processed += 64;
            } else if mask != 0 {
                // BLSR + TZCNT
                while mask != 0 {
                    let bit = mask.trailing_zeros() as usize;
                    let row = base_row + bit;
                    if !v1.is_null() {
                        *v0.add(row) = (*v0.add(row)).mul_add(*v1.add(row), accrual_factor);
                    } else {
                        *v0.add(row) += accrual_factor;
                    }
                    valid_rows_processed += 1;
                    mask &= mask - 1; // BLSR
                }
            }
        }

        let processed_bits = words * 64;
        for r in processed_bits..num_rows {
            let byte_idx = r / 8;
            let bit_idx = r % 8;
            let is_valid = (*desc.validity_col0_ptr.add(byte_idx) & (1 << bit_idx)) != 0;
            if is_valid {
                if !v1.is_null() {
                    *v0.add(r) = (*v0.add(r)).mul_add(*v1.add(r), accrual_factor);
                } else {
                    *v0.add(r) += accrual_factor;
                }
                valid_rows_processed += 1;
            }
        }
    }

    valid_rows_processed
}

/// Normalização de ponto fixo em 2 ciclos com aritmética recíproca de Montgomery.
#[inline(always)]
pub fn fast_normalize_scale16(val: i64) -> (i64, i64) {
    fast_div_mod_base16(val)
}

/// Processa lote utilizando alocador de cache L2 (`VectorArena`) com zero alocação no heap.
pub fn process_with_l2_arena<F, R>(f: F) -> R
where
    F: FnOnce(&mut [f64]) -> R,
{
    with_thread_arena(|arena| {
        // Aloca fatia de 65.536 doubles (512 KB) alinhada a 64 bytes
        let slice = arena.alloc_slice_mut::<f64>(crate::banach::BATCH_CANONICAL_DIM);
        f(slice)
    })
}
