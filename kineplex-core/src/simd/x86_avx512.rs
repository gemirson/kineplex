#[cfg(target_arch = "x86_64")]
use crate::simd::x86_avx2::prefix_xor_clmul_x86;
#[cfg(target_arch = "x86_64")]
use crate::simd::StructuralMask64;

/// Tier 1 Completo: Extração de 64 bytes com AVX-512BW + PCLMULQDQ (> 12 GB/s por core)
///
/// # Safety
/// O chamador deve garantir que a CPU suporta `avx512bw` e `pclmulqdq`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,pclmulqdq")]
pub unsafe fn extract_structural_mask_avx512(
    block_64: &[u8; 64],
    delim: u8,
    prev_in_quote: u64,
) -> StructuralMask64 {
    use std::arch::x86_64::{
        _mm512_cmpeq_epi8_mask, _mm512_loadu_si512, _mm512_set1_epi8,
    };

    let v_data = _mm512_loadu_si512(block_64.as_ptr() as *const _);
    let v_quote = _mm512_set1_epi8(b'"' as i8);
    let v_delim = _mm512_set1_epi8(delim as i8);
    let v_nl = _mm512_set1_epi8(b'\n' as i8);

    // Gera as 3 máscaras de 64 bits diretamente nos registradores k1, k2, k3 da CPU
    let quote_bits: u64 = _mm512_cmpeq_epi8_mask(v_data, v_quote);
    let raw_delim_bits: u64 = _mm512_cmpeq_epi8_mask(v_data, v_delim);
    let raw_nl_bits: u64 = _mm512_cmpeq_epi8_mask(v_data, v_nl);

    // Polinômio em GF(2)[x] via instrução CLMUL + propagação de carry de aspas
    let quote_mask = prefix_xor_clmul_x86(quote_bits) ^ prev_in_quote;
    let next_prev = ((quote_mask as i64) >> 63) as u64;

    StructuralMask64 {
        valid_delimiters_mask: raw_delim_bits & !quote_mask,
        valid_newlines_mask: raw_nl_bits & !quote_mask,
        next_prev_in_quote: next_prev,
    }
}
