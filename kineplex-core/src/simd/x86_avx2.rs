#[cfg(target_arch = "x86_64")]
use crate::simd::StructuralMask64;

/// Tier 1 & 2 (x86_64 PCLMULQDQ / AVX-512 VPCLMULQDQ):
/// Calcula o Prefix-XOR multiplicando quote_bits por 0xFFFF_FFFF_FFFF_FFFF no corpo de Galois GF(2)!
///
/// # Safety
/// O chamador deve garantir que a CPU suporta a instrução `pclmulqdq`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "pclmulqdq")]
pub unsafe fn prefix_xor_clmul_x86(quote_bits: u64) -> u64 {
    use std::arch::x86_64::{_mm_clmulepi64_si128, _mm_cvtsi128_si64, _mm_set_epi64x};
    let poly_q = _mm_set_epi64x(0, quote_bits as i64);
    let all_ones = _mm_set_epi64x(0, -1i64); // 0xFFFF_FFFF_FFFF_FFFF
    let prod = _mm_clmulepi64_si128(poly_q, all_ones, 0x00);
    _mm_cvtsi128_si64(prod) as u64
}

/// Tier 2 Completo: Extração de 64 bytes com AVX2 + PCLMULQDQ (Intel Xeon / AMD EPYC)
///
/// # Safety
/// O chamador deve garantir que a CPU suporta `avx2` e `pclmulqdq`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,pclmulqdq")]
pub unsafe fn extract_structural_mask_avx2(
    block_64: &[u8; 64],
    delim: u8,
    prev_in_quote: u64,
) -> StructuralMask64 {
    use std::arch::x86_64::{
        _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };

    let lo = _mm256_loadu_si256(block_64.as_ptr() as *const _);
    let hi = _mm256_loadu_si256(block_64.as_ptr().add(32) as *const _);

    let v_quote = _mm256_set1_epi8(b'"' as i8);
    let v_delim = _mm256_set1_epi8(delim as i8);
    let v_nl = _mm256_set1_epi8(b'\n' as i8);

    let pack_u64 = |m_lo: i32, m_hi: i32| -> u64 {
        ((m_lo as u32) as u64) | (((m_hi as u32) as u64) << 32)
    };

    let quote_bits = pack_u64(
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_quote)),
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_quote)),
    );
    let raw_delim_bits = pack_u64(
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_delim)),
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_delim)),
    );
    let raw_nl_bits = pack_u64(
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v_nl)),
        _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v_nl)),
    );

    let quote_mask = prefix_xor_clmul_x86(quote_bits) ^ prev_in_quote;
    let next_prev = ((quote_mask as i64) >> 63) as u64;

    StructuralMask64 {
        valid_delimiters_mask: raw_delim_bits & !quote_mask,
        valid_newlines_mask: raw_nl_bits & !quote_mask,
        next_prev_in_quote: next_prev,
    }
}
