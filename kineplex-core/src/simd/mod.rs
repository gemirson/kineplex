pub mod arm_neon;
pub mod swar;
pub mod x86_avx2;
pub mod x86_avx512;

pub use swar::prefix_xor_swar_64;
#[cfg(target_arch = "x86_64")]
pub use x86_avx2::{extract_structural_mask_avx2, prefix_xor_clmul_x86};
#[cfg(target_arch = "x86_64")]
pub use x86_avx512::extract_structural_mask_avx512;

/// Resultado da máscara estrutural de 64 bytes extraída em 1 ciclo
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StructuralMask64 {
    pub valid_delimiters_mask: u64, // Bits 1 exatamente nos delimitadores fora de aspas
    pub valid_newlines_mask: u64,   // Bits 1 exatamente nos '\n' fora de aspas
    pub next_prev_in_quote: u64,    // 0xFFFF_FFFF_FFFF_FFFF se terminou dentro de aspas, 0 caso contrário
}

/// Contrato formal para parsers estruturais SIMD em GF(2)[x]
pub trait SimdStructuralParser: Send + Sync {
    fn parse_block_64(&self, block: &[u8; 64], delim: u8, prev_in_quote: u64) -> StructuralMask64;
}

pub struct DynamicSimdParser;

impl SimdStructuralParser for DynamicSimdParser {
    fn parse_block_64(&self, block: &[u8; 64], delim: u8, prev_in_quote: u64) -> StructuralMask64 {
        compute_quote_mask_dispatch(block, delim, prev_in_quote)
    }
}

/// Dispatcher Dinâmico Resolve-Once: Seleciona em tempo de execução o melhor kernel suportado pelo silício!
#[inline(always)]
pub fn compute_quote_mask_dispatch(
    block_64: &[u8; 64],
    delim: u8,
    prev_in_quote: u64,
) -> StructuralMask64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("pclmulqdq") {
            return unsafe { extract_structural_mask_avx512(block_64, delim, prev_in_quote) };
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("pclmulqdq") {
            return unsafe { extract_structural_mask_avx2(block_64, delim, prev_in_quote) };
        }
    }

    // Fallback portável SWAR (qualquer arquitetura de 64 bits)
    let mut quote_bits = 0u64;
    let mut raw_delim_bits = 0u64;
    let mut raw_nl_bits = 0u64;
    for (i, &b) in block_64.iter().enumerate().take(64) {
        quote_bits |= ((b == b'"') as u64) << i;
        raw_delim_bits |= ((b == delim) as u64) << i;
        raw_nl_bits |= ((b == b'\n') as u64) << i;
    }
    let quote_mask = prefix_xor_swar_64(quote_bits) ^ prev_in_quote;
    let next_prev = ((quote_mask as i64) >> 63) as u64;

    StructuralMask64 {
        valid_delimiters_mask: raw_delim_bits & !quote_mask,
        valid_newlines_mask: raw_nl_bits & !quote_mask,
        next_prev_in_quote: next_prev,
    }
}
