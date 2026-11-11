/// Tier 3 Completo: AWS Graviton (aarch64 NEON + Polinômio PMULL vmull_p64)
///
/// # Safety
/// O chamador deve garantir que a CPU suporta `neon` e `aes`.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon,aes")]
pub unsafe fn prefix_xor_pmull_arm64(quote_bits: u64) -> u64 {
    use std::arch::aarch64::vmull_p64;
    let prod: u128 = vmull_p64(quote_bits, !0u64);
    prod as u64
}
