/// Fallback Universal SWAR: Calcula o Prefix-XOR em GF(2)[x] em 6 passos logarítmicos
#[inline(always)]
pub fn prefix_xor_swar_64(mut quote_bits: u64) -> u64 {
    quote_bits ^= quote_bits << 1;
    quote_bits ^= quote_bits << 2;
    quote_bits ^= quote_bits << 4;
    quote_bits ^= quote_bits << 8;
    quote_bits ^= quote_bits << 16;
    quote_bits ^= quote_bits << 32;
    quote_bits
}
