use kineplex_core::simd::{
    compute_quote_mask_dispatch, prefix_xor_swar_64, DynamicSimdParser, SimdStructuralParser,
};

#[test]
fn test_prefix_xor_swar_64_algebra() {
    // 1. Aspa única na posição 2: bits 2..63 devem estar ligados
    let mask1 = prefix_xor_swar_64(1u64 << 2);
    assert_eq!(mask1 & 0b11, 0); // bits 0 e 1 desligados
    assert_ne!(mask1 & (1u64 << 2), 0);
    assert_ne!(mask1 & (1u64 << 63), 0);

    // 2. Par de aspas nas posições 2 e 5: bits 2, 3, 4 ligados, posições >= 5 desligadas
    let mask2 = prefix_xor_swar_64((1u64 << 2) | (1u64 << 5));
    assert_eq!(mask2 & (1u64 << 1), 0);
    assert_ne!(mask2 & (1u64 << 2), 0);
    assert_ne!(mask2 & (1u64 << 3), 0);
    assert_ne!(mask2 & (1u64 << 4), 0);
    assert_eq!(mask2 & (1u64 << 5), 0);
    assert_eq!(mask2 & (1u64 << 6), 0);
}

#[test]
fn test_simd_quote_mask_dispatch_block_filtering() {
    let mut block = [b' '; 64];
    // "ABC;DEF";GHI;JKL\n
    block[0] = b'"';
    block[1] = b'A';
    block[2] = b'B';
    block[3] = b'C';
    block[4] = b';'; // Delimitador protegido por aspas (deve ser anulado!)
    block[5] = b'D';
    block[6] = b'E';
    block[7] = b'F';
    block[8] = b'"';
    block[9] = b';'; // Delimitador válido (deve permanecer ligado!)
    block[13] = b';'; // Delimitador válido
    block[17] = b'\n'; // Quebra de linha válida

    let res = compute_quote_mask_dispatch(&block, b';', 0);

    // Delimitador na posição 4 (dentro de aspas) deve estar filtrado
    assert_eq!(res.valid_delimiters_mask & (1u64 << 4), 0);

    // Delimitadores nas posições 9 e 13 (fora de aspas) devem estar ligados
    assert_ne!(res.valid_delimiters_mask & (1u64 << 9), 0);
    assert_ne!(res.valid_delimiters_mask & (1u64 << 13), 0);

    // Quebra de linha na posição 17 deve estar ligada
    assert_ne!(res.valid_newlines_mask & (1u64 << 17), 0);

    // O bloco encerra fora de aspas
    assert_eq!(res.next_prev_in_quote, 0);
}

#[test]
fn test_simd_quote_propagation_across_blocks() {
    let mut block = [b' '; 64];
    // Abre aspas na posição 10 e não fecha no bloco de 64 bytes
    block[10] = b'"';
    block[20] = b';'; // Delimitador protegido

    let res1 = compute_quote_mask_dispatch(&block, b';', 0);
    assert_eq!(res1.valid_delimiters_mask & (1u64 << 20), 0);
    // Deve propagar aspas abertas (next_prev_in_quote != 0)
    assert_ne!(res1.next_prev_in_quote, 0);

    // Próximo bloco inicia já sob aspas
    let mut block2 = [b' '; 64];
    block2[5] = b';'; // Ainda dentro de aspas!
    block2[15] = b'"'; // Fecha aspas!
    block2[25] = b';'; // Fora de aspas!

    let res2 = compute_quote_mask_dispatch(&block2, b';', res1.next_prev_in_quote);
    assert_eq!(res2.valid_delimiters_mask & (1u64 << 5), 0);
    assert_ne!(res2.valid_delimiters_mask & (1u64 << 25), 0);
    assert_eq!(res2.next_prev_in_quote, 0); // Fechou aspas
}

#[test]
fn test_simd_parser_trait() {
    let parser = DynamicSimdParser;
    let block = [b';'; 64];
    let res = parser.parse_block_64(&block, b';', 0);
    // Sem aspas, todos os 64 delimitadores são válidos
    assert_eq!(res.valid_delimiters_mask, !0u64);
}
