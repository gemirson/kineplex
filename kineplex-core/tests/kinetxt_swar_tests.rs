use kineplex_core::kinetxt::swar::{fast_parse_f64_ascii, parse_8_digits_swar};

#[test]
fn test_swar_8_digits_conversion() {
    let raw_bytes = b"00012543";
    let ascii_be = u64::from_be_bytes(*raw_bytes);
    let val = parse_8_digits_swar(ascii_be);
    assert_eq!(val, 12543);

    let raw_bytes2 = b"98765432";
    let ascii_be2 = u64::from_be_bytes(*raw_bytes2);
    let val2 = parse_8_digits_swar(ascii_be2);
    assert_eq!(val2, 98765432);

    let raw_bytes3 = b"00000000";
    let ascii_be3 = u64::from_be_bytes(*raw_bytes3);
    assert_eq!(parse_8_digits_swar(ascii_be3), 0);

    let raw_bytes4 = b"12345678";
    let ascii_be4 = u64::from_be_bytes(*raw_bytes4);
    assert_eq!(parse_8_digits_swar(ascii_be4), 12345678);
}

#[test]
fn test_fast_parse_f64_ascii_variations() {
    // 8 dígitos acelerados via SWAR com escala implícita (centavos CNAB)
    let val1 = fast_parse_f64_ascii(b"00012543", 0.01);
    assert!((val1 - 125.43).abs() < 1e-6);

    // Inteiro com sinal negativo e espaços
    let val2 = fast_parse_f64_ascii(b"   -42", 1.0);
    assert_eq!(val2, -42.0);

    // Inteiro com sinal positivo e espaços
    let val2_pos = fast_parse_f64_ascii(b"   +42", 1.0);
    assert_eq!(val2_pos, 42.0);

    // Decimal com ponto
    let val3 = fast_parse_f64_ascii(b"1234.56", 1.0);
    assert!((val3 - 1234.56).abs() < 1e-4);

    // Decimal com vírgula
    let val4 = fast_parse_f64_ascii(b"9876,54", 1.0);
    assert!((val4 - 9876.54).abs() < 1e-4);

    // Vazio ou espaços
    assert_eq!(fast_parse_f64_ascii(b"", 1.0), 0.0);
    assert_eq!(fast_parse_f64_ascii(b"   ", 1.0), 0.0);

    // Retorno de carro \r ou delimitador de término
    let val5 = fast_parse_f64_ascii(b"5544\r", 1.0);
    assert_eq!(val5, 5544.0);

    // Número com escala zero
    assert_eq!(fast_parse_f64_ascii(b"100", 0.0), 0.0);

    // 8 dígitos com caracteres não-dígito (cai no fallback branchless)
    let val_fallback = fast_parse_f64_ascii(b"00012.43", 1.0);
    assert!((val_fallback - 12.43).abs() < 1e-4);
}
