use arrow_array::{Float64Array, Int64Array, StringArray};
use kineplex_core::kinetxt::delimited::{
    clean_quoted_field, compute_quote_mask_64, extract_nth_delimited_field,
};
use kineplex_core::kinetxt::driver::{KineTxtDriver, TextBlockParser, TxtColumnSpec};

#[test]
fn test_compute_quote_mask_64_gf2() {
    // Caso 1: Aspas nas posições 2 e 5 (bits 2 e 5)
    // Dentro das aspas: posições 2, 3, 4
    let raw_quotes = (1u64 << 2) | (1u64 << 5);
    let (mask, next_in_quotes) = compute_quote_mask_64(raw_quotes, false);

    assert!(!next_in_quotes);
    // Bits 2, 3, 4 devem estar ligados
    assert_eq!(mask & (1u64 << 1), 0);
    assert_ne!(mask & (1u64 << 2), 0);
    assert_ne!(mask & (1u64 << 3), 0);
    assert_ne!(mask & (1u64 << 4), 0);
    assert_eq!(mask & (1u64 << 5), 0);
    assert_eq!(mask & (1u64 << 6), 0);

    // Delimitador na posição 3 (dentro das aspas) deve ser anulado
    let raw_delims = 1u64 << 3;
    let valid_delims = raw_delims & !mask;
    assert_eq!(valid_delims, 0);

    // Delimitador na posição 6 (fora das aspas) deve permanecer válido
    let raw_delims_outside = 1u64 << 6;
    let valid_delims_outside = raw_delims_outside & !mask;
    assert_eq!(valid_delims_outside, 1u64 << 6);

    // Caso 2: Propagação de estado anterior em aberto
    let (mask_cont, next_open) = compute_quote_mask_64(0, true);
    assert_eq!(mask_cont, !0u64);
    assert!(next_open);
}

#[test]
fn test_clean_quoted_field() {
    assert_eq!(clean_quoted_field(b"\"HELLO\""), b"HELLO");
    assert_eq!(clean_quoted_field(b"HELLO"), b"HELLO");
    assert_eq!(clean_quoted_field(b"\"\""), b"");
    assert_eq!(clean_quoted_field(b""), b"");
}

#[test]
fn test_extract_nth_delimited_field_directly() {
    let line = b"100;\"ABC;DEF\";99.9\r";
    assert_eq!(extract_nth_delimited_field(line, b';', 0), b"100");
    assert_eq!(extract_nth_delimited_field(line, b';', 1), b"ABC;DEF");
    assert_eq!(extract_nth_delimited_field(line, b';', 2), b"99.9");
    assert_eq!(extract_nth_delimited_field(line, b';', 3), b"");
}

#[test]
fn test_delimited_csv_with_quotes() {
    let raw_csv = b"\
1001;\"DESCRICAO;COM;PONTOEVIRGULA\";1250.50
1002;\"NORMAL\";300.00
1003;\"OUTRO;CAMPO;MISTO\";45.75
";

    let specs = vec![
        TxtColumnSpec::Int64 {
            name: "id".to_string(),
            col_index: 0,
            fixed_range: None,
        },
        TxtColumnSpec::Utf8 {
            name: "descricao".to_string(),
            col_index: 1,
            fixed_range: None,
        },
        TxtColumnSpec::Float64 {
            name: "valor".to_string(),
            col_index: 2,
            fixed_range: None,
            scale: 1.0,
        },
    ];

    let driver = KineTxtDriver::new(b';', specs);
    let batch = driver.parse_delimited_chunk_to_arrow(raw_csv).unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3);

    // Valida coluna ID
    let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id_col.value(0), 1001);
    assert_eq!(id_col.value(1), 1002);
    assert_eq!(id_col.value(2), 1003);

    // Valida coluna Descrição preservando delimitadores internos
    let desc_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(desc_col.value(0), "DESCRICAO;COM;PONTOEVIRGULA");
    assert_eq!(desc_col.value(1), "NORMAL");
    assert_eq!(desc_col.value(2), "OUTRO;CAMPO;MISTO");

    // Valida coluna Valor
    let val_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((val_col.value(0) - 1250.50).abs() < 1e-4);
    assert!((val_col.value(1) - 300.00).abs() < 1e-4);
    assert!((val_col.value(2) - 45.75).abs() < 1e-4);

    // Validação via trait TextBlockParser
    let batch_trait = driver.parse_delimited(raw_csv).unwrap();
    assert_eq!(batch_trait.num_rows(), 3);
}

#[test]
fn test_delimited_empty_and_trailing_variations() {
    let specs = vec![
        TxtColumnSpec::Int64 {
            name: "a".to_string(),
            col_index: 0,
            fixed_range: None,
        },
        TxtColumnSpec::Utf8 {
            name: "b".to_string(),
            col_index: 1,
            fixed_range: None,
        },
    ];
    let driver = KineTxtDriver::new(b',', specs);

    // Linha única sem '\n' no final
    let raw = b"42,teste";
    let batch = driver.parse_delimited_chunk_to_arrow(raw).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let str_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(str_col.value(0), "teste");

    // Buffer completamente vazio
    let empty_batch = driver.parse_delimited_chunk_to_arrow(b"").unwrap();
    assert_eq!(empty_batch.num_rows(), 0);
}
