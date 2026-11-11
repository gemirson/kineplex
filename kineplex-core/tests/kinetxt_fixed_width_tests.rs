use arrow_array::{Float64Array, Int64Array, StringArray};
use kineplex_core::kinetxt::driver::{KineTxtDriver, TextBlockParser, TxtColumnSpec};

#[test]
fn test_fixed_width_cnab240_parsing() {
    let stride = 240;
    let num_rows = 1000;
    let mut raw_buffer = Vec::with_capacity(num_rows * stride);

    // Cria 1000 linhas de 240 bytes
    for i in 0..num_rows {
        let mut line = vec![b' '; stride];
        // Coluna 0 (Int64, range 0..8): "00001234"
        let acc_str = format!("{:08}", 1000 + i);
        line[0..8].copy_from_slice(acc_str.as_bytes());

        // Coluna 1 (Float64 com escala 0.01, range 8..20): "000000125430" = 1254.30
        let bal_str = format!("{:012}", 125430 + i * 10);
        line[8..20].copy_from_slice(bal_str.as_bytes());

        // Coluna 2 (Utf8, range 20..50): "CLIENTE TESTE NUMERO..."
        let name_str = format!("CLIENTE_{:04}", i);
        line[20..20 + name_str.len()].copy_from_slice(name_str.as_bytes());

        // Quebra de linha no final
        line[stride - 1] = b'\n';
        raw_buffer.extend_from_slice(&line);
    }

    let specs = vec![
        TxtColumnSpec::Int64 {
            name: "conta".to_string(),
            col_index: 0,
            fixed_range: Some((0, 8)),
        },
        TxtColumnSpec::Float64 {
            name: "saldo".to_string(),
            col_index: 1,
            fixed_range: Some((8, 20)),
            scale: 0.01,
        },
        TxtColumnSpec::Utf8 {
            name: "nome".to_string(),
            col_index: 2,
            fixed_range: Some((20, 50)),
        },
    ];

    let driver = KineTxtDriver::new_fixed_width(stride, specs);
    let start = std::time::Instant::now();
    let batch = driver.parse_fixed_width_to_arrow(&raw_buffer, stride).unwrap();
    let elapsed = start.elapsed();

    assert_eq!(batch.num_rows(), num_rows);
    assert_eq!(batch.num_columns(), 3);

    // Valida coluna Int64
    let acc_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(acc_col.value(0), 1000);
    assert_eq!(acc_col.value(999), 1999);

    // Valida coluna Float64 com escala
    let bal_col = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((bal_col.value(0) - 1254.30).abs() < 1e-4);

    // Valida coluna Utf8 Zero-Copy
    let name_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(name_col.value(0).starts_with("CLIENTE_0000"));

    println!(
        "FixedWidth CNAB 240 parsing de {} linhas executado em {:?}",
        num_rows, elapsed
    );

    // Teste via trait TextBlockParser
    let batch_trait = driver.parse_fixed_width(&raw_buffer, stride).unwrap();
    assert_eq!(batch_trait.num_rows(), num_rows);
}

#[test]
fn test_fixed_width_error_handling() {
    let specs = vec![TxtColumnSpec::Int64 {
        name: "conta".to_string(),
        col_index: 0,
        fixed_range: Some((0, 8)),
    }];
    let driver = KineTxtDriver::new_fixed_width(240, specs.clone());

    // Erro ao passar stride zero
    let err_zero = driver.parse_fixed_width_to_arrow(b"test", 0);
    assert!(err_zero.is_err());
    assert!(err_zero.unwrap_err().contains("Stride não pode ser zero"));

    // Erro quando fixed_range for None no modo fixed-width
    let invalid_specs = vec![TxtColumnSpec::Int64 {
        name: "conta".to_string(),
        col_index: 0,
        fixed_range: None,
    }];
    let driver_invalid = KineTxtDriver::new_fixed_width(240, invalid_specs);
    let err_missing_range = driver_invalid.parse_fixed_width_to_arrow(b"12345678", 8);
    assert!(err_missing_range.is_err());

    // Erro quando fixed_range exceder o stride
    let oob_specs = vec![TxtColumnSpec::Float64 {
        name: "valor".to_string(),
        col_index: 0,
        fixed_range: Some((10, 300)),
        scale: 1.0,
    }];
    let driver_oob = KineTxtDriver::new_fixed_width(240, oob_specs);
    let err_oob = driver_oob.parse_fixed_width_to_arrow(b"12345678", 240);
    assert!(err_oob.is_err());

    // Buffer vazio resulta em 0 linhas válidas
    let empty_batch = driver.parse_fixed_width_to_arrow(b"", 240).unwrap();
    assert_eq!(empty_batch.num_rows(), 0);
}
