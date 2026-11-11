use std::time::Instant;
use kineplex_core::kinetxt::driver::{KineTxtDriver, TxtColumnSpec};

fn main() {
    println!("=== Benchmark: KineTxt Driver High-Throughput Engine ===");

    // 1. Benchmark do Modo Posicional CNAB 240 (O(1) Stride)
    println!("\n--- Modo 1: Posicional de Largura Fixa (CNAB 240) ---");
    let stride = 240;
    let num_rows = 100_000;
    let mut raw_buffer = Vec::with_capacity(num_rows * stride);

    for i in 0..num_rows {
        let mut line = vec![b' '; stride];
        let acc_str = format!("{:08}", i % 100000000);
        line[0..8].copy_from_slice(acc_str.as_bytes());

        let bal_str = format!("{:012}", (i * 137) % 1000000000000);
        line[8..20].copy_from_slice(bal_str.as_bytes());

        let name_str = format!("BENCH_CLIENTE_{:06}", i);
        line[20..20 + name_str.len()].copy_from_slice(name_str.as_bytes());

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

    // Warm-up
    let _ = driver.parse_fixed_width_to_arrow(&raw_buffer, stride).unwrap();

    let iterations = 10;
    let start = Instant::now();
    for _ in 0..iterations {
        let batch = driver.parse_fixed_width_to_arrow(&raw_buffer, stride).unwrap();
        assert_eq!(batch.num_rows(), num_rows);
    }
    let elapsed = start.elapsed();

    let total_bytes_processed = (raw_buffer.len() * iterations) as f64;
    let gb_processed = total_bytes_processed / (1024.0 * 1024.0 * 1024.0);
    let throughput_gbs = gb_processed / elapsed.as_secs_f64();

    println!("Linhas por iteração: {}", num_rows);
    println!("Tamanho do buffer por iteração: {:.2} MB", raw_buffer.len() as f64 / (1024.0 * 1024.0));
    println!("Tempo total ({} iterações): {:?}", iterations, elapsed);
    println!("Throughput Medido (Posicional): {:.2} GB/s", throughput_gbs);
    assert!(throughput_gbs >= 1.0, "Throughput posicional deve ser superior a 1 GB/s");

    // 2. Benchmark do Modo Delimitado Vetorizado (CSV com aspas)
    println!("\n--- Modo 2: Delimitado Vetorizado (CSV com aspas) ---");
    let mut raw_csv = Vec::new();
    for i in 0..num_rows {
        let line = format!(
            "{};\"CLIENTE_DESC_{};COM_PONTO_E_VIRGULA\";{:.2}\n",
            1000000 + i,
            i % 500,
            (i as f64) * 0.42
        );
        raw_csv.extend_from_slice(line.as_bytes());
    }

    let csv_specs = vec![
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

    let csv_driver = KineTxtDriver::new(b';', csv_specs);

    // Warm-up
    let _ = csv_driver.parse_delimited_chunk_to_arrow(&raw_csv).unwrap();

    let start_csv = Instant::now();
    for _ in 0..iterations {
        let batch = csv_driver.parse_delimited_chunk_to_arrow(&raw_csv).unwrap();
        assert_eq!(batch.num_rows(), num_rows);
    }
    let elapsed_csv = start_csv.elapsed();

    let total_csv_bytes = (raw_csv.len() * iterations) as f64;
    let gb_csv_processed = total_csv_bytes / (1024.0 * 1024.0 * 1024.0);
    let throughput_csv_gbs = gb_csv_processed / elapsed_csv.as_secs_f64();

    println!("Linhas por iteração: {}", num_rows);
    println!("Tamanho do buffer CSV por iteração: {:.2} MB", raw_csv.len() as f64 / (1024.0 * 1024.0));
    println!("Tempo total ({} iterações): {:?}", iterations, elapsed_csv);
    println!("Throughput Medido (Delimitado): {:.2} GB/s", throughput_csv_gbs);
    assert!(throughput_csv_gbs >= 0.5, "Throughput delimitado deve ser superior a 0.5 GB/s");

    println!("\n=== Benchmark KineTxt Concluído com Sucesso ===");
}
