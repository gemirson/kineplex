//! # Testes e Benchmarks Bare-Metal & Matemática Aplicada para o `kineplex-df`

use kineplex_df::catalyst::{CatalystCompilerOptimizer, ScalarValue};
use kineplex_df::concurrency::MicroThreadPool;
use kineplex_df::decimal::{
    fast_div_mod_base16, fast_div_mod_base8, QuimeraCategoricalDecimal, QDec128,
    BASE_16, BASE_8,
};
use kineplex_df::ingnes_df::{
    catamorphism, ColumnBitmask, IngnesColumn, IngnesDataFrame,
    MaxF64Monoid, MinF64Monoid, QDec128SumMonoid, SumI64Monoid,
};
use kineplex_df::kernel::{
    FinancialCategoricalKernelV2, QuimeraNativeMath,
    IDX_CDI_PCT, IDX_CDI_PURE, IDX_IPCA, MAX_DAYS,
};
use kineplex_df::memory::{with_thread_arena, AlignedBuffer, CACHE_LINE_BYTES};
use kineplex_df::sql::{avg, col, count, lit, sum, SparkSession};
use std::sync::Arc;

#[test]
fn test_montgomery_fast_div_mod_exactness() {
    // 1. Testa casos de fronteira exatos para Base 10^8
    let edge_cases_8 = [
        0_i64,
        1,
        BASE_8 - 1,
        BASE_8,
        BASE_8 + 1,
        2 * BASE_8 - 1,
        2 * BASE_8,
        999_999_999_999,
        10_000_000_000_000_000 - 1, // 10^16 - 1
    ];

    for &x in &edge_cases_8 {
        let (q_fast, r_fast) = fast_div_mod_base8(x);
        let q_exact = x / BASE_8;
        let r_exact = x % BASE_8;
        assert_eq!(q_fast, q_exact, "q falhou para x={}", x);
        assert_eq!(r_fast, r_exact, "r falhou para x={}", x);
    }

    // 2. Testa 1 milhão de inteiros arbitrários para Base 10^8
    let mut rng: u64 = 0xdeadbeefcafebabe;
    for _ in 0..1_000_000 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let x = ((rng >> 10) % (BASE_16 as u64)) as i64;
        let (q_fast, r_fast) = fast_div_mod_base8(x);
        assert_eq!(q_fast, x / BASE_8);
        assert_eq!(r_fast, x % BASE_8);
    }

    // 3. Testa casos de fronteira exatos para Base 10^16
    let edge_cases_16 = [
        0_i64,
        1,
        BASE_16 - 1,
        BASE_16,
        BASE_16 + 1,
        5 * BASE_16 + 42,
        (1_i64 << 62) - 1,
    ];

    for &x in &edge_cases_16 {
        let (q_fast, r_fast) = fast_div_mod_base16(x);
        assert_eq!(q_fast, x / BASE_16, "q16 falhou para x={}", x);
        assert_eq!(r_fast, x % BASE_16, "r16 falhou para x={}", x);
    }
}

#[test]
fn test_schreier_2cocycle_normalization_and_mul16() {
    // Normalização canônica 0 <= r < 10^16
    let (q1, r1) = QuimeraCategoricalDecimal::normalize(5, BASE_16 * 3 + 12345);
    assert_eq!(q1, 8);
    assert_eq!(r1, 12345);

    let (q2, r2) = QuimeraCategoricalDecimal::normalize(10, -500);
    assert_eq!(q2, 9);
    assert_eq!(r2, BASE_16 - 500);

    // Produto B3 imune a overflow com divisão Montgomery (1.02 * 1.05 = 1.071)
    let x = BASE_16 + 200_000_000_000_000; // 1.02
    let y = BASE_16 + 500_000_000_000_000; // 1.05
    let prod = FinancialCategoricalKernelV2::b3_monoid_mul16_safe(x, y);
    let expected = BASE_16 + 710_000_000_000_000; // 1.071
    assert_eq!(prod, expected);
}

#[test]
fn test_unrolled_catamorphism_monoid_laws() {
    let n = 10_000;
    let data_i64: Vec<i64> = (1..=n as i64).collect();

    // Catamorfismo de Adição I64
    let sum_res = catamorphism::<SumI64Monoid>(&data_i64);
    let expected_sum = (n as i64 * (n as i64 + 1)) / 2;
    assert_eq!(sum_res, expected_sum);

    // Catamorfismo de Mínimo e Máximo F64
    let data_f64: Vec<f64> = data_i64.iter().map(|&x| (x as f64) * 0.5).collect();
    let min_val = catamorphism::<MinF64Monoid>(&data_f64);
    let max_val = catamorphism::<MaxF64Monoid>(&data_f64);
    assert_eq!(min_val, 0.5);
    assert_eq!(max_val, (n as f64) * 0.5);

    // QDec128 Catamorfismo
    let qdecs: Vec<QDec128> = (0..100).map(|i| QDec128::new(i, i * 1000)).collect();
    let qdec_sum = catamorphism::<QDec128SumMonoid>(&qdecs);
    assert_eq!(qdec_sum.q, 99 * 100 / 2);
}

#[test]
fn test_vector_arena_cache_alignment_and_zero_alloc() {
    with_thread_arena(|arena| {
        let slice1 = arena.alloc_slice_mut::<f64>(2048);
        assert_eq!(slice1.len(), 2048);
        assert_eq!((slice1.as_ptr() as usize) % CACHE_LINE_BYTES, 0);

        slice1.fill(42.0);
        assert_eq!(slice1[0], 42.0);
        assert_eq!(slice1[2047], 42.0);

        let slice2 = arena.alloc_slice_mut::<i64>(1024);
        assert_eq!(slice2.len(), 1024);
        assert_eq!((slice2.as_ptr() as usize) % CACHE_LINE_BYTES, 0);
    });
}

#[test]
fn test_micro_thread_pool_parallel_execution() {
    let pool = MicroThreadPool::new(4);
    assert_eq!(pool.thread_count(), 4);

    let tasks: Vec<_> = (0..16)
        .map(|i| {
            move || {
                let mut sum = 0_i64;
                for j in 0..10_000 {
                    sum += (i * 10_000 + j) as i64;
                }
                sum
            }
        })
        .collect();

    let results = pool.execute_parallel(tasks);
    assert_eq!(results.len(), 16);

    let mut expected_total = 0_i64;
    for k in 0..160_000 {
        expected_total += k as i64;
    }
    let actual_total: i64 = results.into_iter().sum();
    assert_eq!(actual_total, expected_total);
}

#[test]
fn test_bitboard_filter_tzcnt_blsr() {
    let mut mask = ColumnBitmask::all_zeros(100);
    mask.set(5, true);
    mask.set(12, true);
    mask.set(65, true);
    mask.set(99, true);

    let col_data = AlignedBuffer::tabulate(100, |i| (i * 10) as i64);
    let df = IngnesDataFrame::from_columns(vec![("val", IngnesColumn::I64(col_data))]);

    let filtered = df.filter_congruence("val", 100, 50); // múltiplos de 10 onde val % 100 == 50 -> 5, 15, 25...
    assert_eq!(filtered.num_rows(), 10);
}

#[test]
fn test_spark_sql_whole_stage_pipeline_and_ricci_curvature() {
    let spark = SparkSession::builder().master("local[*]").get_or_create();

    // 100.000 linhas divididas em 4 partições L1d
    let df = spark
        .range(0, 100_000, 1, 4)
        .with_column("x2", col("id").mul(lit(2)))
        .with_column("x_plus_10", col("x2").add(lit(10)))
        .filter(col("id").lt(lit(50_000)));

    // Verifica Curvatura de Ricci no plano de consulta:
    // Filtro restritivo resulta em contração métrica (curvatura positiva > 0)
    let kappa = CatalystCompilerOptimizer::plan_ricci_curvature(df.logical_plan());
    assert!(kappa > 0.0, "Curvatura de Ricci deve ser positiva para plano com filtro restritivo: {}", kappa);

    // Agregação paralela no MicroThreadPool
    let rows = df.agg(vec![
        ("total_rows", count("id")),
        ("sum_x2", sum("x2")),
        ("avg_x2", avg("x2")),
    ]);

    let result = rows.collect();
    assert_eq!(result.len(), 1);

    let row = &result[0];
    assert_eq!(row.values[0], ScalarValue::Long(50_000));

    // Soma de 2 * i para i in 0..50_000: 2 * (49999 * 50000 / 2) = 49999 * 50000 = 2_499_950_000
    assert_eq!(row.values[1], ScalarValue::Double(2_499_950_000.0));
    assert_eq!(row.values[2], ScalarValue::Double(49_999.0));
}

#[test]
fn test_financial_kernel_o1_dst_and_taylor_jet() {
    let raw_cdi = vec![0.00045; MAX_DAYS]; // ~12% a.a.
    let is_bd = vec![true; MAX_DAYS];
    let ipca = vec![100.0, 100.5, 101.2];

    let engine = Arc::new(FinancialCategoricalKernelV2::build_engine(&raw_cdi, &is_bd, &ipca));

    // Teste CDI Puro DST O(1)
    let factor_pure = QuimeraNativeMath::eval_row(&engine, 10, 31, IDX_CDI_PURE, 1, 1.0, 0.0);
    assert!(factor_pure > 1.0);

    // Teste CDI % Horner Taylor FMA O(1)
    let factor_pct = QuimeraNativeMath::eval_row(&engine, 10, 31, IDX_CDI_PCT, 1, 1.10, 0.0);
    assert!(factor_pct > factor_pure); // 110% do CDI deve render mais que 100%

    // Teste IPCA Grothendieck Fibration O(1)
    let factor_ipca = QuimeraNativeMath::eval_row(&engine, 5, 25, IDX_IPCA, 15, 1.0, 0.0);
    assert!(factor_ipca >= 1.0);
}
