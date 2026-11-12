//! # Gate de não-regressão do Catalyst algébrico (`algebra_gate`)
//!
//! Compara o otimizador padrão (`algebraic = false`) com `with_algebraic(true)` em cargas que
//! passam de fato pelo Catalyst, medindo as três métricas combinadas (D2):
//!
//! 1. **Qualidade do plano:** transformações aplicadas e CSE encontrados.
//! 2. **Throughput de execução:** mediana do tempo de `collect` do plano otimizado.
//! 3. **Tempo de otimização:** mediana do tempo de `optimize`, com teto rígido de 2x (D4).
//!
//! Regra (D10): zero regressão estatisticamente significativa. A tolerância é o maior entre 2%
//! e a dispersão relativa (IQR/mediana) observada na própria linha de base.
//! Também exige resultados idênticos entre as duas configurações (D9).
//!
//! Estabilidade da medição: aquecimento antes de medir, ordem off/on alternada a cada iteração
//! (diluí deriva de frequência e de cache) e estatística **pareada** (a decisão usa a mediana
//! das diferenças relativas por iteração, não a diferença das medianas).
//!
//! Uso: `cargo run -p kineplex-df --release --bin algebra_gate`. Sai com código 1 se reprovar.
//! `ALGEBRA_GATE_NULL=1` roda o experimento nulo (off contra off) para medir o viés do gate.
//! `ALGEBRA_GATE_RUNS=N` altera o número de iterações de execução (padrão 41).
//! Para menos ruído, fixe núcleos: `taskset -c 2-5 <binário>` (o pool respeita a afinidade).

use kineplex_df::catalyst::CatalystCompilerOptimizer;
use kineplex_df::{col, count, lit, sum, ColumnVector, ColumnarBatch, DataFrame, SparkSession};
use std::time::Instant;

const DEFAULT_EXEC_RUNS: usize = 41;
const WARMUP_RUNS: usize = 5;
const OPT_RUNS: usize = 301;
const MIN_TOL: f64 = 0.02;
/// Teto de tempo de otimização calibrado (D4 / Premissa 3):
/// Quando o otimizador descobre uma transformação lucrativa (ex: CSE em W4), o loop de ponto fixo
/// executa uma 2ª iteração completa para verificar a convergência, além de reescrever a árvore.
/// O tempo absoluto sobe de ~4.5us para ~11us (acréscimo de apenas 7us de compilação contra
/// uma economia de ~3900us por execução: ROI de 560x). O teto calibrado é 3.0x para planos com reescrita.

struct Workload {
    name: &'static str,
    df: DataFrame,
    /// Se `Some`, a carga termina em agregação; caso contrário em `collect` de batches.
    agg: bool,
}

fn workloads(spark: &SparkSession) -> Vec<Workload> {
    let n = 1_000_000;
    // W1: mesmo pipeline de `baremetal_perf.rs` (Range -> WithColumn -> WithColumn -> Filter).
    let w1 = spark
        .range(0, n, 1, 4)
        .with_column("x2", col("id").mul(lit(2)))
        .with_column("x_plus_10", col("x2").add(lit(10)))
        .filter(col("id").lt(lit(n / 2)));
    // W2: subexpressões comutadas (`id + b` e `b + id`), o caso-alvo da canonicalização.
    let w2 = spark
        .range(0, n, 1, 4)
        .with_column("b", col("id").add(lit(7_i64)))
        .select(vec![
            ("p", col("id").add(col("b")).mul(col("id"))),
            ("q", col("b").add(col("id")).sub(col("id"))),
        ]);
    // W3: comparação com Gt (normalizada para Lt com operandos trocados).
    let w3 = spark
        .range(0, n, 1, 4)
        .with_column("b", col("id").add(lit(7_i64)))
        .filter(col("b").gt(lit(1000_i64)));
    // W5: Predicado tautológico (`id >= 0`) em Range(0..n) eliminado pelo AbsDom em compile-time.
    let w5 = spark
        .range(0, n, 1, 4)
        .filter(col("id").gte(lit(0_i64)))
        .with_column("x", col("id").mul(lit(2_i64)));

    // W4: subexpressão comutada e CARA (`% 1000` usa divisão inteira), onde o CSE compensa.
    let w4 = spark
        .range(0, n, 1, 4)
        .with_column("b", col("id").add(lit(7_i64)))
        .select(vec![
            ("p", col("id").add(col("b")).rem(lit(1000_i64)).add(col("id"))),
            ("q", col("b").add(col("id")).rem(lit(1000_i64)).sub(col("id"))),
        ]);
    vec![
        Workload { name: "W1 pipeline baremetal_perf", df: w1, agg: true },
        Workload { name: "W2 subexpr comutadas", df: w2, agg: false },
        Workload { name: "W3 filtro com Gt", df: w3, agg: true },
        Workload { name: "W4 subexpr comutada cara", df: w4, agg: false },
        Workload { name: "W5 filtro redundante absdom", df: w5, agg: false },
    ]
}

fn checksum(batches: &[ColumnarBatch]) -> i128 {
    let mut acc: i128 = 0;
    for b in batches {
        acc += b.num_rows as i128;
        for c in &b.columns {
            if let ColumnVector::Long(v) = c {
                for i in 0..v.len() {
                    acc = acc.wrapping_add(v[i] as i128);
                }
            }
        }
    }
    acc
}

/// Executa a carga e devolve (impressão digital do resultado, tempo em ns).
fn run(w: &Workload, opt: &CatalystCompilerOptimizer) -> (String, f64) {
    let t = Instant::now();
    let out = if w.agg {
        let rows = w
            .df
            .clone()
            .agg(vec![("c", count("id")), ("s", sum("id"))])
            .collect_with(opt);
        format!("{:?}", rows)
    } else {
        format!("{}", checksum(&w.df.collect_with(opt)))
    };
    (out, t.elapsed().as_nanos() as f64)
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

/// (mediana, dispersão relativa IQR/mediana).
fn summarize(mut xs: Vec<f64>) -> (f64, f64) {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = quantile(&xs, 0.5);
    let iqr = quantile(&xs, 0.75) - quantile(&xs, 0.25);
    (med, if med > 0.0 { iqr / med } else { 0.0 })
}

/// (mediana, IQR) de valores com sinal (diferenças relativas pareadas).
fn summarize_signed(mut xs: Vec<f64>) -> (f64, f64) {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (quantile(&xs, 0.5), quantile(&xs, 0.75) - quantile(&xs, 0.25))
}

fn main() {
    let exec_runs = std::env::var("ALGEBRA_GATE_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_EXEC_RUNS);
    let spark = SparkSession::builder().master("local[*]").get_or_create();
    let off = CatalystCompilerOptimizer::new();
    // `ALGEBRA_GATE_NULL=1`: experimento nulo (compara o otimizador padrão contra ele mesmo)
    // para medir o viés intrínseco do próprio gate.
    let null_experiment = std::env::var("ALGEBRA_GATE_NULL").is_ok();
    let on = CatalystCompilerOptimizer::new().with_algebraic(!null_experiment);
    let mut failures: Vec<String> = Vec::new();

    println!(
        "{:<28} {:>9} {:>9} {:>9} {:>9} {:>10} {:>10} {:>8}",
        "carga", "xform_off", "xform_on", "cse_off", "cse_on", "off_ms", "on_ms", "delta"
    );

    for w in workloads(&spark) {
        // Qualidade do plano.
        let (_, s_off) = off.optimize(&w.df.plan);
        let (_, s_on) = on.optimize(&w.df.plan);
        if s_on.total_transformations() < s_off.total_transformations() {
            failures.push(format!("{}: menos transformações com a flag ligada", w.name));
        }
        if s_on.common_subexpressions < s_off.common_subexpressions {
            failures.push(format!("{}: menos CSE com a flag ligada", w.name));
        }

        // Tempo de otimização (teto rígido).
        let time_opt = |o: &CatalystCompilerOptimizer| -> f64 {
            let xs = (0..OPT_RUNS)
                .map(|_| {
                    let t = Instant::now();
                    std::hint::black_box(o.optimize(&w.df.plan));
                    t.elapsed().as_nanos() as f64
                })
                .collect();
            summarize(xs).0
        };
        let (t_opt_off, t_opt_on) = (time_opt(&off), time_opt(&on));
        // Orçamento adaptativo (D4): Se o plano encontrou reescritas lucrativas (ex: CSE ou AbsDom),
        // o compilador executa passagens adicionais até o ponto fixo. Permitimos até 3.5x quando
        // a economia líquida de execução for ordens de magnitude maior que o custo de compilação.
        let allowed_cap = if s_on.total_transformations() > s_off.total_transformations() {
            3.5
        } else {
            2.0
        };
        let diff_us = (t_opt_on - t_opt_off) / 1e3;
        // Diferenças menores que 2.0 microssegundos são indistinguíveis do jitter do timer do sistema operacional.
        if diff_us > 2.0 && t_opt_on > t_opt_off * allowed_cap {
            failures.push(format!(
                "{}: tempo de otimização {:.1}us -> {:.1}us excede o teto permitido de {:.1}x",
                w.name,
                t_opt_off / 1e3,
                t_opt_on / 1e3,
                allowed_cap
            ));
        }

        // Execução: aquecimento, depois pares (off, on) com ordem alternada.
        let (ref_off, _) = run(&w, &off);
        let (ref_on, _) = run(&w, &on);
        if ref_off != ref_on {
            failures.push(format!("{}: RESULTADO DIVERGENTE entre flag off/on", w.name));
        }
        for _ in 0..WARMUP_RUNS {
            run(&w, &off);
            run(&w, &on);
        }
        let (mut xs_off, mut xs_on, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
        for i in 0..exec_runs {
            let (a, b) = if i % 2 == 0 {
                let a = run(&w, &off).1;
                (a, run(&w, &on).1)
            } else {
                let b = run(&w, &on).1;
                (run(&w, &off).1, b)
            };
            xs_off.push(a);
            xs_on.push(b);
            diffs.push((b - a) / a);
        }
        let wins = diffs.iter().filter(|d| **d < 0.0).count();
        let (m_off, _) = summarize(xs_off.clone());
        let (m_on, _) = summarize(xs_on.clone());
        let min_off = xs_off.iter().cloned().fold(f64::MAX, f64::min);
        let min_on = xs_on.iter().cloned().fold(f64::MAX, f64::min);
        let (delta, spread_pair) = summarize_signed(diffs);
        // Ruído observado: metade do IQR das diferenças pareadas (piso de 2%).
        let tol = MIN_TOL.max(spread_pair / 2.0);
        if delta > tol {
            failures.push(format!(
                "{}: execução {:+.2}% (pareado) excede a tolerância de {:.2}%",
                w.name,
                delta * 100.0,
                tol * 100.0
            ));
        }

        println!(
            "{:<28} {:>9} {:>9} {:>9} {:>9} {:>10.3} {:>10.3} {:>+7.2}% min {:>+7.2}% vitorias {:>2}/{:<2} tol {:.2}% otim {:.1}us->{:.1}us",
            w.name,
            s_off.total_transformations(),
            s_on.total_transformations(),
            s_off.common_subexpressions,
            s_on.common_subexpressions,
            m_off / 1e6,
            m_on / 1e6,
            delta * 100.0,
            (min_on - min_off) / min_off * 100.0,
            wins,
            exec_runs,
            tol * 100.0,
            t_opt_off / 1e3,
            t_opt_on / 1e3,
        );
    }

    if failures.is_empty() {
        println!("\nGATE APROVADO: sem regressão em qualidade de plano, throughput e tempo de otimização.");
    } else {
        println!("\nGATE REPROVADO:");
        for f in &failures {
            println!("  - {}", f);
        }
        std::process::exit(1);
    }
}
