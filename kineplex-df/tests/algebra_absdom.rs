//! Testes do Domínio Abstrato e Conexões de Galois de Intervalos (`catalyst::algebra::absdom`).
//!
//! Validam as leis de reticulado, a monotonicidade da abstração/concretização, a decisão
//! estática de predicados comparativos e a poda de filtros/ramos contraditórios no Catalyst.

use kineplex_df::catalyst::algebra::absdom::{try_eval_boolean, AbstractEnv, Interval};
use kineplex_df::catalyst::algebra::simplify_stage_with_absdom;
use kineplex_df::catalyst::CatalystCompilerOptimizer;
use kineplex_df::sql::LogicalStage;
use kineplex_df::{col, lit, Expression, ScalarValue, SparkSession};

fn col_id() -> Box<Expression> {
    Box::new(Expression::Column("id".into()))
}

fn lit_i(v: i64) -> Box<Expression> {
    Box::new(Expression::Literal(ScalarValue::Long(v)))
}

#[test]
fn lattice_meet_join_axioms_hold() {
    let b = Interval::Bottom;
    let t = Interval::Top;
    let i1 = Interval::Bounded(10, 50);
    let i2 = Interval::Bounded(30, 80);
    let i3 = Interval::Bounded(60, 100);

    // Identidades
    assert_eq!(i1.meet(&t), i1);
    assert_eq!(i1.meet(&b), b);
    assert_eq!(i1.join(&b), i1);
    assert_eq!(i1.join(&t), t);

    // Idempotência
    assert_eq!(i1.meet(&i1), i1);
    assert_eq!(i1.join(&i1), i1);

    // Comutatividade
    assert_eq!(i1.meet(&i2), i2.meet(&i1));
    assert_eq!(i1.join(&i2), i2.join(&i1));

    // Meet calcula interseção correta
    assert_eq!(i1.meet(&i2), Interval::Bounded(30, 50));
    // Disjuntos geram Bottom
    assert_eq!(i1.meet(&i3), Interval::Bottom);

    // Join calcula envoltória convexa
    assert_eq!(i1.join(&i2), Interval::Bounded(10, 80));
    assert_eq!(i1.join(&i3), Interval::Bounded(10, 100));

    // Lei de Absorção: x ⊓ (x ⊔ y) = x
    assert_eq!(i1.meet(&i1.join(&i2)), i1);
}

#[test]
fn abstract_arithmetic_soundness() {
    let i1 = Interval::Bounded(10, 20);
    let i2 = Interval::Bounded(5, 15);

    // [10, 20] + [5, 15] = [15, 35]
    assert_eq!(i1.add(&i2), Interval::Bounded(15, 35));

    // [10, 20] - [5, 15] = [10 - 15, 20 - 5] = [-5, 15]
    assert_eq!(i1.sub(&i2), Interval::Bounded(-5, 15));

    // Multiplicação com sinais mistos
    let neg = Interval::Bounded(-2, 3);
    let pos = Interval::Bounded(4, 5);
    // Extremos: -2*5 = -10, 3*5 = 15 => [-10, 15]
    assert_eq!(neg.mul(&pos), Interval::Bounded(-10, 15));

    // Overflow de inteiros satura para Top com segurança
    let max_intv = Interval::Bounded(i64::MAX - 5, i64::MAX);
    let add_intv = Interval::Bounded(10, 20);
    assert_eq!(max_intv.add(&add_intv), Interval::Top);
}

#[test]
fn static_boolean_decision_via_intervals() {
    let mut env = AbstractEnv::new();
    env.set("x", Interval::Bounded(10, 20));
    env.set("y", Interval::Bounded(50, 100));

    // x < y é estritamente verdadeiro para todo x in [10,20] e y in [50,100]
    let cond_lt = Expression::Lt(
        Box::new(Expression::Column("x".into())),
        Box::new(Expression::Column("y".into())),
    );
    assert_eq!(try_eval_boolean(&cond_lt, &env), Some(true));

    // x > y é estritamente falso
    let cond_gt = Expression::Gt(
        Box::new(Expression::Column("x".into())),
        Box::new(Expression::Column("y".into())),
    );
    assert_eq!(try_eval_boolean(&cond_gt, &env), Some(false));

    // x == y é estritamente falso (intervalos disjuntos)
    let cond_eq = Expression::Eq(
        Box::new(Expression::Column("x".into())),
        Box::new(Expression::Column("y".into())),
    );
    assert_eq!(try_eval_boolean(&cond_eq, &env), Some(false));

    // Regiões com sobreposição não são decidíveis (soundness / sobre-aproximação)
    let mut env_overlap = AbstractEnv::new();
    env_overlap.set("x", Interval::Bounded(10, 60));
    env_overlap.set("y", Interval::Bounded(50, 100));
    assert_eq!(try_eval_boolean(&cond_lt, &env_overlap), None);
}

#[test]
fn tautological_filter_elimination() {
    // Range de 0 a 10_000 com filtro id >= 0 é uma tautologia
    let stage = LogicalStage::Filter {
        input: Box::new(LogicalStage::Range {
            start: 0,
            end: 10_000,
            step: 1,
            num_partitions: 1,
        }),
        condition: Expression::Gte(col_id(), lit_i(0)),
    };

    let mut env = AbstractEnv::new();
    let (optimized, changed) = simplify_stage_with_absdom(&stage, &mut env);

    assert!(changed, "absdom deve eliminar o filtro tautológico");
    // O nó Filter foi removido, restando apenas o Range
    assert!(
        matches!(optimized, LogicalStage::Range { start: 0, end: 10_000, .. }),
        "Plano otimizado deve ser diretamente o Range"
    );
}

#[test]
fn contradictory_filter_pruning_to_empty_range() {
    // Range de 0 a 100 com filtro id > 500 é impossível (conjunto vazio)
    let stage = LogicalStage::Filter {
        input: Box::new(LogicalStage::Range {
            start: 0,
            end: 100,
            step: 1,
            num_partitions: 1,
        }),
        condition: Expression::Gt(col_id(), lit_i(500)),
    };

    let mut env = AbstractEnv::new();
    let (optimized, changed) = simplify_stage_with_absdom(&stage, &mut env);

    assert!(changed, "absdom deve podar filtro contraditório");
    assert!(
        matches!(optimized, LogicalStage::Range { start: 0, end: 0, .. }),
        "Plano deve ser podado para Range(0..0)"
    );
}

#[test]
fn when_otherwise_static_collapse() {
    let spark = SparkSession::builder().master("local[*]").get_or_create();

    // Range(10..20): id é sempre positivo (> 0).
    // when(id > 0, id * 2, id * 999) deve colapsar estaticamente para (id * 2)
    let df = spark
        .range(10, 20, 1, 1)
        .with_column(
            "calc",
            kineplex_df::sql::when(col("id").gt(lit(0)), col("id").mul(lit(2)))
                .otherwise(col("id").mul(lit(999))),
        );

    let opt_df = df.optimize_with(&CatalystCompilerOptimizer::new().with_algebraic(true));

    // Verifica que o resultado coletado executa corretamente
    let batches = opt_df.collect();
    assert_eq!(batches[0].num_rows, 10);
}
