//! Testes da canonicalização algébrica (`catalyst::algebra::canon`).
//!
//! Cobrem: forma normal (idempotência, comutatividade, flip de comparações), escopo seguro
//! (bitwise intocado, literais à direita), equivalência de resultado (teste diferencial no
//! avaliador) e o ganho esperado no CSE sem regressão com a flag desligada.

use kineplex_df::catalyst::algebra::{canonicalize_expr, canonicalize_stage, canonicalize_stage_targeted};
use kineplex_df::catalyst::CatalystCompilerOptimizer;
use kineplex_df::sql::LogicalStage;
use kineplex_df::{AlignedBuffer, ColumnVector, ColumnarBatch, Expression, ScalarValue};

fn c(name: &str) -> Box<Expression> {
    Box::new(Expression::Column(name.to_string()))
}
fn long(v: i64) -> Box<Expression> {
    Box::new(Expression::Literal(ScalarValue::Long(v)))
}
fn b(e: Expression) -> Box<Expression> {
    Box::new(e)
}

fn sample_batch() -> ColumnarBatch {
    let n = 64usize;
    let a: Vec<i64> = (0..n as i64).map(|x| x * 3 - 40).collect();
    let bb: Vec<i64> = (0..n as i64).map(|x| 50 - x * 2).collect();
    let i: Vec<i32> = (0..n as i32).map(|x| x - 20).collect();
    let d: Vec<f64> = (0..n).map(|x| x as f64 * 0.37 - 5.0).collect();
    let e: Vec<f64> = (0..n).map(|x| 100.0 - x as f64 * 1.13).collect();
    ColumnarBatch::new(
        ["a", "b", "i", "d", "e"].iter().map(|s| s.to_string()).collect(),
        vec![
            ColumnVector::Long(AlignedBuffer::from_slice(&a)),
            ColumnVector::Long(AlignedBuffer::from_slice(&bb)),
            ColumnVector::Int(AlignedBuffer::from_slice(&i)),
            ColumnVector::Double(AlignedBuffer::from_slice(&d)),
            ColumnVector::Double(AlignedBuffer::from_slice(&e)),
        ],
    )
}

#[test]
fn commutative_operands_reach_the_same_normal_form() {
    let x = Expression::Add(c("a"), c("b"));
    let y = Expression::Add(c("b"), c("a"));
    assert_ne!(x, y);
    assert_eq!(canonicalize_expr(&x), canonicalize_expr(&y));

    let x = Expression::Mul(c("d"), c("e"));
    let y = Expression::Mul(c("e"), c("d"));
    assert_eq!(canonicalize_expr(&x), canonicalize_expr(&y));

    let x = Expression::Eq(c("a"), c("b"));
    let y = Expression::Eq(c("b"), c("a"));
    assert_eq!(canonicalize_expr(&x), canonicalize_expr(&y));
}

#[test]
fn canonicalization_is_idempotent() {
    let exprs = vec![
        Expression::Add(b(Expression::Mul(c("b"), c("a"))), b(Expression::Add(c("b"), c("a")))),
        Expression::Gt(c("a"), long(5)),
        Expression::And(
            b(Expression::Gte(c("b"), c("a"))),
            b(Expression::Neq(long(1), c("i"))),
        ),
    ];
    for e in exprs {
        let once = canonicalize_expr(&e);
        assert_eq!(once, canonicalize_expr(&once));
    }
}

#[test]
fn comparisons_gt_gte_become_lt_lte_with_swapped_operands() {
    let g = Expression::Gt(c("a"), c("b"));
    assert_eq!(canonicalize_expr(&g), Expression::Lt(c("b"), c("a")));
    let g = Expression::Gte(c("a"), c("b"));
    assert_eq!(canonicalize_expr(&g), Expression::Lte(c("b"), c("a")));
}

#[test]
fn literals_are_kept_on_the_right_to_preserve_existing_patterns() {
    let e = Expression::Add(long(0), c("a"));
    assert_eq!(canonicalize_expr(&e), Expression::Add(c("a"), long(0)));
    let e = Expression::Mul(long(8), c("a"));
    assert_eq!(canonicalize_expr(&e), Expression::Mul(c("a"), long(8)));
}

#[test]
fn bitwise_and_non_symmetric_ops_are_never_swapped() {
    // O avaliador aceita (Long, Int) mas não (Int, Long) em BitAnd/BitOr/BitXor.
    for e in [
        Expression::BitAnd(c("i"), c("a")),
        Expression::BitOr(c("a"), c("i")),
        Expression::BitXor(c("i"), c("a")),
        Expression::Sub(c("b"), c("a")),
        Expression::Div(c("b"), c("a")),
        Expression::Mod(c("b"), c("a")),
        Expression::BitShiftLeft(c("b"), c("i")),
    ] {
        assert_eq!(canonicalize_expr(&e), e);
    }
}

#[test]
fn canonical_form_is_semantically_equivalent_on_the_evaluator() {
    let batch = sample_batch();
    let exprs = vec![
        Expression::Add(c("a"), c("b")),
        Expression::Add(c("b"), c("a")),
        Expression::Add(c("a"), c("i")),
        Expression::Add(c("i"), c("a")),
        Expression::Mul(c("a"), long(7)),
        Expression::Mul(long(7), c("a")),
        Expression::Mul(c("d"), c("e")),
        Expression::Mul(c("e"), c("d")),
        Expression::Eq(c("a"), c("b")),
        Expression::Neq(c("b"), c("i")),
        Expression::Gt(c("a"), c("b")),
        Expression::Gte(c("a"), c("i")),
        Expression::Lt(c("b"), c("a")),
        Expression::Lte(c("d"), c("e")),
        Expression::Gt(c("d"), c("e")),
        Expression::And(
            b(Expression::Gt(c("a"), c("b"))),
            b(Expression::Lt(c("a"), long(5))),
        ),
        Expression::Or(
            b(Expression::Gte(c("b"), c("a"))),
            b(Expression::Eq(c("i"), c("a"))),
        ),
        Expression::Add(
            b(Expression::Mul(c("b"), c("a"))),
            b(Expression::Add(c("b"), c("a"))),
        ),
    ];
    for e in exprs {
        let canon = canonicalize_expr(&e);
        assert_eq!(
            format!("{:?}", e.eval(&batch)),
            format!("{:?}", canon.eval(&batch)),
            "resultado divergiu para {:?} -> {:?}",
            e,
            canon
        );
    }
}

fn plan_with_commuted_subexpr() -> LogicalStage {
    let base = LogicalStage::WithColumn {
        input: Box::new(LogicalStage::Range {
            start: 0,
            end: 1000,
            step: 1,
            num_partitions: 1,
        }),
        column_name: "b".into(),
        expression: Expression::Add(c("id"), long(7)),
    };
    LogicalStage::Select {
        input: Box::new(base),
        projections: vec![
            ("p".into(), Expression::Mul(b(Expression::Add(c("id"), c("b"))), c("id"))),
            ("q".into(), Expression::Sub(b(Expression::Add(c("b"), c("id"))), c("id"))),
        ],
    }
}

/// Mesmo plano, mas a subexpressão comutada é cara (`% 1000` usa divisão inteira).
fn plan_with_commuted_expensive_subexpr() -> LogicalStage {
    let base = LogicalStage::WithColumn {
        input: Box::new(LogicalStage::Range {
            start: 0,
            end: 1000,
            step: 1,
            num_partitions: 1,
        }),
        column_name: "b".into(),
        expression: Expression::Add(c("id"), long(7)),
    };
    LogicalStage::Select {
        input: Box::new(base),
        projections: vec![
            (
                "p".into(),
                Expression::Add(b(Expression::Mod(b(Expression::Add(c("id"), c("b"))), long(1000))), c("id")),
            ),
            (
                "q".into(),
                Expression::Sub(b(Expression::Mod(b(Expression::Add(c("b"), c("id"))), long(1000))), c("id")),
            ),
        ],
    }
}

#[test]
fn cse_of_cheap_commuted_subexpr_is_not_materialized_even_with_algebraic() {
    // Materializar `id + b` custa mais que recalculá-lo: o gate de desempenho mediu +18%.
    let plan = plan_with_commuted_subexpr();
    let (_, off) = CatalystCompilerOptimizer::new().optimize(&plan);
    let (_, on) = CatalystCompilerOptimizer::new().with_algebraic(true).optimize(&plan);
    assert_eq!(off.common_subexpressions, 0);
    assert_eq!(on.common_subexpressions, 0, "CSE barato não deve ser materializado");
}

#[test]
fn cse_finds_expensive_commuted_subexpressions_only_when_algebraic_is_enabled() {
    let plan = plan_with_commuted_expensive_subexpr();
    let (_, off) = CatalystCompilerOptimizer::new().optimize(&plan);
    let (_, on) = CatalystCompilerOptimizer::new().with_algebraic(true).optimize(&plan);
    assert_eq!(off.common_subexpressions, 0, "linha de base não acha CSE");
    assert!(
        on.common_subexpressions >= 1,
        "com canonicalização, (id+b)%1000 e (b+id)%1000 viram uma só subexpressão cara"
    );
}

#[test]
fn algebraic_flag_off_is_bit_for_bit_the_previous_behavior() {
    let plan = plan_with_commuted_subexpr();
    let default_opt = CatalystCompilerOptimizer::new();
    assert!(!default_opt.algebraic);
    let explicit_off = CatalystCompilerOptimizer::new().with_algebraic(false);
    let (p1, s1) = default_opt.optimize(&plan);
    let (p2, s2) = explicit_off.optimize(&plan);
    assert_eq!(format!("{:?}", p1), format!("{:?}", p2));
    assert_eq!(s1, s2);
}

#[test]
fn canonicalize_stage_is_idempotent() {
    let plan = plan_with_commuted_subexpr();
    let once = canonicalize_stage(&plan);
    let twice = canonicalize_stage(&once);
    assert_eq!(format!("{:?}", once), format!("{:?}", twice));
}

#[test]
fn targeted_canon_leaves_plans_without_profitable_sharing_untouched() {
    // Subexpressão barata: não há ganho, o plano deve permanecer idêntico.
    let plan = plan_with_commuted_subexpr();
    assert_eq!(
        format!("{:?}", canonicalize_stage_targeted(&plan)),
        format!("{:?}", plan)
    );
    // E o plano otimizado com a flag ligada é idêntico ao da flag desligada (sem regressão).
    let (p_off, s_off) = CatalystCompilerOptimizer::new().optimize(&plan);
    let (p_on, s_on) = CatalystCompilerOptimizer::new().with_algebraic(true).optimize(&plan);
    assert_eq!(format!("{:?}", p_off), format!("{:?}", p_on));
    assert_eq!(s_off, s_on);
}

#[test]
fn targeted_canon_rewrites_select_when_it_exposes_profitable_sharing() {
    let plan = plan_with_commuted_expensive_subexpr();
    let targeted = canonicalize_stage_targeted(&plan);
    assert_ne!(format!("{:?}", targeted), format!("{:?}", plan));
    // Idempotente: aplicar de novo não muda mais nada.
    assert_eq!(
        format!("{:?}", canonicalize_stage_targeted(&targeted)),
        format!("{:?}", targeted)
    );
}

#[test]
fn targeted_canon_does_not_touch_filters() {
    let plan = LogicalStage::Filter {
        input: Box::new(LogicalStage::Range { start: 0, end: 10, step: 1, num_partitions: 1 }),
        condition: Expression::Gt(c("id"), long(3)),
    };
    assert_eq!(
        format!("{:?}", canonicalize_stage_targeted(&plan)),
        format!("{:?}", plan)
    );
}
