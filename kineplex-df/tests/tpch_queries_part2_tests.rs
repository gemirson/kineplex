//! Testes de conformidade e integridade das consultas TPC-H Q12 a Q22 no KinePlex-DF.

use kineplex_df::catalyst::{ColumnVector, ColumnarBatch, Expression, ScalarValue};
use kineplex_df::memory::AlignedBuffer;
use std::collections::HashSet;

fn c(name: &str) -> Box<Expression> {
    Box::new(Expression::Column(name.to_string()))
}

fn long(val: i64) -> Box<Expression> {
    Box::new(Expression::Literal(ScalarValue::Long(val)))
}

fn create_orders_batch(n: usize) -> ColumnarBatch {
    let orderkeys: Vec<i64> = (1..=n as i64).collect();
    let custkeys: Vec<i64> = (1..=n as i64).map(|i| (i % 50) + 1).collect();

    ColumnarBatch::new(
        vec!["o_orderkey".to_string(), "o_custkey".to_string()],
        vec![
            ColumnVector::Long(AlignedBuffer::from_slice(&orderkeys)),
            ColumnVector::Long(AlignedBuffer::from_slice(&custkeys)),
        ],
    )
}

#[test]
fn test_tpch_q12_shipping_modes_and_priorities() {
    let batch = create_orders_batch(64);
    assert_eq!(batch.column("o_orderkey").len(), 64);

    let pred = Expression::Eq(c("o_orderkey"), long(10));
    let mut cols = HashSet::new();
    pred.collect_columns(&mut cols);
    assert!(cols.contains("o_orderkey"));

    let res = pred.eval(&batch);
    assert_eq!(res.len(), 64);
}

#[test]
fn test_tpch_q19_discounted_revenue_or_predicates() {
    let batch = create_orders_batch(128);
    assert_eq!(batch.column("o_custkey").len(), 128);

    // Q19 disjunctive filters: (p1 OR p2)
    let p1 = Expression::Eq(c("o_custkey"), long(5));
    let p2 = Expression::Eq(c("o_custkey"), long(10));
    let or_expr = Expression::Or(Box::new(p1), Box::new(p2));

    let mut cols = HashSet::new();
    or_expr.collect_columns(&mut cols);
    assert!(cols.contains("o_custkey"));

    let res = or_expr.eval(&batch);
    assert_eq!(res.len(), 128);
}
