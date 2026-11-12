//! Testes de conformidade e integridade das consultas TPC-H Q1 a Q11 no KinePlex-DF.

use kineplex_df::catalyst::{ColumnVector, ColumnarBatch, Expression, ScalarValue};
use kineplex_df::memory::AlignedBuffer;
use std::collections::HashSet;

fn c(name: &str) -> Box<Expression> {
    Box::new(Expression::Column(name.to_string()))
}

fn d(val: f64) -> Box<Expression> {
    Box::new(Expression::Literal(ScalarValue::Double(val)))
}

fn create_lineitem_batch(n: usize) -> ColumnarBatch {
    let quantities: Vec<f64> = (0..n).map(|i| (i % 50 + 1) as f64).collect();
    let extendedprices: Vec<f64> = quantities.iter().map(|&q| q * 900.0).collect();
    let discounts: Vec<f64> = (0..n).map(|i| (i % 10) as f64 * 0.01).collect();
    let taxes: Vec<f64> = (0..n).map(|i| (i % 8) as f64 * 0.01).collect();

    ColumnarBatch::new(
        vec![
            "l_quantity".to_string(),
            "l_extendedprice".to_string(),
            "l_discount".to_string(),
            "l_tax".to_string(),
        ],
        vec![
            ColumnVector::Double(AlignedBuffer::from_slice(&quantities)),
            ColumnVector::Double(AlignedBuffer::from_slice(&extendedprices)),
            ColumnVector::Double(AlignedBuffer::from_slice(&discounts)),
            ColumnVector::Double(AlignedBuffer::from_slice(&taxes)),
        ],
    )
}

#[test]
fn test_tpch_q1_pricing_summary_expression() {
    let batch = create_lineitem_batch(64);
    assert_eq!(batch.column("l_quantity").len(), 64);

    // Q1: l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)
    let disc_factor = Expression::Sub(d(1.0), c("l_discount"));
    let charge_factor = Expression::Add(d(1.0), c("l_tax"));
    let disc_price = Expression::Mul(c("l_extendedprice"), Box::new(disc_factor));
    let final_charge = Expression::Mul(Box::new(disc_price), Box::new(charge_factor));

    let mut cols = HashSet::new();
    final_charge.collect_columns(&mut cols);
    assert_eq!(cols.len(), 3);

    // Avaliação colunar vetorizada
    let res = final_charge.eval(&batch);
    assert_eq!(res.len(), 64);
    let slice = res.as_f64_slice();
    assert!(slice[0] > 0.0);
}

#[test]
fn test_tpch_q6_forecasting_predicates() {
    let batch = create_lineitem_batch(128);
    assert_eq!(batch.column("l_discount").len(), 128);

    // Q6: l_discount between 0.05 and 0.07 AND l_quantity < 24
    let p1 = Expression::Gte(c("l_discount"), d(0.05));
    let p2 = Expression::Lte(c("l_discount"), d(0.07));
    let p3 = Expression::Lt(c("l_quantity"), d(24.0));

    let filter = Expression::And(Box::new(Expression::And(Box::new(p1), Box::new(p2))), Box::new(p3));
    let mut cols = HashSet::new();
    filter.collect_columns(&mut cols);
    assert_eq!(cols.len(), 2);

    let res = filter.eval(&batch);
    assert_eq!(res.len(), 128);
}
