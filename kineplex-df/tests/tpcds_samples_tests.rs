//! Testes de conformidade e integridade das amostras TPC-DS no KinePlex-DF.

use kineplex_df::catalyst::{ColumnVector, ColumnarBatch, Expression, ScalarValue};
use kineplex_df::memory::AlignedBuffer;
use std::collections::HashSet;

fn c(name: &str) -> Box<Expression> {
    Box::new(Expression::Column(name.to_string()))
}

fn d(val: f64) -> Box<Expression> {
    Box::new(Expression::Literal(ScalarValue::Double(val)))
}

#[test]
fn test_tpcds_ratio_and_rollup_expressions() {
    let sales: Vec<f64> = vec![1000.0, 1500.0, 2000.0, 2500.0];
    let returns: Vec<f64> = vec![50.0, 120.0, 80.0, 150.0];

    let batch = ColumnarBatch::new(
        vec!["sales".to_string(), "returns".to_string()],
        vec![
            ColumnVector::Double(AlignedBuffer::from_slice(&sales)),
            ColumnVector::Double(AlignedBuffer::from_slice(&returns)),
        ],
    );
    assert_eq!(batch.column("sales").len(), 4);

    // DS-Q01: ratio = returns / sales
    let ratio_expr = Expression::Div(c("returns"), c("sales"));
    let mut cols = HashSet::new();
    ratio_expr.collect_columns(&mut cols);
    assert_eq!(cols.len(), 2);

    let res = ratio_expr.eval(&batch);
    assert_eq!(res.len(), 4);
    let slice = res.as_f64_slice();
    assert!((slice[0] - 0.05).abs() < 1e-4);

    // DS-Q03: sales threshold check
    let threshold = Expression::Gte(c("sales"), d(1200.0));
    let mut cols2 = HashSet::new();
    threshold.collect_columns(&mut cols2);
    assert!(cols2.contains("sales"));

    let res2 = threshold.eval(&batch);
    assert_eq!(res2.len(), 4);
}
