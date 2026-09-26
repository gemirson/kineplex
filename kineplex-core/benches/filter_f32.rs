use std::sync::Arc;

use arrow::array::{Float32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kineplex_core::physical_plan::{FilterNode, PhysicalPlan};

fn filter_ten_million_f32(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "score",
        DataType::Float32,
        false,
    )]));
    let values = Float32Array::from_iter_values((0..10_000_000).map(|value| value as f32));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("valid benchmark batch");
    let plan = PhysicalPlan::FilterNode(FilterNode {
        column: "score".to_owned(),
        threshold: 5_000_000.0,
    });

    c.bench_function("arrow_filter_f32_10m", |bencher| {
        bencher.iter(|| plan.execute(black_box(&batch)).expect("filter succeeds"));
    });
}

criterion_group!(benches, filter_ten_million_f32);
criterion_main!(benches);
