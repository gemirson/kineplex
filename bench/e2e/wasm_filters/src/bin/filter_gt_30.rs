use arrow::array::{Float32Array, RecordBatch};
use arrow::compute::{filter_record_batch, kernels::cmp::gt};
use kineplex_sdk::synapse;

#[synapse]
fn filter(batch: RecordBatch) -> RecordBatch {
    let Ok(index) = batch.schema().index_of("score") else {
        return batch;
    };
    let Some(values) = batch.column(index).as_any().downcast_ref::<Float32Array>() else {
        return batch;
    };
    let Ok(mask) = gt(values, &Float32Array::new_scalar(30.0)) else {
        return batch;
    };
    filter_record_batch(&batch, &mask).unwrap_or(batch)
}

fn main() {}
