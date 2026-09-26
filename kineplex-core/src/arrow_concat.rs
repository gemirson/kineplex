//! Schema-checked consolidation of Arrow micro-batches.

use std::error::Error;
use std::fmt::{Display, Formatter};

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::error::ArrowError;

/// Maximum retained Arrow array bytes for one consolidation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConcatConfig {
    pub max_chunk_bytes: usize,
}

impl ConcatConfig {
    pub const fn new(max_chunk_bytes: usize) -> Self {
        Self { max_chunk_bytes }
    }
}

/// Validates and concatenates micro-batches into one logical record batch.
pub fn concat_record_batches(
    batches: &[RecordBatch],
    config: ConcatConfig,
) -> Result<RecordBatch, ConcatError> {
    let first = batches.first().ok_or(ConcatError::EmptyInput)?;
    let expected_schema = first.schema();
    let mut total_bytes = 0usize;

    for (index, batch) in batches.iter().enumerate() {
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Err(ConcatError::SchemaMismatch { batch_index: index });
        }
        total_bytes = total_bytes
            .checked_add(batch.get_array_memory_size())
            .ok_or(ConcatError::SizeOverflow)?;
        if total_bytes > config.max_chunk_bytes {
            return Err(ConcatError::ChunkTooLarge {
                observed: total_bytes,
                maximum: config.max_chunk_bytes,
            });
        }
    }

    concat_batches(&expected_schema, batches.iter()).map_err(ConcatError::Arrow)
}

/// Validation or Arrow kernel error from a concatenation request.
#[derive(Debug)]
pub enum ConcatError {
    EmptyInput,
    SchemaMismatch { batch_index: usize },
    SizeOverflow,
    ChunkTooLarge { observed: usize, maximum: usize },
    Arrow(ArrowError),
}

impl Display for ConcatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyInput => formatter.write_str("cannot concatenate an empty batch list"),
            Self::SchemaMismatch { batch_index } => {
                write!(
                    formatter,
                    "record batch {batch_index} has a different schema"
                )
            }
            Self::SizeOverflow => formatter.write_str("combined Arrow batch size overflowed usize"),
            Self::ChunkTooLarge { observed, maximum } => write!(
                formatter,
                "combined Arrow batch is {observed} bytes, exceeding limit {maximum}"
            ),
            Self::Arrow(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ConcatError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{concat_record_batches, ConcatConfig, ConcatError};

    fn batch(values: Vec<f32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(values))])
            .expect("batch is valid")
    }

    #[test]
    fn merges_matching_batches_into_one_batch() {
        let batches = vec![batch(vec![1.0, 2.0]), batch(vec![3.0])];
        let result = concat_record_batches(&batches, ConcatConfig::new(1024))
            .expect("schemas match and size fits");
        assert_eq!(result.num_rows(), 3);
        let values = result
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn rejects_schema_mismatch_and_oversized_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Float32,
            false,
        )]));
        let mismatch = RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![3.0]))])
            .expect("batch is valid");
        assert!(matches!(
            concat_record_batches(&[batch(vec![1.0]), mismatch], ConcatConfig::new(1024)),
            Err(ConcatError::SchemaMismatch { batch_index: 1 })
        ));
        assert!(matches!(
            concat_record_batches(&[batch(vec![1.0; 100])], ConcatConfig::new(1)),
            Err(ConcatError::ChunkTooLarge { .. })
        ));
    }
}
