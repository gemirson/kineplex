//! Arrow C Data Interface exports with explicit ownership of every exported buffer.

use arrow::array::{Array, RecordBatch};
use arrow::error::Result as ArrowResult;
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

/// Keeps Arrow C Data Interface values alive for the duration of a host FFI call.
///
/// The pointers embedded in these structures belong to the host process. They
/// are suitable for native C Data Interface consumers and must not be treated
/// as addresses in a Wasmtime linear memory.
pub struct ArrowBatchFfi {
    columns: Vec<(FFI_ArrowArray, FFI_ArrowSchema)>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Float32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::ArrowBatchFfi;

    #[test]
    fn ffi_export_keeps_each_array_and_named_schema_alive() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
        )
        .expect("batch is valid");
        let exported = ArrowBatchFfi::from_record_batch(&batch).expect("FFI export succeeds");
        assert_eq!(exported.column_count(), 1);
        assert!(exported.column(0).is_some());
    }
}

impl ArrowBatchFfi {
    /// Converts all columns in `batch` to Arrow C Data Interface structures.
    pub fn from_record_batch(batch: &RecordBatch) -> ArrowResult<Self> {
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let (array, _datatype_schema) = to_ffi(&column.to_data())?;
                let schema = FFI_ArrowSchema::try_from(batch.schema().field(index))?;
                Ok((array, schema))
            })
            .collect::<ArrowResult<Vec<_>>>()?;
        Ok(Self { columns })
    }

    /// Number of columns retained by this export.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns borrowed ABI structures for a column.
    #[must_use]
    pub fn column(&self, index: usize) -> Option<(&FFI_ArrowArray, &FFI_ArrowSchema)> {
        self.columns
            .get(index)
            .map(|(array, schema)| (array, schema))
    }
}
