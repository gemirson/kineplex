//! Native Arrow execution path for simple relational operations.

use std::error::Error;
use std::fmt::{Display, Formatter};

use arrow::array::{Array, Float32Array, RecordBatch};
use arrow::compute::{filter_record_batch, kernels::cmp::gt};
use arrow::error::ArrowError;

/// A native physical plan operation.
#[derive(Clone, Debug, PartialEq)]
pub enum PhysicalPlan {
    /// Filter a Float32 column using a strict greater-than comparison.
    FilterNode(FilterNode),
}

/// Parameters for a native greater-than filter.
#[derive(Clone, Debug, PartialEq)]
pub struct FilterNode {
    /// Name of the Float32 input column.
    pub column: String,
    /// Keep values strictly greater than this threshold.
    pub threshold: f32,
}

impl PhysicalPlan {
    /// Executes this plan over an Arrow batch and returns a filtered batch.
    pub fn execute(&self, input: &RecordBatch) -> Result<RecordBatch, PhysicalPlanError> {
        match self {
            Self::FilterNode(filter) => filter.execute(input),
        }
    }
}

impl FilterNode {
    /// Runs Arrow's native comparison and filter kernels on a named Float32 column.
    pub fn execute(&self, input: &RecordBatch) -> Result<RecordBatch, PhysicalPlanError> {
        let index = input
            .schema()
            .index_of(&self.column)
            .map_err(PhysicalPlanError::Arrow)?;
        let column = input.column(index);
        let values = column
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| PhysicalPlanError::ColumnType {
                name: self.column.clone(),
                expected: "Float32",
            })?;
        let predicate = gt(values, &Float32Array::new_scalar(self.threshold))
            .map_err(PhysicalPlanError::Arrow)?;
        filter_record_batch(input, &predicate).map_err(PhysicalPlanError::Arrow)
    }
}

/// Failure while resolving or evaluating a native filter plan.
#[derive(Debug)]
pub enum PhysicalPlanError {
    /// Arrow could not resolve or evaluate the requested column.
    Arrow(ArrowError),
    /// The selected column did not have the required type.
    ColumnType {
        name: String,
        expected: &'static str,
    },
}

impl Display for PhysicalPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow(error) => Display::fmt(error, formatter),
            Self::ColumnType { name, expected } => {
                write!(formatter, "column {name:?} must have type {expected}")
            }
        }
    }
}

impl Error for PhysicalPlanError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{FilterNode, PhysicalPlan};

    #[test]
    fn native_filter_preserves_batch_schema_and_matching_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float32Array::from(vec![2.0, 11.0, 15.0]))],
        )
        .expect("batch is valid");
        let output = PhysicalPlan::FilterNode(FilterNode {
            column: "score".to_owned(),
            threshold: 10.0,
        })
        .execute(&batch)
        .expect("filter succeeds");
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("output remains Float32");
        assert_eq!(values.values(), &[11.0, 15.0]);
    }

    #[test]
    fn native_filter_rejects_non_float_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![11]))])
            .expect("batch is valid");
        let error = FilterNode {
            column: "score".to_owned(),
            threshold: 10.0,
        }
        .execute(&batch)
        .expect_err("wrong type is rejected");
        assert!(error.to_string().contains("Float32"));
    }
}
