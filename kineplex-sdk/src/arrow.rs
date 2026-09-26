//! Typed, zero-copy Arrow accessors for SDK users.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

/// Fallible typed access to Arrow columns, retaining borrowed Arrow buffers.
pub trait KineColumnExt {
    fn column_as_i8(&self, name: &str) -> Result<&[i8], KineColumnError>;
    fn column_as_i16(&self, name: &str) -> Result<&[i16], KineColumnError>;
    fn column_as_i32(&self, name: &str) -> Result<&[i32], KineColumnError>;
    fn column_as_i64(&self, name: &str) -> Result<&[i64], KineColumnError>;
    fn column_as_u8(&self, name: &str) -> Result<&[u8], KineColumnError>;
    fn column_as_u16(&self, name: &str) -> Result<&[u16], KineColumnError>;
    fn column_as_u32(&self, name: &str) -> Result<&[u32], KineColumnError>;
    fn column_as_u64(&self, name: &str) -> Result<&[u64], KineColumnError>;
    fn column_as_f32(&self, name: &str) -> Result<&[f32], KineColumnError>;
    fn column_as_f64(&self, name: &str) -> Result<&[f64], KineColumnError>;
    fn column_as_bool(&self, name: &str) -> Result<&BooleanArray, KineColumnError>;
    fn column_as_string(&self, name: &str) -> Result<&StringArray, KineColumnError>;
    fn column_as_large_string(&self, name: &str) -> Result<&LargeStringArray, KineColumnError>;
}

impl KineColumnExt for RecordBatch {
    fn column_as_i8(&self, name: &str) -> Result<&[i8], KineColumnError> {
        typed::<Int8Array>(self, name, "Int8").map(|array| array.values().as_ref())
    }
    fn column_as_i16(&self, name: &str) -> Result<&[i16], KineColumnError> {
        typed::<Int16Array>(self, name, "Int16").map(|array| array.values().as_ref())
    }
    fn column_as_i32(&self, name: &str) -> Result<&[i32], KineColumnError> {
        typed::<Int32Array>(self, name, "Int32").map(|array| array.values().as_ref())
    }
    fn column_as_i64(&self, name: &str) -> Result<&[i64], KineColumnError> {
        typed::<Int64Array>(self, name, "Int64").map(|array| array.values().as_ref())
    }
    fn column_as_u8(&self, name: &str) -> Result<&[u8], KineColumnError> {
        typed::<UInt8Array>(self, name, "UInt8").map(|array| array.values().as_ref())
    }
    fn column_as_u16(&self, name: &str) -> Result<&[u16], KineColumnError> {
        typed::<UInt16Array>(self, name, "UInt16").map(|array| array.values().as_ref())
    }
    fn column_as_u32(&self, name: &str) -> Result<&[u32], KineColumnError> {
        typed::<UInt32Array>(self, name, "UInt32").map(|array| array.values().as_ref())
    }
    fn column_as_u64(&self, name: &str) -> Result<&[u64], KineColumnError> {
        typed::<UInt64Array>(self, name, "UInt64").map(|array| array.values().as_ref())
    }
    fn column_as_f32(&self, name: &str) -> Result<&[f32], KineColumnError> {
        typed::<Float32Array>(self, name, "Float32").map(|array| array.values().as_ref())
    }
    fn column_as_f64(&self, name: &str) -> Result<&[f64], KineColumnError> {
        typed::<Float64Array>(self, name, "Float64").map(|array| array.values().as_ref())
    }
    fn column_as_bool(&self, name: &str) -> Result<&BooleanArray, KineColumnError> {
        typed(self, name, "Boolean")
    }
    fn column_as_string(&self, name: &str) -> Result<&StringArray, KineColumnError> {
        typed(self, name, "Utf8")
    }
    fn column_as_large_string(&self, name: &str) -> Result<&LargeStringArray, KineColumnError> {
        typed(self, name, "LargeUtf8")
    }
}

fn typed<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    expected: &'static str,
) -> Result<&'a T, KineColumnError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| KineColumnError::MissingColumn(name.to_owned()))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| KineColumnError::WrongType {
            name: name.to_owned(),
            expected,
            actual: batch.column(index).data_type().clone(),
        })
}

/// Missing column or physical Arrow type mismatch.
#[derive(Clone, Debug, PartialEq)]
pub enum KineColumnError {
    MissingColumn(String),
    WrongType {
        name: String,
        expected: &'static str,
        actual: DataType,
    },
}

impl std::fmt::Display for KineColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingColumn(name) => write!(f, "column {name:?} does not exist"),
            Self::WrongType {
                name,
                expected,
                actual,
            } => write!(f, "column {name:?} is {actual:?}, expected {expected}"),
        }
    }
}

impl std::error::Error for KineColumnError {}

/// Builder for a RecordBatch from typed Arrow arrays.
#[derive(Default)]
pub struct KineRecordBatchBuilder {
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
}

impl KineRecordBatchBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a column without copying its underlying Arrow buffers.
    pub fn column(
        mut self,
        name: impl Into<String>,
        nullable: bool,
        values: ArrayRef,
    ) -> Result<Self, KineColumnError> {
        let name = name.into();
        self.fields
            .push(Field::new(name, values.data_type().clone(), nullable));
        self.columns.push(values);
        Ok(self)
    }

    pub fn build(self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(std::sync::Arc::new(Schema::new(self.fields)), self.columns)
    }
}

#[cfg(test)]
mod tests {
    use super::{KineColumnExt, KineRecordBatchBuilder};
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn typed_accessors_cover_arrow_primitive_and_string_arrays_without_copying() {
        let fields = vec![
            Field::new("i8", DataType::Int8, false),
            Field::new("i16", DataType::Int16, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, false),
            Field::new("u8", DataType::UInt8, false),
            Field::new("u16", DataType::UInt16, false),
            Field::new("u32", DataType::UInt32, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("str", DataType::Utf8, false),
            Field::new("large", DataType::LargeUtf8, false),
        ];
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![1])),
            Arc::new(Int16Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(UInt8Array::from(vec![5])),
            Arc::new(UInt16Array::from(vec![6])),
            Arc::new(UInt32Array::from(vec![7])),
            Arc::new(UInt64Array::from(vec![8])),
            Arc::new(Float32Array::from(vec![9.0])),
            Arc::new(Float64Array::from(vec![10.0])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec!["text"])),
            Arc::new(LargeStringArray::from(vec!["large"])),
        ];
        let batch = arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("batch is valid");
        assert_eq!(batch.column_as_i8("i8").expect("Int8"), &[1]);
        assert_eq!(batch.column_as_i16("i16").expect("Int16"), &[2]);
        assert_eq!(batch.column_as_i32("i32").expect("Int32"), &[3]);
        assert_eq!(batch.column_as_i64("i64").expect("Int64"), &[4]);
        assert_eq!(batch.column_as_u8("u8").expect("UInt8"), &[5]);
        assert_eq!(batch.column_as_u16("u16").expect("UInt16"), &[6]);
        assert_eq!(batch.column_as_u32("u32").expect("UInt32"), &[7]);
        assert_eq!(batch.column_as_u64("u64").expect("UInt64"), &[8]);
        assert_eq!(batch.column_as_f32("f32").expect("Float32"), &[9.0]);
        assert_eq!(batch.column_as_f64("f64").expect("Float64"), &[10.0]);
        assert_eq!(
            batch.column_as_bool("bool").expect("Boolean").value(0),
            true
        );
        assert_eq!(
            batch.column_as_string("str").expect("Utf8").value(0),
            "text"
        );
        assert_eq!(
            batch
                .column_as_large_string("large")
                .expect("LargeUtf8")
                .value(0),
            "large"
        );
    }

    #[test]
    fn type_mismatch_is_an_error_and_builder_keeps_array_buffers() {
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![2.0]))],
        )
        .expect("batch valid");
        assert!(batch
            .column_as_i32("value")
            .unwrap_err()
            .to_string()
            .contains("Float64"));
        let array = Arc::new(Int32Array::from(vec![1, 2]));
        let built = KineRecordBatchBuilder::new()
            .column("items", false, array.clone())
            .unwrap()
            .build()
            .unwrap();
        assert!(Arc::ptr_eq(built.column(0), &(array as ArrayRef)));
    }
}
