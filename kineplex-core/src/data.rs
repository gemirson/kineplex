//! Data handling for Arrow/IPC and Parquet
//! 
//! This module provides utilities for:
//! - Arrow IPC serialization/deserialization
//! - Parquet file writing
//! - Data passage between pipeline stages

use crate::{Result, CoreError};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use std::io::Cursor;
use std::sync::Arc;
use std::collections::HashMap;

/// Arrow IPC data container for passing between pipeline stages
#[derive(Debug, Clone)]
pub struct ArrowData {
    /// Record batches making up the data
    pub batches: Vec<RecordBatch>,
    /// Schema used
    pub schema: Arc<arrow::datatypes::Schema>,
    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl ArrowData {
    /// Create new ArrowData from a single record batch
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batches: vec![batch],
            schema,
            metadata: HashMap::new(),
        }
    }
    
    /// Create new ArrowData from multiple record batches
    pub fn from_batches(batches: Vec<RecordBatch>, schema: Arc<arrow::datatypes::Schema>) -> Self {
        Self {
            batches,
            schema,
            metadata: HashMap::new(),
        }
    }
    
    /// Serialize to IPC format (Arrow IPC file format)
    pub fn to_ipc(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let schema = self.schema.clone();
        
        // Write schema
        {
            let mut writer = FileWriter::try_new(&mut buffer, &schema)
                .map_err(|e| CoreError::ArrowError(e.to_string()))?;
            
            for batch in &self.batches {
                writer.write(batch)
                    .map_err(|e| CoreError::ArrowError(e.to_string()))?;
            }
            
            writer.finish()
                .map_err(|e| CoreError::ArrowError(e.to_string()))?;
        }
        
        Ok(buffer)
    }
    
    /// Deserialize from IPC format
    pub fn from_ipc(data: &[u8]) -> Result<Self> {
        let reader = FileReader::try_new(Cursor::new(data), None)
            .map_err(|e| CoreError::ArrowError(e.to_string()))?;
        
        let schema = reader.schema();
        let mut batches = Vec::new();
        
        for batch in reader {
            batches.push(batch.map_err(|e| CoreError::ArrowError(e.to_string()))?);
        }
        
        Ok(Self::from_batches(batches, schema))
    }
    
    /// Get total number of rows
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
    
    /// Add metadata entry
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }
}

/// Parquet writer for terminal stage output
pub struct ParquetWriter {
    /// Output path
    output_path: String,
}

impl ParquetWriter {
    /// Create a new ParquetWriter
    pub fn new(output_path: impl Into<String>) -> Self {
        Self {
            output_path: output_path.into(),
        }
    }
    
    /// Write Arrow data to Parquet file
    pub fn write(&self, data: &ArrowData, partition_id: u32) -> Result<String> {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::fs::File;
        use std::io::BufWriter;
        
        // Create partition filename: part-{partition_id}.parquet
        let filename = format!("part-{}.parquet", partition_id);
        let path = std::path::Path::new(&self.output_path).join(&filename);
        
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(CoreError::IoError)?;
        }
        
        let file = File::create(&path)
            .map_err(CoreError::IoError)?;
        let buf_writer = BufWriter::new(file);
        
        let mut writer = ArrowWriter::try_new(
            buf_writer,
            data.schema.clone(),
            Some(WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build())
        ).map_err(|e| CoreError::ParquetError(e.to_string()))?;
        
        for batch in &data.batches {
            writer.write(batch)
                .map_err(|e| CoreError::ParquetError(e.to_string()))?;
        }
        
        writer.close()
            .map_err(|e| CoreError::ParquetError(e.to_string()))?;
        
        Ok(path.to_string_lossy().to_string())
    }
}

/// Data plane node identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Pipeline stage executor
pub trait StageExecutor: Send + Sync {
    /// Execute this stage
    fn execute(&self, input: ArrowData, node_id: &NodeId) -> impl std::future::Future<Output = Result<ArrowData>> + Send;
    
    /// Stage name
    fn name(&self) -> &str;
}

/// Receptor stage - receives input data
pub struct ReceptorStage;

impl StageExecutor for ReceptorStage {
    fn name(&self) -> &str {
        "receptor"
    }
    
    async fn execute(&self, input: ArrowData, _node_id: &NodeId) -> Result<ArrowData> {
        // Receptor stage validates and prepares input
        let mut output = input;
        output.add_metadata("stage", "receptor");
        output.add_metadata("processed_at", chrono::Utc::now().to_rfc3339());
        Ok(output)
    }
}

/// Wasm execution stage
pub struct WasmStage {
    wasm_module: Vec<u8>,
    max_memory_mb: u64,
    max_fuel: u64,
}

impl WasmStage {
    pub fn new(wasm_module: Vec<u8>, max_memory_mb: u64, max_fuel: u64) -> Self {
        Self {
            wasm_module,
            max_memory_mb,
            max_fuel,
        }
    }
}

impl StageExecutor for WasmStage {
    fn name(&self) -> &str {
        "wasm"
    }
    
    async fn execute(&self, input: ArrowData, node_id: &NodeId) -> Result<ArrowData> {
        // This would invoke the actual Wasm runtime
        // For now, we just pass through the data with metadata
        let mut output = input;
        output.add_metadata("stage", "wasm");
        output.add_metadata("executed_by", node_id.0.clone());
        output.add_metadata("wasm_module_bytes", self.wasm_module.len().to_string());
        output.add_metadata("wasm_max_memory_mb", self.max_memory_mb.to_string());
        output.add_metadata("wasm_max_fuel", self.max_fuel.to_string());
        Ok(output)
    }
}

/// Aggregation stage
pub struct AggregationStage;

impl StageExecutor for AggregationStage {
    fn name(&self) -> &str {
        "aggregation"
    }
    
    async fn execute(&self, input: ArrowData, _node_id: &NodeId) -> Result<ArrowData> {
        // Aggregation stage combines data from multiple sources
        let mut output = input;
        output.add_metadata("stage", "aggregation");
        output.add_metadata("aggregated_at", chrono::Utc::now().to_rfc3339());
        Ok(output)
    }
}

/// Terminal stage - produces final output including Parquet
pub struct TerminalStage {
    output_path: String,
}

impl TerminalStage {
    pub fn new(output_path: impl Into<String>) -> Self {
        Self {
            output_path: output_path.into(),
        }
    }
}

impl StageExecutor for TerminalStage {
    fn name(&self) -> &str {
        "terminal"
    }
    
    async fn execute(&self, input: ArrowData, node_id: &NodeId) -> Result<ArrowData> {
        let mut output = input;
        output.add_metadata("stage", "terminal");
        output.add_metadata("executed_by", node_id.0.clone());
        output.add_metadata("output_path", self.output_path.clone());
        output.add_metadata("completed_at", chrono::Utc::now().to_rfc3339());
        
        // Write Parquet output (FT-072)
        let writer = ParquetWriter::new(&self.output_path);
        let path = writer.write(&output, 0)?;
        output.add_metadata("parquet_path", path);
        
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema, DataType};
    
    #[test]
    fn test_arrow_data_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.0), Some(2.0), None])) as Arc<dyn Array>,
            ],
        ).unwrap();
        
        let data = ArrowData::new(batch);
        assert_eq!(data.total_rows(), 3);
    }
}



#[cfg(test)]
mod coverage_tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs;

    fn sample_data() -> ArrowData {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
                Arc::new(arrow::array::Float64Array::from(vec![Some(1.5), None])) as Arc<dyn Array>,
            ],
        ).unwrap();
        ArrowData::from_batches(vec![batch], schema)
    }

    #[test]
    fn ipc_round_trip_and_metadata() {
        let mut data = sample_data();
        data.add_metadata("source", "test");
        assert_eq!(data.metadata.get("source"), Some(&"test".to_string()));
        let encoded = data.to_ipc().unwrap();
        let decoded = ArrowData::from_ipc(&encoded).unwrap();
        assert_eq!(decoded.total_rows(), 2);
        assert_eq!(decoded.batches.len(), 1);
    }

    #[test]
    fn invalid_ipc_returns_arrow_error() {
        let result = ArrowData::from_ipc(b"not-arrow");
        assert!(matches!(result, Err(CoreError::ArrowError(_))));
    }

    #[tokio::test]
    async fn stage_executors_add_stage_metadata() {
        let input = sample_data();
        let node = NodeId::new("node-1");
        let receptor = ReceptorStage.execute(input, &node).await.unwrap();
        assert_eq!(receptor.metadata.get("stage"), Some(&"receptor".to_string()));

        let wasm = WasmStage::new(vec![1, 2, 3], 64, 1000)
            .execute(receptor, &node).await.unwrap();
        assert_eq!(wasm.metadata.get("stage"), Some(&"wasm".to_string()));
        assert_eq!(wasm.metadata.get("wasm_module_bytes"), Some(&"3".to_string()));

        let aggregate = AggregationStage.execute(wasm, &node).await.unwrap();
        assert_eq!(aggregate.metadata.get("stage"), Some(&"aggregation".to_string()));
    }

    #[tokio::test]
    async fn terminal_writes_partitioned_parquet() {
        let path = std::env::temp_dir().join(format!("kineplex-data-{}", uuid::Uuid::new_v4()));
        let output = TerminalStage::new(path.to_string_lossy().to_string())
            .execute(sample_data(), &NodeId::new("node-1")).await.unwrap();
        let parquet_path = output.metadata.get("parquet_path").unwrap();
        assert!(std::path::Path::new(parquet_path).exists());
        assert!(parquet_path.ends_with("part-0.parquet"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn node_id_constructor_preserves_value() {
        assert_eq!(NodeId::new("abc").0, "abc");
    }
}