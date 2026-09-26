//! Data handling for Arrow/IPC and Parquet
//! 
//! This module provides utilities for:
//! - Arrow IPC serialization/deserialization
//! - Parquet file writing
//! - Data passage between pipeline stages

use crate::{Result, CoreError};
use arrow::array::{Array, RecordBatch};
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
use std::sync::Arc;
use parking_lot::Mutex;
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
            let options = arrow::ipc::writer::IpcWriteOptions::default();
            let mut writer = FileWriter::try_new(&mut buffer, &schema, Some(options))
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
        let reader = FileReader::try_new(data, None)
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
        use parquet::basic::Compression;
        use std::fs::File;
        use std::io::BufWriter;
        
        // Create partition filename: part-{partition_id}.parquet
        let filename = format!("part-{}.parquet", partition_id);
        let path = std::path::Path::new(&self.output_path).join(&filename);
        
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CoreError::IoError(e))?;
        }
        
        let file = File::create(&path)
            .map_err(|e| CoreError::IoError(e))?;
        let buf_writer = BufWriter::new(file);
        
        let mut writer = ArrowWriter::try_new(
            buf_writer,
            data.schema.clone(),
            Some(parquet::writer::WriterProperties::builder()
                .compression(Compression::SNAPPY)
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
                arrow::array::Int64Array::from(vec![1, 2, 3]),
                arrow::array::Float64Array::from(vec![Some(1.0), Some(2.0), None]),
            ],
        ).unwrap();
        
        let data = ArrowData::new(batch);
        assert_eq!(data.total_rows(), 3);
    }
}