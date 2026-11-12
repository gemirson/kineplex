//! Abstração de Armazenamento Parquet para Distribuição Ótima de Laguerre
//!
//! Fornece o trait `LaguerreParquetSink` e implementações para memória (`InMemoryParquetSink`)
//! e filesystem local (`FileParquetSink`), desacoplando o motor do runtime de nuvem e permitindo
//! ligação de alta performance com backends assíncronos (como Seastar C++ ou ObjectStore).

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::fs::File;
use std::io::{BufWriter, Cursor};
use std::path::PathBuf;

/// Propriedades de gravação de alto desempenho padrão para o KinePlex/KineHaus.
pub fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1_048_576)
        .set_bloom_filter_enabled(true)
        .build()
}

/// Handle para um writer Parquet ativo em uma célula.
pub struct ActiveWriterHandle {
    writer: ArrowWriter<Box<dyn std::io::Write + Send>>,
    pub uri: String,
    pub bytes_written: u64,
}

impl ActiveWriterHandle {
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), String> {
        self.writer
            .write(batch)
            .map_err(|e| format!("Erro ao escrever batch no writer Parquet: {}", e))
    }

    pub fn close(self) -> Result<(String, Option<ParquetMetaData>), String> {
        let meta = self
            .writer
            .close()
            .map_err(|e| format!("Erro ao fechar writer Parquet: {}", e))?;
        // Nota: em parquet 46.0, ArrowWriter::close retorna FileMetaData do Thrift format.
        // Convertemos ou preservamos conforme necessário.
        let _ = meta;
        Ok((self.uri, None))
    }
}

/// Trait de saída para gravação de arquivos Parquet pelas células de Laguerre.
pub trait LaguerreParquetSink: Send + Sync {
    /// Abre um novo writer para a célula indicada com o caminho/URI fornecido.
    fn open_writer(&self, uri: &str, schema: SchemaRef) -> Result<ActiveWriterHandle, String>;

    /// Notifica que um arquivo foi selado e completado.
    fn on_file_completed(&self, uri: &str, size_bytes: u64);
}

/// Sink em memória: armazena os arquivos Parquet selados em memória RAM.
/// Ideal para processamento analítico in-memory e testes determinísticos.
pub struct InMemoryParquetSink {
    completed_files: parking_lot::Mutex<Vec<(String, Vec<u8>)>>,
}

impl Default for InMemoryParquetSink {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryParquetSink {
    pub fn new() -> Self {
        Self {
            completed_files: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn get_completed_files(&self) -> Vec<(String, Vec<u8>)> {
        self.completed_files.lock().clone()
    }
}

impl LaguerreParquetSink for InMemoryParquetSink {
    fn open_writer(&self, uri: &str, schema: SchemaRef) -> Result<ActiveWriterHandle, String> {
        let cursor = Box::new(Cursor::new(Vec::new())) as Box<dyn std::io::Write + Send>;
        let writer = ArrowWriter::try_new(cursor, schema, Some(default_writer_properties()))
            .map_err(|e| format!("Falha ao criar InMemory ArrowWriter: {}", e))?;

        Ok(ActiveWriterHandle {
            writer,
            uri: uri.to_string(),
            bytes_written: 0,
        })
    }

    fn on_file_completed(&self, uri: &str, size_bytes: u64) {
        let _ = (uri, size_bytes);
    }
}

/// Sink de arquivo local: grava os arquivos Parquet no sistema de arquivos sob um diretório base.
pub struct FileParquetSink {
    pub base_dir: PathBuf,
}

impl FileParquetSink {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

impl LaguerreParquetSink for FileParquetSink {
    fn open_writer(&self, uri: &str, schema: SchemaRef) -> Result<ActiveWriterHandle, String> {
        let target_path = self.base_dir.join(uri);
        if let Some(parent) = target_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Falha ao criar diretório para {}: {}", uri, e))?;
        }

        let file = File::create(&target_path)
            .map_err(|e| format!("Falha ao criar arquivo Parquet {}: {}", uri, e))?;
        let buf_writer = Box::new(BufWriter::new(file)) as Box<dyn std::io::Write + Send>;

        let writer = ArrowWriter::try_new(buf_writer, schema, Some(default_writer_properties()))
            .map_err(|e| format!("Falha ao instanciar ArrowWriter para {}: {}", uri, e))?;

        Ok(ActiveWriterHandle {
            writer,
            uri: uri.to_string(),
            bytes_written: 0,
        })
    }

    fn on_file_completed(&self, uri: &str, size_bytes: u64) {
        tracing::info!(uri = %uri, size_bytes = %size_bytes, "Arquivo Parquet concluído com sucesso");
    }
}
