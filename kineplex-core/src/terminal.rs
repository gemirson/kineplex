//! Async Parquet sink with bounded-memory file rotation.

use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::errors::ParquetError;
use tokio::fs::{self, File};
use tokio::sync::{mpsc, watch};

/// Summary returned after a clean EOF flush.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalSummary {
    pub files: Vec<PathBuf>,
    pub batches_written: u64,
    pub rows_written: u64,
}

/// Rotating asynchronous Parquet writer for one graph terminal.
pub struct ParquetTerminal {
    directory: PathBuf,
    schema: SchemaRef,
    max_file_bytes: usize,
    next_part: u64,
    writer: Option<AsyncArrowWriter<File>>,
    files: Vec<PathBuf>,
    batches_written: u64,
    rows_written: u64,
    finished: bool,
}

impl ParquetTerminal {
    /// Creates the output directory and configures the rotation threshold.
    pub async fn create(
        directory: impl AsRef<Path>,
        schema: SchemaRef,
        max_file_bytes: usize,
    ) -> Result<Self, TerminalError> {
        if max_file_bytes == 0 {
            return Err(TerminalError::InvalidRotationSize);
        }
        fs::create_dir_all(directory.as_ref()).await?;
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
            schema,
            max_file_bytes,
            next_part: 0,
            writer: None,
            files: Vec::new(),
            batches_written: 0,
            rows_written: 0,
            finished: false,
        })
    }

    /// Writes one batch and rotates after the Parquet writer reaches its size target.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), TerminalError> {
        if self.finished {
            return Err(TerminalError::AlreadyFinished);
        }
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(TerminalError::SchemaMismatch);
        }
        self.ensure_writer().await?;
        let writer = self
            .writer
            .as_mut()
            .ok_or(TerminalError::WriterUnavailable)?;
        writer.write(batch).await?;
        writer.flush().await?;
        self.batches_written = self.batches_written.saturating_add(1);
        self.rows_written = self.rows_written.saturating_add(batch.num_rows() as u64);
        if writer.bytes_written() >= self.max_file_bytes {
            self.close_part().await?;
        }
        Ok(())
    }

    /// Consumes a streaming batch channel until EOF is signaled, then closes the writer.
    pub async fn consume(
        mut self,
        mut batches: mpsc::Receiver<RecordBatch>,
        mut eof: watch::Receiver<bool>,
    ) -> Result<TerminalSummary, TerminalError> {
        loop {
            tokio::select! {
                changed = eof.changed() => {
                    if changed.is_err() { return Err(TerminalError::EofSignalClosed); }
                    if *eof.borrow() {
                        while let Ok(batch) = batches.try_recv() {
                            self.write_batch(&batch).await?;
                        }
                        return self.finish().await;
                    }
                }
                batch = batches.recv() => match batch {
                    Some(batch) => self.write_batch(&batch).await?,
                    None => return self.finish().await,
                }
            }
        }
    }

    /// Consumes Arrow IPC streams from the network-facing channel until EOF.
    pub async fn consume_ipc(
        mut self,
        mut payloads: mpsc::Receiver<Vec<u8>>,
        mut eof: watch::Receiver<bool>,
    ) -> Result<TerminalSummary, TerminalError> {
        loop {
            tokio::select! {
                changed = eof.changed() => {
                    if changed.is_err() { return Err(TerminalError::EofSignalClosed); }
                    if *eof.borrow() {
                        while let Ok(payload) = payloads.try_recv() {
                            self.write_ipc(&payload).await?;
                        }
                        return self.finish().await;
                    }
                }
                payload = payloads.recv() => match payload {
                    Some(payload) => self.write_ipc(&payload).await?,
                    None => return self.finish().await,
                }
            }
        }
    }

    async fn write_ipc(&mut self, payload: &[u8]) -> Result<(), TerminalError> {
        let reader = StreamReader::try_new(Cursor::new(payload), None)
            .map_err(|error| TerminalError::Arrow(error.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|error| TerminalError::Arrow(error.to_string()))?;
            self.write_batch(&batch).await?;
        }
        Ok(())
    }

    /// Flushes and closes the final part. Safe to call once.
    pub async fn finish(mut self) -> Result<TerminalSummary, TerminalError> {
        if let Some(writer) = self.writer.take() {
            let path = self.current_path();
            writer.close().await?;
            self.files.push(path);
            self.next_part += 1;
        }
        self.finished = true;
        Ok(TerminalSummary {
            files: self.files,
            batches_written: self.batches_written,
            rows_written: self.rows_written,
        })
    }

    async fn ensure_writer(&mut self) -> Result<(), TerminalError> {
        if self.writer.is_some() {
            return Ok(());
        }
        let path = self.current_path();
        let file = File::create(&path).await?;
        self.writer = Some(AsyncArrowWriter::try_new(
            file,
            Arc::clone(&self.schema),
            None,
        )?);
        Ok(())
    }

    async fn close_part(&mut self) -> Result<(), TerminalError> {
        if let Some(writer) = self.writer.take() {
            let path = self.current_path();
            writer.close().await?;
            self.files.push(path);
            self.next_part += 1;
        }
        Ok(())
    }

    fn current_path(&self) -> PathBuf {
        self.directory
            .join(format!("part-{:04}.parquet", self.next_part))
    }
}

/// File, Parquet, schema, or lifecycle error from a Terminal.
#[derive(Debug)]
pub enum TerminalError {
    Io(std::io::Error),
    Parquet(ParquetError),
    Arrow(String),
    InvalidRotationSize,
    SchemaMismatch,
    WriterUnavailable,
    AlreadyFinished,
    EofSignalClosed,
}

impl std::fmt::Display for TerminalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "terminal file I/O failed: {error}"),
            Self::Parquet(error) => write!(f, "terminal Parquet write failed: {error}"),
            Self::Arrow(error) => write!(f, "terminal Arrow IPC decode failed: {error}"),
            Self::InvalidRotationSize => f.write_str("terminal rotation size must be non-zero"),
            Self::SchemaMismatch => {
                f.write_str("terminal batch schema differs from its writer schema")
            }
            Self::WriterUnavailable => f.write_str("terminal Parquet writer is unavailable"),
            Self::AlreadyFinished => f.write_str("terminal writer is already finished"),
            Self::EofSignalClosed => f.write_str("terminal EOF signal channel closed unexpectedly"),
        }
    }
}

impl std::error::Error for TerminalError {}

impl From<std::io::Error> for TerminalError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ParquetError> for TerminalError {
    fn from(value: ParquetError) -> Self {
        Self::Parquet(value)
    }
}
