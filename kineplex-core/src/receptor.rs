//! Async, backpressure-aware Arrow sources for Parquet and CSV files.

use std::io::Cursor;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::datatypes::SchemaRef;
use futures_util::Stream;
use futures_util::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, watch};

type ParquetStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, ParquetError>> + Send>>;

/// Async local source that yields bounded Arrow record batches.
pub enum Receptor {
    Parquet(ParquetStream),
    Csv(CsvSource),
}

impl Receptor {
    /// Opens a Parquet file and configures the maximum rows per streamed batch.
    pub async fn open_parquet(
        path: impl AsRef<Path>,
        batch_rows: usize,
    ) -> Result<Self, ReceptorError> {
        if batch_rows == 0 {
            return Err(ReceptorError::InvalidBatchSize);
        }
        let file = File::open(path).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .map_err(|error| ReceptorError::Parquet(error.to_string()))?;
        let stream = builder
            .with_batch_size(batch_rows)
            .build()
            .map_err(|error| ReceptorError::Parquet(error.to_string()))?;
        Ok(Self::Parquet(Box::pin(stream)))
    }

    /// Opens CSV with a caller-provided schema; parsing is paged by record count.
    pub async fn open_csv(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        batch_rows: usize,
        has_header: bool,
    ) -> Result<Self, ReceptorError> {
        Ok(Self::Csv(
            CsvSource::open(path, schema, batch_rows, has_header).await?,
        ))
    }

    /// Loads one bounded batch. Calling this only when downstream is ready applies backpressure.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ReceptorError> {
        match self {
            Self::Parquet(stream) => match stream.as_mut().next().await {
                Some(batch) => batch
                    .map(Some)
                    .map_err(|error| ReceptorError::Parquet(error.to_string())),
                None => Ok(None),
            },
            Self::Csv(source) => source.next_batch().await,
        }
    }

    /// Pipes batches through a bounded channel and pauses reads while `paused` is true.
    pub async fn pipe(
        &mut self,
        output: &mpsc::Sender<RecordBatch>,
        mut paused: watch::Receiver<bool>,
    ) -> Result<(), ReceptorError> {
        loop {
            while *paused.borrow() {
                paused
                    .changed()
                    .await
                    .map_err(|_| ReceptorError::ThrottleClosed)?;
            }
            match self.next_batch().await? {
                Some(batch) => output
                    .send(batch)
                    .await
                    .map_err(|_| ReceptorError::OutputClosed)?,
                None => return Ok(()),
            }
        }
    }
}

struct CsvSource {
    reader: BufReader<File>,
    schema: SchemaRef,
    batch_rows: usize,
    exhausted: bool,
}

impl CsvSource {
    async fn open(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        batch_rows: usize,
        has_header: bool,
    ) -> Result<Self, ReceptorError> {
        if batch_rows == 0 {
            return Err(ReceptorError::InvalidBatchSize);
        }
        let mut reader = BufReader::new(File::open(path).await?);
        if has_header {
            let mut header = Vec::new();
            reader.read_until(b'\n', &mut header).await?;
        }
        Ok(Self {
            reader,
            schema,
            batch_rows,
            exhausted: false,
        })
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ReceptorError> {
        if self.exhausted {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        let mut line = Vec::new();
        for _ in 0..self.batch_rows {
            line.clear();
            let read = self.reader.read_until(b'\n', &mut line).await?;
            if read == 0 {
                self.exhausted = true;
                break;
            }
            bytes.extend_from_slice(&line);
        }
        if bytes.is_empty() {
            return Ok(None);
        }
        let mut reader = CsvReaderBuilder::new(Arc::clone(&self.schema))
            .with_header(false)
            .with_batch_size(self.batch_rows)
            .build(Cursor::new(bytes))
            .map_err(|error| ReceptorError::Csv(error.to_string()))?;
        reader
            .next()
            .transpose()
            .map_err(|error| ReceptorError::Csv(error.to_string()))
    }
}

/// File, parser, throttle, or output-channel failure in a Receptor.
#[derive(Debug)]
pub enum ReceptorError {
    Io(std::io::Error),
    Csv(String),
    Parquet(String),
    InvalidBatchSize,
    ThrottleClosed,
    OutputClosed,
}

impl std::fmt::Display for ReceptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "receptor file I/O failed: {error}"),
            Self::Csv(error) => write!(f, "CSV batch parsing failed: {error}"),
            Self::Parquet(error) => write!(f, "Parquet batch reading failed: {error}"),
            Self::InvalidBatchSize => f.write_str("receptor batch row count must be non-zero"),
            Self::ThrottleClosed => f.write_str("receptor backpressure channel was closed"),
            Self::OutputClosed => f.write_str("receptor output channel was closed"),
        }
    }
}

impl std::error::Error for ReceptorError {}

impl From<std::io::Error> for ReceptorError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::Receptor;

    #[tokio::test]
    async fn csv_source_reads_rows_in_bounded_pages() {
        let path = std::env::temp_dir().join(format!(
            "kineplex-receptor-{}-{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after epoch")
                .as_nanos()
        ));
        tokio::fs::write(&path, "score\n1.0\n2.0\n3.0\n")
            .await
            .expect("fixture file writes");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        let mut source = Receptor::open_csv(&path, schema, 2, true)
            .await
            .expect("CSV source opens");
        assert_eq!(
            source
                .next_batch()
                .await
                .expect("first page")
                .expect("batch")
                .num_rows(),
            2
        );
        assert_eq!(
            source
                .next_batch()
                .await
                .expect("second page")
                .expect("batch")
                .num_rows(),
            1
        );
        assert!(source.next_batch().await.expect("EOF").is_none());
        tokio::fs::remove_file(path)
            .await
            .expect("fixture is removed");
    }
}
