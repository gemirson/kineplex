//! Per-synapse, lock-free batching of Arrow record batches.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use arrow::array::RecordBatch;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};

/// Batch accumulation thresholds injected when a synapse task starts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccumulatorConfig {
    /// Flush once estimated Arrow memory reaches this size.
    pub threshold_bytes: usize,
    /// Flush after the first queued batch has waited this long.
    pub threshold_ms: u64,
}

impl AccumulatorConfig {
    /// Creates a validated accumulator configuration.
    pub fn new(threshold_bytes: usize, threshold_ms: u64) -> Result<Self, AccumulatorError> {
        if threshold_bytes == 0 || threshold_ms == 0 {
            return Err(AccumulatorError::InvalidConfig);
        }
        Ok(Self {
            threshold_bytes,
            threshold_ms,
        })
    }

    fn timeout(self) -> Duration {
        Duration::from_millis(self.threshold_ms)
    }
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 64 * 1024,
            threshold_ms: 10,
        }
    }
}

/// Local state for one synapse task. Cloning a RecordBatch retains its Arrow
/// buffers through reference counts, without copying array values.
#[derive(Debug)]
pub struct BatchAccumulator {
    config: AccumulatorConfig,
    batches: VecDeque<RecordBatch>,
    queued_bytes: usize,
}

impl BatchAccumulator {
    /// Creates an empty task-local accumulator.
    #[must_use]
    pub fn new(config: AccumulatorConfig) -> Self {
        Self {
            config,
            batches: VecDeque::new(),
            queued_bytes: 0,
        }
    }

    /// Enqueues a batch and reports whether the byte threshold is reached.
    pub fn push(&mut self, batch: RecordBatch) -> bool {
        let bytes = batch.get_array_memory_size();
        self.queued_bytes = self.queued_bytes.saturating_add(bytes);
        self.batches.push_back(batch);
        self.queued_bytes >= self.config.threshold_bytes
    }

    /// Drains all queued batches and resets the accumulated byte count.
    pub fn flush(&mut self) -> Vec<RecordBatch> {
        self.queued_bytes = 0;
        self.batches.drain(..).collect()
    }

    /// Current number of queued batches.
    #[must_use]
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Whether no batches are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Estimated bytes retained by queued Arrow arrays.
    #[must_use]
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }
}

/// Receives batches, flushing at the configured byte threshold or idle timeout.
///
/// This function is intended to run as the local Tokio task for one synapse;
/// it uses no global state or locks. Each output contains the batches accumulated
/// for one flush. Remaining batches are sent when the input channel closes.
pub async fn run_accumulator(
    mut input: mpsc::Receiver<RecordBatch>,
    output: mpsc::Sender<Vec<RecordBatch>>,
    config: AccumulatorConfig,
) -> Result<(), AccumulatorError> {
    let mut accumulator = BatchAccumulator::new(config);
    let mut timer = Box::pin(sleep(Duration::from_secs(86400)));
    let mut timer_armed = false;

    loop {
        tokio::select! {
            batch = input.recv() => match batch {
                Some(batch) => {
                    let was_empty = accumulator.is_empty();
                    let should_flush = accumulator.push(batch);
                    if was_empty {
                        timer.as_mut().reset(Instant::now() + config.timeout());
                        timer_armed = true;
                    }
                    if should_flush {
                        output.send(accumulator.flush()).await.map_err(|_| AccumulatorError::OutputClosed)?;
                        timer_armed = false;
                    }
                }
                None => {
                    if !accumulator.is_empty() {
                        output.send(accumulator.flush()).await.map_err(|_| AccumulatorError::OutputClosed)?;
                    }
                    return Ok(());
                }
            },
            () = &mut timer, if timer_armed => {
                output.send(accumulator.flush()).await.map_err(|_| AccumulatorError::OutputClosed)?;
                timer_armed = false;
            }
        }
    }
}

/// Accumulator configuration or output-channel failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccumulatorError {
    InvalidConfig,
    OutputClosed,
}

impl Display for AccumulatorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig => formatter.write_str("accumulator thresholds must be non-zero"),
            Self::OutputClosed => formatter.write_str("accumulator output channel was closed"),
        }
    }
}

impl Error for AccumulatorError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{run_accumulator, AccumulatorConfig, BatchAccumulator};

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Float32Array::from(vec![1.0; rows]))])
            .expect("batch is valid")
    }

    #[test]
    fn byte_threshold_flushes_and_resets_state() {
        let one_batch = batch(8_192);
        let bytes = one_batch.get_array_memory_size();
        let config = AccumulatorConfig::new(bytes * 2, 1_000).expect("valid config");
        let mut accumulator = BatchAccumulator::new(config);
        assert!(!accumulator.push(one_batch.clone()));
        assert!(accumulator.push(one_batch));
        assert_eq!(accumulator.flush().len(), 2);
        assert!(accumulator.is_empty());
        assert_eq!(accumulator.queued_bytes(), 0);
    }

    #[tokio::test]
    async fn idle_timeout_flushes_infrequent_batches() {
        let config = AccumulatorConfig::new(usize::MAX, 10).expect("valid config");
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
        let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(run_accumulator(input_rx, output_tx, config));
        input_tx.send(batch(1)).await.expect("input open");
        let flushed = output_rx.recv().await.expect("timeout flush arrives");
        assert_eq!(flushed.len(), 1);
        drop(input_tx);
        task.await
            .expect("task joins")
            .expect("accumulator succeeds");
    }
}
