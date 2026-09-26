//! Structured, non-blocking observability for KinePlex processes.

use std::env::VarError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Write;

use tracing::subscriber::SetGlobalDefaultError;
use tracing::Span;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "info";

/// Keeps the background tracing worker alive for the process lifetime.
///
/// Dropping this value flushes all pending log records and joins the background
/// writer thread. Applications should retain it in the outermost boot scope.
#[derive(Debug)]
#[must_use = "dropping the guard stops the background tracing worker"]
pub struct TracingGuard {
    _worker_guard: WorkerGuard,
}

/// Error returned when the global tracing subscriber cannot be initialized.
#[derive(Debug)]
pub enum InitTracingError {
    /// `RUST_LOG` contains directives that cannot be parsed.
    InvalidFilter(ParseError),
    /// `RUST_LOG` is not valid Unicode.
    NonUnicodeFilter,
    /// Another component has already installed a global subscriber.
    GlobalSubscriberAlreadySet(SetGlobalDefaultError),
}

impl Display for InitTracingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFilter(error) => write!(formatter, "invalid RUST_LOG filter: {error}"),
            Self::NonUnicodeFilter => formatter.write_str("RUST_LOG must contain valid Unicode"),
            Self::GlobalSubscriberAlreadySet(error) => {
                write!(
                    formatter,
                    "global tracing subscriber is already set: {error}"
                )
            }
        }
    }
}

impl Error for InitTracingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidFilter(error) => Some(error),
            Self::GlobalSubscriberAlreadySet(error) => Some(error),
            Self::NonUnicodeFilter => None,
        }
    }
}

/// Installs the process-wide JSON tracing subscriber with a non-blocking writer.
///
/// Events are formatted as newline-delimited JSON and sent to standard output by a
/// dedicated background thread. `RUST_LOG` controls filtering and defaults to `info`.
/// The writer is explicitly lossy under sustained backpressure so application threads
/// never wait for telemetry I/O.
///
/// The returned [`TracingGuard`] must remain alive until process shutdown. Its `Drop`
/// implementation flushes queued events before the background thread exits.
///
/// # Errors
///
/// Returns [`InitTracingError`] when `RUST_LOG` is invalid or another global tracing
/// subscriber has already been installed.
pub fn init_tracing() -> Result<TracingGuard, InitTracingError> {
    let filter = filter_from_environment()?;
    let (subscriber, worker_guard) = build_json_subscriber(std::io::stdout(), filter);

    tracing::subscriber::set_global_default(subscriber)
        .map_err(InitTracingError::GlobalSubscriberAlreadySet)?;

    Ok(TracingGuard {
        _worker_guard: worker_guard,
    })
}

/// Creates a span that automatically adds graph and task identifiers to nested events.
///
/// Enter this span for synchronous work. For asynchronous work, attach it with
/// [`tracing::Instrument::instrument`] rather than holding an entered span across an
/// `.await` point.
#[must_use]
pub fn task_span(graph_id: u64, task_id: u64) -> Span {
    tracing::info_span!("kineplex.task", graph_id, task_id)
}

fn filter_from_environment() -> Result<EnvFilter, InitTracingError> {
    match std::env::var("RUST_LOG") {
        Ok(directives) => EnvFilter::try_new(directives).map_err(InitTracingError::InvalidFilter),
        Err(VarError::NotPresent) => Ok(EnvFilter::new(DEFAULT_FILTER)),
        Err(VarError::NotUnicode(_)) => Err(InitTracingError::NonUnicodeFilter),
    }
}

fn build_json_subscriber<W>(
    destination: W,
    filter: EnvFilter,
) -> (impl tracing::Subscriber + Send + Sync, WorkerGuard)
where
    W: Write + Send + 'static,
{
    let (writer, worker_guard) = NonBlockingBuilder::default()
        .lossy(true)
        .finish(destination);
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .finish();

    (subscriber, worker_guard)
}

#[cfg(test)]
mod tests {
    use std::io::{Result as IoResult, Write};
    use std::sync::{Arc, Mutex};

    use serde_json::Value;
    use tracing_subscriber::EnvFilter;

    use super::{build_json_subscriber, task_span};

    #[derive(Clone, Default)]
    struct SharedWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl SharedWriter {
        fn contents(&self) -> String {
            let bytes = self
                .bytes
                .lock()
                .expect("the test log buffer mutex must not be poisoned")
                .clone();
            String::from_utf8(bytes).expect("the JSON formatter must write valid UTF-8")
        }
    }

    impl Write for SharedWriter {
        fn write(&mut self, buffer: &[u8]) -> IoResult<usize> {
            self.bytes
                .lock()
                .expect("the test log buffer mutex must not be poisoned")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            Ok(())
        }
    }

    #[test]
    fn configures_non_blocking_json_and_flushes_on_guard_drop() {
        let destination = SharedWriter::default();
        let captured = destination.clone();
        let (subscriber, worker_guard) = build_json_subscriber(destination, EnvFilter::new("info"));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(component = "test", "observability initialized");
        });
        drop(worker_guard);

        let event: Value = serde_json::from_str(captured.contents().trim())
            .expect("the captured log record must be JSON");
        assert_eq!(event["level"], "INFO");
        assert_eq!(event["fields"]["message"], "observability initialized");
        assert_eq!(event["fields"]["component"], "test");
        assert!(event["timestamp"].is_string());
    }

    #[test]
    fn injects_graph_and_task_context_into_json_events() {
        let destination = SharedWriter::default();
        let captured = destination.clone();
        let (subscriber, worker_guard) = build_json_subscriber(destination, EnvFilter::new("info"));

        tracing::subscriber::with_default(subscriber, || {
            let span = task_span(42, 7);
            let _entered = span.enter();
            tracing::info!(peer = "192.168.1.10:8000", "Synapse established");
        });
        drop(worker_guard);

        let event: Value = serde_json::from_str(captured.contents().trim())
            .expect("the captured contextual log record must be JSON");
        assert_eq!(event["fields"]["message"], "Synapse established");
        assert_eq!(event["fields"]["peer"], "192.168.1.10:8000");
        assert_eq!(event["span"]["name"], "kineplex.task");
        assert_eq!(event["span"]["graph_id"], 42);
        assert_eq!(event["span"]["task_id"], 7);
        assert_eq!(event["spans"][0]["graph_id"], 42);
        assert_eq!(event["spans"][0]["task_id"], 7);
    }
}
