//! Buffer de Escrita Assíncrona para ObjectStore (BufWriter)

use object_store::path::Path;
use object_store::ObjectStore;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Buffer de escrita assíncrona compatível com a API de BufWriter do ObjectStore
pub struct BufWriter {
    store: Arc<dyn ObjectStore>,
    path: Path,
    capacity: usize,
    max_concurrency: usize,
    buffer: Vec<u8>,
    shutdown_task: Option<BoxFuture<'static, std::io::Result<()>>>,
}

impl BufWriter {
    /// Inicializa o BufWriter com capacity configurada.
    pub fn with_capacity(store: Arc<dyn ObjectStore>, path: Path, capacity: usize) -> Self {
        Self {
            store,
            path,
            capacity,
            max_concurrency: 8,
            buffer: Vec::with_capacity(capacity.min(1024 * 1024)),
            shutdown_task: None,
        }
    }

    /// Configura a concorrência máxima de uploads multipartes.
    pub fn with_max_concurrency(mut self, concurrency: usize) -> Self {
        self.max_concurrency = concurrency;
        self
    }

    /// Retorna a capacidade configurada.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Retorna a concorrência configurada.
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

impl AsyncWrite for BufWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.shutdown_task.is_none() {
            let store = Arc::clone(&this.store);
            let path = this.path.clone();
            let data = bytes::Bytes::from(std::mem::take(&mut this.buffer));
            this.shutdown_task = Some(Box::pin(async move {
                store
                    .put(&path, data)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            }));
        }

        if let Some(fut) = this.shutdown_task.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(res) => {
                    this.shutdown_task = None;
                    Poll::Ready(res)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
