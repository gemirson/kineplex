//! Non-blocking metadata boundary between Data Plane callers and mesh geometry.

use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::geometry::{
    pin_control_thread, AtomicGeodesicTable, MetricSample, MetricTensor, RouteEntry, RouteKey,
    RouteSnapshot,
};

enum GeometryCommand {
    Metric(MetricSample),
    Routes(RouteSnapshot),
}

/// Data Plane port: bounded non-blocking metadata writes and atomic route reads.
#[derive(Clone)]
pub struct DataPlaneGeometry {
    sender: SyncSender<GeometryCommand>,
    routes: Arc<AtomicGeodesicTable>,
}

impl DataPlaneGeometry {
    /// Enqueues a small metric sample. A full channel drops the sample immediately.
    pub fn try_report_metric(&self, sample: MetricSample) -> Result<(), MetricSample> {
        match self.sender.try_send(GeometryCommand::Metric(sample)) {
            Ok(()) => Ok(()),
            Err(_) => Err(sample),
        }
    }

    /// Requests an off-thread route table replacement.
    pub fn try_publish_routes(&self, snapshot: RouteSnapshot) -> Result<(), RouteSnapshot> {
        match self
            .sender
            .try_send(GeometryCommand::Routes(snapshot.clone()))
        {
            Ok(()) => Ok(()),
            Err(_) => Err(snapshot),
        }
    }

    /// Reads the immutable active route without touching a Control Plane lock.
    #[must_use]
    pub fn route(&self, edge: RouteKey) -> Option<RouteEntry> {
        self.routes.lookup(edge)
    }
}

/// Dedicated Control Plane geometry worker and its data-plane metadata port.
pub struct PlaneIsolation {
    tensor: Arc<RwLock<MetricTensor>>,
    routes: Arc<AtomicGeodesicTable>,
    shutdown: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl PlaneIsolation {
    /// Starts one CPU-affined geometry worker and returns a non-blocking Data Plane handle.
    pub fn spawn(
        initial_routes: RouteSnapshot,
        channel_capacity: usize,
    ) -> Result<(Self, DataPlaneGeometry), String> {
        if channel_capacity == 0 {
            return Err("geometry metadata channel capacity must be non-zero".to_owned());
        }
        let (sender, commands) = mpsc::sync_channel(channel_capacity);
        let (shutdown, stop) = mpsc::channel();
        let routes = Arc::new(AtomicGeodesicTable::new(initial_routes));
        let worker_routes = Arc::clone(&routes);
        let tensor = Arc::new(RwLock::new(MetricTensor::default()));
        let worker_tensor = Arc::clone(&tensor);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("kineplex-control-geometry".to_owned())
            .spawn(move || {
                if let Err(error) = pin_control_thread() {
                    let _send_result = ready_tx.send(Err(error));
                    return;
                }
                let _ready_result = ready_tx.send(Ok(()));
                loop {
                    if stop.try_recv().is_ok() {
                        return;
                    }
                    match commands.recv_timeout(Duration::from_millis(10)) {
                        Ok(GeometryCommand::Metric(sample)) => {
                            let current = *worker_tensor
                                .read()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let update =
                                MetricTensor::from_sample(sample, current.revision.wrapping_add(1));
                            *worker_tensor
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = update;
                        }
                        Ok(GeometryCommand::Routes(snapshot)) => worker_routes.publish(snapshot),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        match ready_rx.recv().map_err(|error| error.to_string())? {
            Ok(()) => Ok((
                Self {
                    tensor,
                    routes: Arc::clone(&routes),
                    shutdown: Some(shutdown),
                    worker: Some(worker),
                },
                DataPlaneGeometry { sender, routes },
            )),
            Err(error) => {
                let _join_result = worker.join();
                Err(error)
            }
        }
    }

    /// Reads the latest Control Plane metric tensor for diagnostics.
    #[must_use]
    pub fn metric_snapshot(&self) -> MetricTensor {
        *self
            .tensor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reads the active immutable route revision.
    #[must_use]
    pub fn route_revision(&self) -> u64 {
        self.routes.revision()
    }

    pub fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _join_result = worker.join();
        }
    }
}

impl Drop for PlaneIsolation {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _send_result = shutdown.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _join_result = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PlaneIsolation;
    use crate::geometry::{MetricSample, RouteEntry, RouteSnapshot};
    use std::collections::HashMap;
    use std::time::Duration;

    #[test]
    fn geometry_changes_cross_the_plane_boundary_as_bounded_metadata() {
        let initial = RouteSnapshot {
            revision: 1,
            routes: HashMap::from([(
                1,
                RouteEntry {
                    next_node: 2,
                    resistance: 1.0,
                },
            )]),
        };
        let Ok((control, data)) = PlaneIsolation::spawn(initial, 4) else {
            return;
        };
        assert_eq!(data.route(1).map(|route| route.next_node), Some(2));
        assert!(data
            .try_report_metric(MetricSample {
                latency_ms: 5.0,
                queued_bytes: 128.0,
                cpu_percent: 25.0
            })
            .is_ok());
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(control.metric_snapshot().revision, 1);
        control.shutdown();
    }
}
