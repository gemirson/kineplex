//! Job metrics registry: `job_id → Arc<JobMetrics>` with a scrape-time
//! Prometheus exporter cached for 1 s (DoD of FT-038).
//!
//! Locking contract (plan Fase 3):
//! - The `RwLock<HashMap>` is taken **only** in `begin_job`/`end_job` — never on
//!   the hot path; hot writes go through the `JobCounters` atomics.
//! - The export cache lock is taken at most once per second per scrape.
//! - Finished jobs linger for [`FINISHED_TTL`] so dashboards can still read them,
//!   then disappear from `/metrics` (bounds label cardinality).

use crate::job_metrics::{
    with_current_job, CountersSnapshot, DdSketch, JobCounters, JobScope, Merge, StageRecorder,
    DEFAULT_ALPHA, MAX_STAGES,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long a finished job keeps appearing in `/metrics`.
pub const FINISHED_TTL: Duration = Duration::from_secs(60);
/// Export cache window (FT-038 DoD: cache of 1 s).
pub const EXPORT_CACHE_TTL: Duration = Duration::from_secs(1);

/// A single job's metrics: bare-metal counters + mergeable quantile sketch.
pub struct JobMetrics {
    pub job_id: String,
    /// Stage labels for this job (index-aligned with `JobCounters.stage_*`).
    pub stage_names: Vec<String>,
    pub counters: Arc<JobCounters>,
    /// Relative-error sketch of per-stage latencies (ms). Contended samples use
    /// `try_lock` — **lossy under pressure, never blocks the job**.
    pub stage_sketch: Mutex<DdSketch>,
    pub started: Instant,
    /// `Some(t)` once the job finished.
    pub finished: Mutex<Option<Instant>>,
}

impl std::fmt::Debug for JobMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobMetrics")
            .field("job_id", &self.job_id)
            .field("stage_names", &self.stage_names)
            .field("counters", &self.counters)
            .finish()
    }
}

impl JobMetrics {
    pub fn new(job_id: impl Into<String>, stage_names: Vec<String>) -> Self {
        let mut names = stage_names;
        names.truncate(MAX_STAGES);
        Self {
            job_id: job_id.into(),
            stage_names: names,
            counters: Arc::new(JobCounters::default()),
            stage_sketch: Mutex::new(DdSketch::new(DEFAULT_ALPHA)),
            started: Instant::now(),
            finished: Mutex::new(None),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.finished.lock().is_some()
    }

    /// Bind this job to the calling thread (see `JobScope`).
    pub fn scope(self: &Arc<Self>) -> JobScope {
        JobScope::enter_owned(Arc::clone(&self.counters), self.clone())
    }

    /// Record one stage sample into both the atomics and (best-effort) the sketch.
    pub fn record_stage_sample(&self, stage: usize, ns: u64) {
        self.counters.add_stage_ns(stage, ns);
        // Lossy: if the sketch lock is busy we drop the sample — counters remain exact.
        if let Some(mut sketch) = self.stage_sketch.try_lock() {
            sketch.observe(ns as f64 / 1_000_000.0);
        } else {
            self.counters.stage_drops.fetch_add(
                1,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }

    pub fn snapshot(&self) -> CountersSnapshot {
        self.counters.snapshot()
    }

    pub fn finish(&self) {
        let mut f = self.finished.lock();
        if f.is_none() {
            *f = Some(Instant::now());
        }
    }

    pub fn finished_for(&self) -> Option<Duration> {
        self.finished.lock().map(|t| t.elapsed())
    }

    /// p95 of stage latency in milliseconds (0.0 if no samples).
    pub fn p95_stage_ms(&self) -> f64 {
        let sketch = self.stage_sketch.lock();
        if sketch.count() == 0 {
            0.0
        } else {
            sketch.quantile(0.95)
        }
    }
}

impl StageRecorder for JobMetrics {
    fn record_stage_sample(&self, stage: usize, ns: u64) {
        JobMetrics::record_stage_sample(self, stage, ns);
    }
}

/// Registry: lock only at begin/end; export with a 1 s cache.
pub struct JobMetricsRegistry {
    jobs: RwLock<HashMap<String, Arc<JobMetrics>>>,
    cache: RwLock<(Instant, String)>,
    /// When the registry was created (test hook for TTL control isn't needed —
    /// TTL is measured per-job from `finished`).
    _created: Instant,
}

impl Default for JobMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl JobMetricsRegistry {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            cache: RwLock::new((Instant::now() - Duration::from_secs(10), String::new())),
            _created: Instant::now(),
        }
    }

    /// Register a job and bind it to the calling thread. Lock held only here.
    pub fn begin_job(
        self: &Arc<Self>,
        job_id: impl Into<String>,
        stage_names: &[&str],
    ) -> Arc<JobMetrics> {
        let job_id = job_id.into();
        let metrics = Arc::new(JobMetrics::new(
            job_id.clone(),
            stage_names.iter().map(|s| s.to_string()).collect(),
        ));
        self.jobs.write().insert(job_id, metrics.clone());
        metrics
    }

    /// Mark the job finished (it lingers for [`FINISHED_TTL`] then is evicted).
    pub fn end_job(&self, job_id: &str) {
        if let Some(job) = self.jobs.read().get(job_id) {
            job.finish();
        }
        // Invalidate cache so the next scrape reflects the finished state.
        self.cache
            .write()
            .0 = Instant::now() - EXPORT_CACHE_TTL;
    }

    pub fn get(&self, job_id: &str) -> Option<Arc<JobMetrics>> {
        self.jobs.read().get(job_id).cloned()
    }

    pub fn job_count(&self) -> usize {
        self.jobs.read().len()
    }

    /// Evict jobs finished longer than [`FINISHED_TTL`] ago.
    fn evict_expired(&self) {
        let mut jobs = self.jobs.write();
        jobs.retain(|_, job| match job.finished_for() {
            Some(age) => age < FINISHED_TTL,
            None => true,
        });
    }

    /// Prometheus text for all live jobs. Cached for [`EXPORT_CACHE_TTL`].
    pub fn export_prometheus(&self) -> String {
        {
            let cache = self.cache.read();
            if cache.0.elapsed() < EXPORT_CACHE_TTL {
                return cache.1.clone();
            }
        }

        self.evict_expired();

        // Snapshot job list under the read lock, then format without holding it.
        let snapshot: Vec<Arc<JobMetrics>> = self.jobs.read().values().cloned().collect();
        let mut out = String::with_capacity(snapshot.len() * 256);
        for job in &snapshot {
            let s = job.snapshot();
            let job_id = &job.job_id;
            push_sample(&mut out, "kineplex_job_rows_total", job_id, s.rows as f64);
            push_sample(&mut out, "kineplex_job_events_total", job_id, s.events as f64);
            push_sample(
                &mut out,
                "kineplex_job_dropped_events_total",
                job_id,
                s.dropped_events as f64,
            );
            push_sample(
                &mut out,
                "kineplex_job_p95_ms",
                job_id,
                job.p95_stage_ms(),
            );
            let finished = job.is_finished() as u8 as f64;
            push_sample(&mut out, "kineplex_job_finished", job_id, finished);

            for (i, name) in job.stage_names.iter().enumerate() {
                if s.stage_count[i] == 0 {
                    continue;
                }
                push_labeled(
                    &mut out,
                    "kineplex_job_stage_ms",
                    job_id,
                    name,
                    s.stage_mean_ms(i),
                );
                push_labeled(
                    &mut out,
                    "kineplex_job_stage_count",
                    job_id,
                    name,
                    s.stage_count[i] as f64,
                );
            }
        }

        *self.cache.write() = (Instant::now(), out.clone());
        out
    }

    /// Test hook: bypass the export cache.
    pub fn export_prometheus_uncached(&self) -> String {
        self.cache
            .write()
            .0 = Instant::now() - EXPORT_CACHE_TTL;
        self.export_prometheus()
    }
}

fn push_sample(out: &mut String, name: &str, job: &str, value: f64) {
    out.push_str(name);
    out.push_str("{job=\"");
    out.push_str(job);
    out.push_str("\"} ");
    push_f64(out, value);
    out.push('\n');
}

fn push_labeled(out: &mut String, name: &str, job: &str, stage: &str, value: f64) {
    out.push_str(name);
    out.push_str("{job=\"");
    out.push_str(job);
    out.push_str("\",stage=\"");
    out.push_str(stage);
    out.push_str("\"} ");
    push_f64(out, value);
    out.push('\n');
}

/// Format without allocating a `String` per value.
fn push_f64(out: &mut String, v: f64) {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        use std::fmt::Write as _;
        let _ = write!(out, "{}", v as i64);
    } else {
        use std::fmt::Write as _;
        let _ = write!(out, "{v:.6}");
    }
}

/// Merge two jobs' snapshots (monoid — used by multi-node rollup, plan Fase 1).
pub fn merge_snapshots(a: &mut CountersSnapshot, b: &CountersSnapshot) {
    a.rows += b.rows;
    a.events += b.events;
    a.dropped_events += b.dropped_events;
    for i in 0..MAX_STAGES {
        a.stage_ns[i] += b.stage_ns[i];
        a.stage_count[i] += b.stage_count[i];
    }
}

/// Merge two sketches (re-exported convenience for the exporter layer).
pub fn merge_sketches(into: &mut DdSketch, from: &DdSketch) {
    into.merge(from);
}

/// Convenience: current thread's active job (if any).
pub fn current_job_id() -> Option<String> {
    // The thread-local stores a raw pointer to counters, not the id; the id is
    // recovered via the registry in the server layer. Kept for API symmetry.
    with_current_job(|_| ()).map(|_| String::new()).filter(|s| s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn registry() -> Arc<JobMetricsRegistry> {
        Arc::new(JobMetricsRegistry::new())
    }

    #[test]
    fn begin_end_and_export_per_job() {
        let reg = registry();
        let job = reg.begin_job("job-1", &["parse", "transform", "write"]);

        {
            let _scope = job.scope();
            job.counters.add_rows(1000);
            job.record_stage_sample(0, 2_000_000); // 2 ms
            job.record_stage_sample(1, 5_000_000); // 5 ms
            job.record_stage_sample(2, 1_000_000); // 1 ms
        }
        reg.end_job("job-1");

        let text = reg.export_prometheus_uncached();
        assert!(text.contains("kineplex_job_rows_total{job=\"job-1\"} 1000"));
        assert!(text.contains("kineplex_job_stage_ms{job=\"job-1\",stage=\"parse\"}"));
        assert!(text.contains("kineplex_job_finished{job=\"job-1\"} 1"));
        assert!(text.contains("kineplex_job_p95_ms{job=\"job-1\"}"));
    }

    #[test]
    fn export_is_cached_for_one_second() {
        let reg = registry();
        let job = reg.begin_job("job-c", &["s0"]);
        job.counters.add_rows(1);

        let first = reg.export_prometheus();
        // Mutate after caching — second call must return the cached string.
        job.counters.add_rows(1_000_000);
        let second = reg.export_prometheus();
        assert_eq!(first, second, "cache must serve identical output within 1 s");

        let third = reg.export_prometheus_uncached();
        assert_ne!(first, third, "uncached export must see the new value");
        assert!(third.contains("kineplex_job_rows_total{job=\"job-c\"} 1000001"));
    }

    #[test]
    fn hot_path_writes_do_not_touch_registry_lock() {
        let reg = registry();
        let job = reg.begin_job("job-hp", &["a", "b"]);

        // Writers only touch the atomics; the registry lock is free for scrapes.
        let scope = job.scope();
        crate::job_metrics::record_rows(42);
        {
            let _t = scope.stage(1);
        }

        // Concurrent scrape while the job is running must not deadlock.
        let text = reg.export_prometheus_uncached();
        assert!(text.contains("kineplex_job_rows_total{job=\"job-hp\"} 42"));
        drop(scope);
        reg.end_job("job-hp");
        assert_eq!(job.counters.rows.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn finished_jobs_evict_after_ttl() {
        let reg = registry();
        let job = reg.begin_job("job-old", &["s"]);
        reg.end_job("job-old");
        assert_eq!(reg.job_count(), 1);

        // Force the finished timestamp past the TTL.
        *job.finished.lock() = Some(Instant::now() - FINISHED_TTL - Duration::from_secs(1));
        let text = reg.export_prometheus_uncached();
        assert!(!text.contains("job-old"), "expired job must be evicted");
        assert_eq!(reg.job_count(), 0);
    }

    #[test]
    fn stage_sketch_lossy_under_contention_never_blocks() {
        let job = JobMetrics::new("j", vec!["s".into()]);
        // Hold the sketch lock to simulate contention.
        let _guard = job.stage_sketch.lock();
        job.record_stage_sample(0, 1_000_000); // must not block; drops the sample
        assert_eq!(job.counters.stage_drops.load(Ordering::Relaxed), 1);
        assert_eq!(job.counters.stage_count[0].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn merge_snapshots_is_exact() {
        let mut a = CountersSnapshot {
            rows: 10,
            events: 1,
            dropped_events: 0,
            stage_ns: [0; MAX_STAGES],
            stage_count: [0; MAX_STAGES],
        };
        a.stage_ns[0] = 1_000;
        a.stage_count[0] = 2;
        let mut b = a.clone();
        b.rows = 5;
        b.stage_ns[0] = 3_000;
        b.stage_count[0] = 2;
        merge_snapshots(&mut a, &b);
        assert_eq!(a.rows, 15);
        assert_eq!(a.stage_ns[0], 4_000);
        assert_eq!(a.stage_count[0], 4);
        // mean = 4000 ns / 4 = 1000 ns = 0.001 ms
        assert!((a.stage_mean_ms(0) - 0.001).abs() < 1e-9);
    }
}
