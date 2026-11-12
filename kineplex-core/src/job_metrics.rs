//! Per-job metrics: mergeable monoids, streaming statistics and bare-metal hot path.
//!
//! Three mathematical theses (see docs/plans/metrics-budget-controller.md):
//!
//! 1. **Every metric value is a mergeable monoid** (`Merge`): associative, commutative,
//!    with an identity element. Merging is how aggregates combine across threads and
//!    nodes without coordination or shipping raw samples (CRDT-style).
//! 2. **Sufficient statistics, not raw samples**: `Welford` (online mean/variance) and
//!    `DdSketch` (relative-error quantiles, guaranteed error ≤ α) preserve the inference
//!    we need in O(1)/O(1/α) memory.
//! 3. **Hot path is bare-metal**: one cache-line-aligned struct of `AtomicU64` relaxed
//!    counters per job, reached through a thread-local pointer — no locks, no allocation,
//!    no formatting on the write path.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Overhead budget for metrics collection, as a fraction of job CPU time.
/// Contract: instrumentation must stay at or below this ratio (Fase 4 enforces it
/// with a feedback controller).
pub const OVERHEAD_BUDGET: f64 = 0.01;

/// Default relative accuracy of quantile sketches (α = 1%).
pub const DEFAULT_ALPHA: f64 = 0.01;

/// Maximum number of distinct stages per job (fixed array, no allocation).
pub const MAX_STAGES: usize = 16;

// ---------------------------------------------------------------------------
// Hot path (Fase 2): bare-metal per-job counters
// ---------------------------------------------------------------------------

/// Per-job write-side counters. One allocation per job, cache-line aligned to
/// avoid false sharing when several jobs land on the same line.
///
/// Write path contract: **zero locks, zero allocation, zero formatting** —
/// every mutation is a single `fetch_add(Relaxed)` (~1 ns) or `fetch_max`.
#[repr(align(64))]
pub struct JobCounters {
    /// Rows produced/consumed by the job.
    pub rows: AtomicU64,
    /// Discrete events recorded (may be sampled — see `sampled_events`).
    pub events: AtomicU64,
    /// Events dropped by the lossy sampler (never blocks the job).
    pub dropped_events: AtomicU64,
    /// Per-stage accumulated nanoseconds (`stage_ns[i]` ↔ `stage_count[i]`).
    pub stage_ns: [AtomicU64; MAX_STAGES],
    pub stage_count: [AtomicU64; MAX_STAGES],
    /// Stage timing samples discarded so far (budget controller pressure valve).
    pub stage_drops: AtomicU64,
}

impl Default for JobCounters {
    fn default() -> Self {
        Self {
            rows: AtomicU64::new(0),
            events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            stage_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            stage_count: std::array::from_fn(|_| AtomicU64::new(0)),
            stage_drops: AtomicU64::new(0),
        }
    }
}

impl std::fmt::Debug for JobCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCounters")
            .field("rows", &self.rows.load(Ordering::Relaxed))
            .field("events", &self.events.load(Ordering::Relaxed))
            .field("dropped_events", &self.dropped_events.load(Ordering::Relaxed))
            .finish()
    }
}

impl JobCounters {
    #[inline]
    pub fn add_rows(&self, n: u64) {
        self.rows.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_event(&self) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn drop_event(&self) {
        self.dropped_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one stage sample in nanoseconds. `stage` is clamped to `MAX_STAGES`
    /// (out-of-range stages land in the last slot — never panic on the hot path).
    #[inline]
    pub fn add_stage_ns(&self, stage: usize, ns: u64) {
        let i = stage.min(MAX_STAGES - 1);
        self.stage_ns[i].fetch_add(ns, Ordering::Relaxed);
        self.stage_count[i].fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot for the exporter (called only on scrape, never on the hot path).
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            rows: self.rows.load(Ordering::Relaxed),
            events: self.events.load(Ordering::Relaxed),
            dropped_events: self.dropped_events.load(Ordering::Relaxed),
            stage_ns: std::array::from_fn(|i| self.stage_ns[i].load(Ordering::Relaxed)),
            stage_count: std::array::from_fn(|i| self.stage_count[i].load(Ordering::Relaxed)),
        }
    }
}

/// Immutable copy of a job's counters, produced at scrape time.
#[derive(Debug, Clone)]
pub struct CountersSnapshot {
    pub rows: u64,
    pub events: u64,
    pub dropped_events: u64,
    pub stage_ns: [u64; MAX_STAGES],
    pub stage_count: [u64; MAX_STAGES],
}

impl CountersSnapshot {
    /// Mean duration of stage `i` in milliseconds (0.0 if never sampled).
    pub fn stage_mean_ms(&self, i: usize) -> f64 {
        if i >= MAX_STAGES || self.stage_count[i] == 0 {
            return 0.0;
        }
        self.stage_ns[i] as f64 / self.stage_count[i] as f64 / 1_000_000.0
    }
}

thread_local! {
    /// Raw pointer to the job active on *this* thread. The `JobScope` guard owns
    /// the `Arc` and clears this on drop, so the pointee is always alive while set.
    static CURRENT_JOB: Cell<*const JobCounters> = const { Cell::new(std::ptr::null()) };
}

/// Set the current thread's job (called by `JobScope::enter`).
/// # Safety
/// `ptr` must remain valid until `clear_current_job()` runs on the same thread.
#[inline]
pub unsafe fn set_current_job(ptr: *const JobCounters) {
    CURRENT_JOB.with(|c| c.set(ptr));
}

#[inline]
pub fn clear_current_job() {
    CURRENT_JOB.with(|c| c.set(std::ptr::null()));
}

/// Borrow the current thread's job counters, if any.
#[inline]
pub fn with_current_job<R>(f: impl FnOnce(&JobCounters) -> R) -> Option<R> {
    CURRENT_JOB.with(|c| {
        let ptr = c.get();
        if ptr.is_null() {
            None
        } else {
            // SAFETY: set by JobScope::enter which keeps the Arc alive; cleared on drop.
            Some(unsafe { f(&*ptr) })
        }
    })
}

/// Hot-path row accounting for the current job (no-op if no job is active).
#[inline]
pub fn record_rows(n: u64) {
    with_current_job(|jc| jc.add_rows(n));
}

/// Hot-path event accounting for the current job (no-op if no job is active).
#[inline]
pub fn record_event() {
    with_current_job(|jc| jc.add_event());
}

/// Records a completed stage sample (implemented by `JobMetrics` so the
/// exporter can also feed the latency sketch).
pub trait StageRecorder: Send + Sync {
    fn record_stage_sample(&self, stage: usize, ns: u64);
}

/// RAII scope binding a job to the current thread and providing stage timers.
///
/// Dropping the scope clears the thread-local. Re-entering (e.g. a tokio task
/// migrating threads) simply re-sets the pointer — see plan risk "thread-local
/// não vê job em task migratória": the setter runs at the *start of each stage*.
pub struct JobScope {
    counters: Arc<JobCounters>,
    /// Optional richer recorder (e.g. `JobMetrics` with its sketch). When `None`,
    /// stage samples only hit the atomics.
    recorder: Option<Arc<dyn StageRecorder>>,
}

impl JobScope {
    pub fn enter(counters: Arc<JobCounters>) -> Self {
        // SAFETY: we hold the Arc; the pointer is cleared in `Drop` before the
        // Arc can be released.
        unsafe { set_current_job(Arc::as_ptr(&counters)) };
        Self {
            counters,
            recorder: None,
        }
    }

    /// Bind a job whose owner also wants stage samples (sketch, etc.).
    pub fn enter_owned(
        counters: Arc<JobCounters>,
        recorder: Arc<dyn StageRecorder>,
    ) -> Self {
        unsafe { set_current_job(Arc::as_ptr(&counters)) };
        Self {
            counters,
            recorder: Some(recorder),
        }
    }

    #[inline]
    pub fn counters(&self) -> &JobCounters {
        &self.counters
    }

    /// Start timing a stage. The returned guard records elapsed nanoseconds
    /// into the job's counters when dropped.
    #[inline]
    pub fn stage(&self, stage: usize) -> StageTimer<'_> {
        StageTimer {
            counters: &self.counters,
            recorder: self.recorder.as_deref(),
            stage,
            start: Instant::now(),
        }
    }
}

impl Drop for JobScope {
    fn drop(&mut self) {
        clear_current_job();
    }
}

/// Guard that measures a stage's wall time via `Instant::now()` (vDSO on Linux).
#[must_use = "stage timer records its measurement on drop"]
pub struct StageTimer<'a> {
    counters: &'a JobCounters,
    recorder: Option<&'a dyn StageRecorder>,
    stage: usize,
    start: Instant,
}

impl Drop for StageTimer<'_> {
    #[inline]
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos() as u64;
        match self.recorder {
            Some(r) => r.record_stage_sample(self.stage, ns),
            // Plain counters path (no sketch).
            None => self.counters.add_stage_ns(self.stage, ns),
        }
    }
}


/// A value that can be merged with another of the same type.
///
/// Laws (verified by tests in `tests/job_metrics.rs`):
/// - **Associativity**: `(a ⋅ b) ⋅ c = a ⋅ (b ⋅ c)`
/// - **Commutativity**: `a ⋅ b = b ⋅ a`
/// - **Identity**: `identity() ⋅ a = a`
pub trait Merge {
    fn merge(&mut self, other: &Self);
    fn identity() -> Self;
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// Monotonic counter backed by a relaxed atomic (hot-path friendly).
///
/// `fetch_add(Relaxed)` is ~1 ns and needs no synchronization on x86-64/ARM64
/// for plain counters; merging is integer addition.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    #[inline]
    pub fn inc(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        let c = Self::default();
        c.inc(self.get());
        c
    }
}

impl Merge for Counter {
    fn merge(&mut self, other: &Self) {
        self.value
            .fetch_add(other.value.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    fn identity() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// Welford — online mean / variance
// ---------------------------------------------------------------------------

/// Welford's online algorithm: one-pass, numerically stable mean and variance.
///
/// Merge is the parallel (Chan) combination of two partial aggregates, so shards
/// can compute independently and combine exactly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Welford {
    count: u64,
    mean: f64,
    m2: f64,
}

impl Welford {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Add one observation (hot path: plain arithmetic, no allocation).
    #[inline]
    pub fn observe(&mut self, x: f64) {
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.mean
        }
    }

    /// Unbiased sample variance (Bessel-corrected). 0.0 when count < 2.
    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / (self.count - 1) as f64
        }
    }

    pub fn stddev(&self) -> f64 {
        self.variance().sqrt()
    }
}

impl Default for Welford {
    fn default() -> Self {
        Self::new()
    }
}

impl Merge for Welford {
    /// Chan's parallel variance combination — exact, order-independent.
    fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }
        let total = self.count + other.count;
        let delta = other.mean - self.mean;
        let combined_mean = self.mean + delta * (other.count as f64 / total as f64);
        let combined_m2 = self.m2
            + other.m2
            + delta * delta * (self.count as f64 * other.count as f64 / total as f64);
        self.count = total;
        self.mean = combined_mean;
        self.m2 = combined_m2;
    }

    fn identity() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// DDSketch — relative-error quantiles (mergeable)
// ---------------------------------------------------------------------------

/// DDSketch: bucket index `i = floor(ln v / ln(1+α))` guarantees that the relative
/// error of any returned quantile is ≤ α (α = 1% by default).
///
/// `merge` is element-wise bucket addition — associative and commutative by
/// construction of integer addition.
///
/// Values ≤ 0 are tracked in `zero_count` (exact). Bucket range is bounded by
/// `MAX_BUCKETS`; overflow/underflow land in the last/first bucket, keeping
/// memory O(1) for any input magnitude (log-scale clamping, as in DDSketch).
pub struct DdSketch {
    alpha: f64,
    log_gamma: f64,
    zero_count: u64,
    /// Sparse representation: bucket index (biased by `INDEX_OFFSET`) → count.
    buckets: std::collections::BTreeMap<i32, u64>,
    total: u64,
}

/// Bias added to bucket indices so they fit in `i32` (covers e^-647 .. e^647).
const INDEX_OFFSET: i32 = 1_000_000;
/// Hard cap on distinct buckets (memory bound); α=1% needs ~1400 for real ranges.
const MAX_BUCKETS: usize = 4096;

impl DdSketch {
    pub fn new(alpha: f64) -> Self {
        assert!(
            alpha > 0.0 && alpha < 1.0,
            "alpha must be in (0, 1), got {alpha}"
        );
        Self {
            alpha,
            log_gamma: (1.0 + alpha).ln(),
            zero_count: 0,
            buckets: std::collections::BTreeMap::new(),
            total: 0,
        }
    }

    /// Index for value `v > 0`: floor(ln v / ln(1+α)), biased to be non-negative.
    #[inline]
    fn index_of(&self, v: f64) -> i32 {
        let i = (v.ln() / self.log_gamma).floor();
        // Clamp to the representable range to bound memory.
        let i = i.clamp(-(INDEX_OFFSET as f64), INDEX_OFFSET as f64 - 1.0);
        (i as i32) + INDEX_OFFSET
    }

    /// Representative value of a bucket (its geometric midpoint).
    #[inline]
    fn value_of(&self, index: i32) -> f64 {
        let i = (index - INDEX_OFFSET) as f64;
        // Midpoint of [ (1+α)^i , (1+α)^(i+1) ) in log space.
        ((i + 0.5) * self.log_gamma).exp()
    }

    /// Record one non-negative observation.
    #[inline]
    pub fn observe(&mut self, v: f64) {
        self.total += 1;
        if v <= 0.0 || !v.is_finite() {
            self.zero_count += 1;
            return;
        }
        let idx = self.index_of(v);
        let count = self.buckets.entry(idx).or_insert(0);
        *count += 1;
        // Memory bound: if we exceed MAX_BUCKETS, collapse into coarser neighbours.
        if self.buckets.len() > MAX_BUCKETS {
            self.coarsen();
        }
    }

    /// Halve the resolution (merge adjacent buckets) to enforce the memory bound.
    fn coarsen(&mut self) {
        let mut new: std::collections::BTreeMap<i32, u64> = std::collections::BTreeMap::new();
        for (idx, count) in std::mem::take(&mut self.buckets) {
            let shifted = if idx - INDEX_OFFSET >= 0 {
                INDEX_OFFSET + (idx - INDEX_OFFSET) / 2
            } else {
                INDEX_OFFSET + (idx - INDEX_OFFSET - 1) / 2
            };
            *new.entry(shifted).or_insert(0) += count;
        }
        self.buckets = new;
    }

    pub fn count(&self) -> u64 {
        self.total
    }

    /// Approximate quantile `q ∈ [0, 1]`. Relative error ≤ α (until coarsening kicks in,
    /// which only ever increases error toward 2α — documented trade-off for O(1) memory).
    pub fn quantile(&self, q: f64) -> f64 {
        assert!((0.0..=1.0).contains(&q), "q must be in [0,1]");
        if self.total == 0 {
            return 0.0;
        }
        if self.zero_count > 0 && q <= (self.zero_count as f64 / self.total as f64) {
            return 0.0;
        }
        let rank = q * self.total as f64;
        let mut cum = self.zero_count as f64;
        for (&idx, &count) in &self.buckets {
            cum += count as f64;
            if cum >= rank {
                return self.value_of(idx);
            }
        }
        // q == 1.0 overshoot: return largest bucket value.
        self.buckets
            .iter()
            .next_back()
            .map(|(&idx, _)| self.value_of(idx))
            .unwrap_or(0.0)
    }
}

impl PartialEq for DdSketch {
    fn eq(&self, other: &Self) -> bool {
        self.alpha == other.alpha
            && self.zero_count == other.zero_count
            && self.total == other.total
            && self.buckets == other.buckets
    }
}

impl Merge for DdSketch {
    fn merge(&mut self, other: &Self) {
        debug_assert!(
            (self.alpha - other.alpha).abs() < 1e-12,
            "merging sketches with different alpha: {} vs {}",
            self.alpha,
            other.alpha
        );
        self.total += other.total;
        self.zero_count += other.zero_count;
        for (&idx, &count) in &other.buckets {
            *self.buckets.entry(idx).or_insert(0) += count;
        }
        if self.buckets.len() > MAX_BUCKETS {
            self.coarsen();
        }
    }

    fn identity() -> Self {
        // alpha value is irrelevant for the empty sketch; 1% is the default.
        Self::new(DEFAULT_ALPHA)
    }
}

impl std::fmt::Debug for DdSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdSketch")
            .field("alpha", &self.alpha)
            .field("count", &self.total)
            .field("buckets", &self.buckets.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic LCG so property tests are reproducible (no external crate).
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn next_f64(&mut self) -> f64 {
            (self.next() >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    fn check_merge_laws<A: Merge + PartialEq + std::fmt::Debug + CloneForTest>(
        a: &A,
        b: &A,
        c: &A,
        name: &str,
    ) {
        // Associativity: (a⋅b)⋅c == a⋅(b⋅c)
        let mut ab_c = a.clone_for_test();
        ab_c.merge(b);
        ab_c.merge(c);

        let mut a_bc = a.clone_for_test();
        let mut bc = b.clone_for_test();
        bc.merge(c);
        a_bc.merge(&bc);
        assert_eq!(ab_c, a_bc, "{name}: associativity violated");

        // Commutativity: a⋅b == b⋅a
        let mut ab = a.clone_for_test();
        ab.merge(b);
        let mut ba = b.clone_for_test();
        ba.merge(a);
        assert_eq!(ab, ba, "{name}: commutativity violated");

        // Identity: identity()⋅a == a
        let mut id_a = A::identity();
        id_a.merge(a);
        assert_eq!(id_a, *a, "{name}: identity law violated");
    }

    /// Laws for `f64`-valued aggregates: floating-point addition is only
    /// *associative up to rounding*, so we assert within a relative tolerance.
    fn check_merge_laws_approx(a: &Welford, b: &Welford, c: &Welford) {
        let close = |x: &Welford, y: &Welford, what: &str| {
            assert_eq!(x.count(), y.count(), "Welford {what}: count");
            let rel = |p: f64, q: f64| {
                let denom = p.abs().max(q.abs()).max(1.0);
                ((p - q) / denom).abs()
            };
            assert!(
                rel(x.mean(), y.mean()) < 1e-12,
                "Welford {what}: mean {} vs {}",
                x.mean(),
                y.mean()
            );
            assert!(
                rel(x.variance(), y.variance()) < 1e-9,
                "Welford {what}: variance {} vs {}",
                x.variance(),
                y.variance()
            );
        };

        let mut ab_c = *a;
        ab_c.merge(b);
        ab_c.merge(c);
        let mut bc = *b;
        bc.merge(c);
        let mut a_bc = *a;
        a_bc.merge(&bc);
        close(&ab_c, &a_bc, "associativity");

        let mut ab = *a;
        ab.merge(b);
        let mut ba = *b;
        ba.merge(a);
        close(&ab, &ba, "commutativity");

        let mut id_a = Welford::identity();
        id_a.merge(a);
        close(&id_a, a, "identity");
    }

    /// Helper trait so the generic law-checker works with `Copy`-free types too.
    trait CloneForTest: Sized {
        fn clone_for_test(&self) -> Self;
    }
    impl CloneForTest for Welford {
        fn clone_for_test(&self) -> Self {
            *self
        }
    }
    impl CloneForTest for DdSketch {
        fn clone_for_test(&self) -> Self {
            let mut s = DdSketch::new(self.alpha);
            s.merge(self);
            s
        }
    }

    #[test]
    fn counter_laws() {
        let a = {
            let c = Counter::default();
            c.inc(3);
            c
        };
        let b = {
            let c = Counter::default();
            c.inc(7);
            c
        };
        let c_val = {
            let c = Counter::default();
            c.inc(11);
            c
        };
        let (a, b, c_val) = (a, b, c_val);

        // Counter is not Clone/PartialEq — check values directly.
        let mut ab_c = a.clone_for_test();
        ab_c.merge(&b);
        ab_c.merge(&c_val);
        let mut bc = b.clone_for_test();
        bc.merge(&c_val);
        let mut a_bc = a.clone_for_test();
        a_bc.merge(&bc);
        assert_eq!(ab_c.get(), a_bc.get(), "counter associativity");

        let mut ab = a.clone_for_test();
        ab.merge(&b);
        let mut ba = b.clone_for_test();
        ba.merge(&a);
        assert_eq!(ab.get(), ba.get(), "counter commutativity");

        let mut id_a = Counter::identity();
        id_a.merge(&a);
        assert_eq!(id_a.get(), a.get(), "counter identity");
    }

    impl CloneForTest for Counter {
        fn clone_for_test(&self) -> Self {
            let c = Counter::default();
            c.inc(self.get());
            c
        }
    }

    #[test]
    fn welford_matches_bruteforce_moments() {
        let mut rng = Lcg(42);
        let mut w = Welford::new();
        let mut xs = Vec::new();
        for _ in 0..10_000 {
            let x = rng.next_f64() * 1000.0 - 500.0;
            xs.push(x);
            w.observe(x);
        }
        let n = xs.len() as f64;
        let mean = xs.iter().sum::<f64>() / n;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        assert!((w.mean() - mean).abs() < 1e-9, "mean mismatch");
        assert!((w.variance() - var).abs() < 1e-6, "variance mismatch");
    }

    #[test]
    fn welford_merge_equals_single_pass() {
        let mut rng = Lcg(7);
        let xs: Vec<f64> = (0..5_000).map(|_| rng.next_f64() * 100.0).collect();

        let mut single = Welford::new();
        for &x in &xs {
            single.observe(x);
        }

        let mut left = Welford::new();
        let mut right = Welford::new();
        for (i, &x) in xs.iter().enumerate() {
            if i % 2 == 0 {
                left.observe(x);
            } else {
                right.observe(x);
            }
        }
        left.merge(&right);

        assert_eq!(left.count(), single.count());
        assert!((left.mean() - single.mean()).abs() < 1e-9);
        assert!((left.variance() - single.variance()).abs() < 1e-9);
    }

    #[test]
    fn welford_merge_laws_property() {
        let mut rng = Lcg(1234);
        for _ in 0..50 {
            let mut a = Welford::new();
            let mut b = Welford::new();
            let mut c = Welford::new();
            for _ in 0..rng.next() % 30 {
                a.observe(rng.next_f64() * 10.0);
            }
            for _ in 0..rng.next() % 30 {
                b.observe(rng.next_f64() * 10.0);
            }
            for _ in 0..rng.next() % 30 {
                c.observe(rng.next_f64() * 10.0);
            }
            check_merge_laws_approx(&a, &b, &c);
        }
    }

    #[test]
    fn ddsketch_relative_error_within_alpha() {
        let alpha = 0.01;
        let mut sketch = DdSketch::new(alpha);
        // Deterministic dataset: 1..=100_000 ms.
        let data: Vec<f64> = (1..=100_000).map(|v| v as f64).collect();
        for &v in &data {
            sketch.observe(v);
        }

        let mut sorted = data.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        for &q in &[0.5f64, 0.9, 0.95, 0.99] {
            let exact = sorted[((q * sorted.len() as f64).ceil() as usize - 1).min(sorted.len() - 1)];
            let approx = sketch.quantile(q);
            let rel_err = ((approx - exact) / exact).abs();
            assert!(
                rel_err <= alpha + 1e-12,
                "q={q}: exact={exact}, approx={approx}, rel_err={rel_err} > α={alpha}"
            );
        }
    }

    #[test]
    fn ddsketch_merge_preserves_quantiles() {
        let alpha = 0.01;
        let data: Vec<f64> = (1..=20_000).map(|v| v as f64).collect();

        let mut whole = DdSketch::new(alpha);
        let mut left = DdSketch::new(alpha);
        let mut right = DdSketch::new(alpha);
        for (i, &v) in data.iter().enumerate() {
            whole.observe(v);
            if i % 2 == 0 {
                left.observe(v);
            } else {
                right.observe(v);
            }
        }
        left.merge(&right);

        assert_eq!(left.count(), whole.count());
        for &q in &[0.5f64, 0.95, 0.99] {
            let a = whole.quantile(q);
            let b = left.quantile(q);
            let rel = ((a - b) / a).abs();
            assert!(rel <= 2.0 * alpha, "merged quantile drift: {a} vs {b}");
        }
    }

    #[test]
    fn ddsketch_merge_laws_property() {
        let mut rng = Lcg(99);
        for _ in 0..30 {
            let mut a = DdSketch::new(DEFAULT_ALPHA);
            let mut b = DdSketch::new(DEFAULT_ALPHA);
            let mut c = DdSketch::new(DEFAULT_ALPHA);
            for _ in 0..rng.next() % 50 {
                a.observe(rng.next_f64() * 5000.0);
            }
            for _ in 0..rng.next() % 50 {
                b.observe(rng.next_f64() * 5000.0);
            }
            for _ in 0..rng.next() % 50 {
                c.observe(rng.next_f64() * 5000.0);
            }
            check_merge_laws(&a, &b, &c, "DdSketch");
        }
    }

    #[test]
    fn ddsketch_handles_zero_and_extremes() {
        let mut s = DdSketch::new(DEFAULT_ALPHA);
        s.observe(0.0);
        s.observe(-5.0);
        s.observe(f64::INFINITY);
        s.observe(1e-300);
        s.observe(1e300);
        assert_eq!(s.count(), 5);
        assert!(s.quantile(0.0) >= 0.0);
        assert!(s.quantile(1.0).is_finite());
    }

    #[test]
    fn overhead_budget_is_one_percent() {
        assert!((OVERHEAD_BUDGET - 0.01).abs() < f64::EPSILON);
        assert!((DEFAULT_ALPHA - 0.01).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // Fase 2 — hot path
    // -----------------------------------------------------------------------

    #[test]
    fn hot_path_10m_writes_under_one_second() {
        let counters = Arc::new(JobCounters::default());
        let scope = JobScope::enter(counters.clone());

        let t0 = Instant::now();
        for _ in 0..10_000_000 {
            record_rows(1);
        }
        let elapsed = t0.elapsed();
        drop(scope);

        assert_eq!(counters.rows.load(Ordering::Relaxed), 10_000_000);
        assert!(
            elapsed.as_secs() < 1,
            "10M hot-path writes took {elapsed:?}, budget is < 1s"
        );
    }

    #[test]
    fn stage_timer_records_elapsed_time() {
        let counters = Arc::new(JobCounters::default());
        let scope = JobScope::enter(counters.clone());

        {
            let _t = scope.stage(0);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        {
            let _t = scope.stage(1);
            std::thread::sleep(std::time::Duration::from_millis(3));
        }
        drop(scope);

        let snap = counters.snapshot();
        assert_eq!(snap.stage_count[0], 1);
        assert_eq!(snap.stage_count[1], 1);
        assert!(
            snap.stage_ns[0] >= 5_000_000,
            "stage 0 measured {} ns, expected ≥ 5ms",
            snap.stage_ns[0]
        );
        assert!(snap.stage_ns[1] >= 3_000_000);
        assert!(snap.stage_mean_ms(0) >= 5.0);
    }

    #[test]
    fn stage_out_of_range_is_clamped_not_panicking() {
        let counters = Arc::new(JobCounters::default());
        let scope = JobScope::enter(counters.clone());
        {
            let _t = scope.stage(999);
        }
        drop(scope);
        let snap = counters.snapshot();
        assert_eq!(snap.stage_count[MAX_STAGES - 1], 1);
    }

    #[test]
    fn eight_threads_million_writes_each_no_race() {
        use std::sync::atomic::AtomicUsize;

        let counters = Arc::new(JobCounters::default());
        let done = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let counters = counters.clone();
                let done = done.clone();
                std::thread::spawn(move || {
                    let scope = JobScope::enter(counters);
                    for i in 0..1_000_000u64 {
                        record_rows(1);
                        if i % 1000 == 0 {
                            record_event();
                        }
                    }
                    // Stage timing from every thread.
                    {
                        let _t = scope.stage(2);
                    }
                    drop(scope);
                    done.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("worker thread panicked");
        }

        assert_eq!(done.load(Ordering::SeqCst), 8);
        let snap = counters.snapshot();
        assert_eq!(snap.rows, 8_000_000, "rows must be exact across threads");
        assert_eq!(snap.events, 8 * 1000);
        assert_eq!(snap.stage_count[2], 8, "one stage sample per thread");
    }

    #[test]
    fn scope_clears_thread_local_on_drop() {
        let counters = Arc::new(JobCounters::default());
        {
            let _scope = JobScope::enter(counters.clone());
            record_rows(5);
            assert!(with_current_job(|jc| jc.rows.load(Ordering::Relaxed)).is_some());
        }
        // Outside the scope: hot path is a no-op, not a panic.
        record_rows(99);
        assert!(with_current_job(|_| ()).is_none());
    }

    #[test]
    fn hot_path_is_noop_without_scope() {
        // Must not panic when no job is active (e.g. background threads).
        record_rows(1);
        record_event();
        assert!(with_current_job(|_| ()).is_none());
    }
}
