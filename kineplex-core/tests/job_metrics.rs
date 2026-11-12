//! Integration tests for the public job-metrics API (Fase 1).
//! Verifies the monoid laws and statistical guarantees through the exported types.

use kineplex_core::job_metrics::{
    Counter, DdSketch, Merge, Welford, DEFAULT_ALPHA, OVERHEAD_BUDGET,
};

#[test]
fn public_api_exported() {
    assert!((OVERHEAD_BUDGET - 0.01).abs() < f64::EPSILON);
    assert!((DEFAULT_ALPHA - 0.01).abs() < f64::EPSILON);
}

#[test]
fn counter_monoid_via_public_api() {
    let a = {
        let c = Counter::default();
        c.inc(100);
        c
    };
    let b = {
        let c = Counter::default();
        c.inc(200);
        c
    };
    let c = {
        let c = Counter::default();
        c.inc(300);
        c
    };

    let mut ab_c = Counter::default();
    ab_c.merge(&a);
    ab_c.merge(&b);
    ab_c.merge(&c);

    let mut a_bc = a.clone();
    let mut bc = b.clone();
    bc.merge(&c);
    a_bc.merge(&bc);

    assert_eq!(ab_c.get(), 600);
    assert_eq!(ab_c.get(), a_bc.get());

    let mut id = Counter::identity();
    id.merge(&a);
    assert_eq!(id.get(), 100);
}

#[test]
fn welford_merge_stability_across_shards() {
    // Simulate 4 shards computing partial stats, then merge (Fase 3 pattern).
    let shards: Vec<Vec<f64>> = vec![
        vec![10.0, 12.0, 14.0],
        vec![11.0, 13.0],
        vec![9.0, 15.0, 16.0],
        vec![8.0],
    ];

    let mut merged = Welford::new();
    for shard in &shards {
        let mut w = Welford::new();
        for &x in shard {
            w.observe(x);
        }
        merged.merge(&w);
    }

    let mut single = Welford::new();
    for shard in &shards {
        for &x in shard {
            single.observe(x);
        }
    }

    assert_eq!(merged.count(), 9);
    assert!((merged.mean() - single.mean()).abs() < 1e-12);
    assert!((merged.variance() - single.variance()).abs() < 1e-12);
}

#[test]
fn ddsketch_p95_within_one_percent() {
    let mut s = DdSketch::new(DEFAULT_ALPHA);
    // Latency-like dataset: 1..=10_000 ms.
    for v in 1..=10_000u64 {
        s.observe(v as f64);
    }
    let exact_p95 = 9500.0;
    let approx = s.quantile(0.95);
    let rel_err = ((approx - exact_p95) / exact_p95).abs();
    assert!(
        rel_err <= DEFAULT_ALPHA,
        "p95 rel error {rel_err} > α={DEFAULT_ALPHA} (approx={approx})"
    );
}

#[test]
fn ddsketch_merge_is_order_independent() {
    let data: Vec<f64> = (1..=1000).map(|v| v as f64 * 1.7).collect();

    let forward = {
        let mut s1 = DdSketch::new(DEFAULT_ALPHA);
        let mut s2 = DdSketch::new(DEFAULT_ALPHA);
        for (i, &v) in data.iter().enumerate() {
            if i % 2 == 0 {
                s1.observe(v);
            } else {
                s2.observe(v);
            }
        }
        s1.merge(&s2);
        s1
    };
    let backward = {
        let mut s2 = DdSketch::new(DEFAULT_ALPHA);
        let mut s1 = DdSketch::new(DEFAULT_ALPHA);
        for (i, &v) in data.iter().enumerate() {
            if i % 2 == 0 {
                s1.observe(v);
            } else {
                s2.observe(v);
            }
        }
        s2.merge(&s1);
        s2
    };

    assert_eq!(forward, backward);
    for &q in &[0.5f64, 0.95] {
        let a = forward.quantile(q);
        let b = backward.quantile(q);
        assert!((a - b).abs() / a < 1e-12, "quantile({q}) differs: {a} vs {b}");
    }
}
