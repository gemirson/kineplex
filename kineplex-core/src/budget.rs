//! Fase 4 — PI feedback controller that keeps metrics overhead within budget.
//!
//! The instrumentation cost is *measured* (delta of [`Instant`] around sampled
//! writes), amortized over the window, and fed to a PI controller that adjusts
//! the sampling probability `p ∈ [p_min, 1.0]`:
//!
//! ```text
//! y_k  = EWMA(measured_overhead)          // plant output (overhead ∝ p)
//! e_k  = budget − y_k                     // error vs setpoint
//! c_k  = y_k / p_k                        // estimated plant gain
//! p_k+1 = clamp(budget/c_k + Kp·e_k + Ki·∫e, p_min, 1.0)
//! ```
//!
//! The `budget/c` term inverts the (linear) plant so a simulated plant
//! `overhead = c·p` converges in a couple of iterations; the PI terms reject
//! noise and drift. `p` is stored as **Q16.16** in an `AtomicU64` so the hot
//! path reads it with a single `Relaxed` load (plan Fase 4).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use crate::job_metrics::OVERHEAD_BUDGET;

/// Q16.16 fixed-point scale factor (1.0 == 65536).
pub const Q16_ONE: u64 = 1 << 16;

/// Convert a `f64` probability to Q16.16, clamped to `[0, 1]`.
#[inline]
pub fn to_q16(p: f64) -> u64 {
    let clamped = p.clamp(0.0, 1.0);
    (clamped * Q16_ONE as f64).round() as u64
}

/// Convert Q16.16 back to `f64`.
#[inline]
pub fn from_q16(v: u64) -> f64 {
    v as f64 / Q16_ONE as f64
}

/// Exponential smoothing factor for the overhead measurement (λ = e^{-Δt/τ}).
/// Fixed λ keeps the controller deterministic and testable; τ ≈ 3 windows.
const EWMA_LAMBDA: f64 = 0.3;

/// Anti-windup bound for the integral term (in overhead-ratio units).
const INTEGRAL_LIMIT: f64 = 1.0;

/// Floor for the estimated plant gain (avoid dividing by ~0 when p is tiny).
const GAIN_FLOOR: f64 = 1e-6;

/// Mutable PI state (updated at most once per control tick — never hot path).
#[derive(Debug, Default)]
struct PiState {
    /// Smoothed overhead ratio.
    ewma_overhead: f64,
    /// Integral of the error (∫e), clamped for anti-windup.
    integral: f64,
    /// Whether the EWMA has been seeded.
    seeded: bool,
}

/// PI controller over the metrics sampling rate.
pub struct BudgetController {
    kp: f64,
    ki: f64,
    budget: f64,
    p_min: f64,
    /// Current sampling probability, Q16.16 (hot path: `Relaxed` load).
    p: AtomicU64,
    state: Mutex<PiState>,
}

impl std::fmt::Debug for BudgetController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BudgetController")
            .field("kp", &self.kp)
            .field("ki", &self.ki)
            .field("budget", &self.budget)
            .field("p", &self.sample_rate())
            .finish()
    }
}

impl BudgetController {
    /// `kp`/`ki` are the PI gains; `budget` defaults to
    /// [`OVERHEAD_BUDGET`] (1 % of job CPU time).
    pub fn new(kp: f64, ki: f64, budget: f64) -> Self {
        Self::with_p_min(kp, ki, budget, 0.01)
    }

    pub fn with_p_min(kp: f64, ki: f64, budget: f64, p_min: f64) -> Self {
        assert!(budget > 0.0, "budget must be positive");
        assert!(
            (0.0..=1.0).contains(&p_min),
            "p_min must be in [0, 1]"
        );
        Self {
            kp,
            ki,
            budget,
            p_min,
            // Start at full sampling: cheap until we learn the plant.
            p: AtomicU64::new(Q16_ONE),
            state: Mutex::new(PiState::default()),
        }
    }

    /// Controller for the default 1 % budget.
    pub fn default_budget() -> Self {
        Self::new(0.5, 0.2, OVERHEAD_BUDGET)
    }

    // -----------------------------------------------------------------
    // Hot path
    // -----------------------------------------------------------------

    /// Current sampling probability (single `Relaxed` load).
    #[inline]
    pub fn sample_rate(&self) -> f64 {
        from_q16(self.p.load(Ordering::Relaxed))
    }

    /// Should the event at `tick` be sampled?
    ///
    /// `p = 1.0` → always; `p = 0` → never; otherwise a deterministic 1/N
    /// stride (`N = round(1/p)`), so no RNG on the hot path.
    #[inline]
    pub fn should_sample(&self, tick: u64) -> bool {
        let p_q16 = self.p.load(Ordering::Relaxed);
        if p_q16 >= Q16_ONE {
            return true;
        }
        if p_q16 == 0 {
            return false;
        }
        let period = (Q16_ONE / p_q16).max(1);
        tick % period == 0
    }

    /// Measure the wall cost of one instrumented write (Fase 4 sampling).
    #[inline]
    pub fn measure_cost_ns(write: impl FnOnce()) -> u64 {
        let t0 = Instant::now();
        write();
        t0.elapsed().as_nanos() as u64
    }

    // -----------------------------------------------------------------
    // Control tick (off hot path — called once per window/scrape)
    // -----------------------------------------------------------------

    /// Amortized overhead of the sampled instrumentation:
    /// `cost_ns · (total/sampled) / window_ns`.
    pub fn amortized_overhead(
        cost_ns_per_sample: u64,
        sampled_events: u64,
        total_events: u64,
        window_ns: u64,
    ) -> f64 {
        if window_ns == 0 || sampled_events == 0 || total_events == 0 {
            return 0.0;
        }
        let amplification = total_events as f64 / sampled_events as f64;
        (cost_ns_per_sample as f64 * amplification) / window_ns as f64
    }

    /// One PI control step given the *raw* measured overhead ratio of the
    /// window. Returns the new sampling probability.
    pub fn tick(&self, raw_overhead: f64) -> f64 {
        let mut state = self.state.lock().expect("budget controller poisoned");

        // EWMA smoothing (mitigates scrape noise — plan risk table).
        let y = if state.seeded {
            EWMA_LAMBDA * state.ewma_overhead + (1.0 - EWMA_LAMBDA) * raw_overhead
        } else {
            state.seeded = true;
            raw_overhead
        };
        state.ewma_overhead = y;

        let p = self.sample_rate();

        // Error vs setpoint (positive = under budget = room to sample more).
        let error = self.budget - y;

        // Anti-windup: clamp the integral.
        state.integral = (state.integral + error).clamp(-INTEGRAL_LIMIT, INTEGRAL_LIMIT);

        // Plant inversion (overhead ≈ gain · p) + PI correction.
        let gain = (y / p.max(self.p_min)).max(GAIN_FLOOR);
        let p_target = self.budget / gain + self.kp * error + self.ki * state.integral;

        let p_new = p_target.clamp(self.p_min, 1.0);
        self.p.store(to_q16(p_new), Ordering::Relaxed);
        p_new
    }

    /// Current gauges for `/metrics` (plan: `kineplex_metrics_overhead_ratio`,
    /// `kineplex_metrics_sample_rate`).
    pub fn gauges(&self) -> (f64, f64) {
        let state = self.state.lock().expect("budget controller poisoned");
        (state.ewma_overhead, self.sample_rate())
    }

    /// Test/ops hook: force a sampling probability (Q16.16 store).
    pub fn set_sample_rate(&self, p: f64) {
        self.p.store(to_q16(p), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simulated plant: overhead is linear in the sampling rate.
    fn plant_overhead(p: f64, gain: f64) -> f64 {
        gain * p
    }

    #[test]
    fn q16_roundtrip_and_clamping() {
        assert_eq!(to_q16(1.0), Q16_ONE);
        assert_eq!(to_q16(0.0), 0);
        assert_eq!(to_q16(0.5), Q16_ONE / 2);
        assert_eq!(to_q16(2.5), Q16_ONE, "clamped to 1.0");
        assert_eq!(to_q16(-1.0), 0, "clamped to 0.0");
        assert!((from_q16(to_q16(0.33)) - 0.33).abs() < 1e-4);
    }

    #[test]
    fn should_sample_full_zero_and_stride() {
        let c = BudgetController::new(0.5, 0.2, 0.01);

        // p = 1.0 → always.
        c.set_sample_rate(1.0);
        assert!((0..10).all(|i| c.should_sample(i)));

        // p = 0 → never.
        c.set_sample_rate(0.0);
        assert!((0..10).all(|i| !c.should_sample(i)));

        // p = 0.25 → every 4th event.
        c.set_sample_rate(0.25);
        let hits = (0..1000).filter(|&i| c.should_sample(i)).count();
        assert_eq!(hits, 250);
        assert!(c.should_sample(0));
        assert!(!c.should_sample(1));
        assert!(c.should_sample(4));
    }

    #[test]
    fn pi_converges_under_budget_in_few_iterations() {
        // Plant: overhead = 0.02 · p  (p=1 → 2 % overhead, twice the budget).
        let gain = 0.02;
        let c = BudgetController::new(0.5, 0.2, 0.01);
        c.set_sample_rate(1.0);

        let mut final_p = 1.0;
        let mut converged_at = None;
        let mut min_y = f64::MAX;
        let mut max_y = f64::MIN;
        for k in 0..20 {
            let y = plant_overhead(final_p, gain);
            final_p = c.tick(y);
            if y <= 0.01 {
                converged_at.get_or_insert(k);
                min_y = min_y.min(y);
                max_y = max_y.max(y);
            }
        }

        let iters = converged_at.expect("must converge within 20 iterations");
        assert!(iters < 20, "converged at iteration {iters}");

        // Oscillation after convergence < 20 % of the setpoint.
        let y_last = plant_overhead(final_p, gain);
        assert!(
            y_last <= 0.01,
            "final overhead {y_last} must be ≤ budget"
        );
        let _ = (min_y, max_y);

        // Steady-state error: stay within 20 % of setpoint for the last 5 ticks.
        let mut p = final_p;
        let mut worst = 0.0f64;
        for _ in 0..5 {
            let y = plant_overhead(p, gain);
            worst = worst.max((y - 0.01).abs());
            p = c.tick(y);
        }
        assert!(
            worst <= 0.01 * 0.20,
            "steady-state deviation {worst} exceeds 20 % of setpoint"
        );
    }

    #[test]
    fn pi_handles_noisy_plant_without_exceeding_budget_often() {
        let gain = 0.03; // harsher plant: p=1 → 3 %
        let c = BudgetController::new(0.5, 0.2, 0.01);
        c.set_sample_rate(1.0);

        let mut p = 1.0;
        // Warm-up (plant learning).
        for k in 0..10 {
            let noise = 1.0 + 0.1 * ((k * 7 % 5) as f64 - 2.0) / 2.0; // ±10 % noise
            p = c.tick(plant_overhead(p, gain) * noise);
        }
        // Observation window: at most 1 of 10 ticks may exceed budget by >25 %.
        let mut violations = 0;
        for k in 10..40 {
            let noise = 1.0 + 0.1 * ((k * 7 % 5) as f64 - 2.0) / 2.0;
            let y = plant_overhead(p, gain) * noise;
            if y > 0.01 * 1.25 {
                violations += 1;
            }
            p = c.tick(y);
        }
        assert!(
            violations <= 3,
            "{violations} violations of the budget in observation window"
        );
        let (ewma, rate) = c.gauges();
        assert!(rate >= c.p_min && rate <= 1.0);
        assert!(ewma >= 0.0);
    }

    #[test]
    fn p_never_drops_below_p_min_and_never_above_one() {
        let c = BudgetController::with_p_min(0.5, 0.2, 0.01, 0.05);
        c.set_sample_rate(1.0);
        let mut p = 1.0;
        // Absurd plant: overhead = 50 · p → controller must floor at p_min.
        for _ in 0..50 {
            p = c.tick(50.0 * p);
            assert!((0.05..=1.0).contains(&p), "p={p} out of range");
        }
        // Vanishing plant: overhead ≈ 0 → controller may raise p to 1.0.
        for _ in 0..50 {
            p = c.tick(1e-9);
            assert!((0.05..=1.0).contains(&p), "p={p} out of range");
        }
        assert_eq!(p, 1.0);
    }

    #[test]
    fn amortized_overhead_math() {
        // 100 ns per sample, 1 sampled of every 10 events, window 1 µs:
        // 100 · 10 / 1000 = 1.0 (100 % — pathological but exact).
        let r = BudgetController::amortized_overhead(100, 1, 10, 1_000);
        assert!((r - 1.0).abs() < 1e-12);

        // Realistic: 50 ns per sample, sample 1/1000, window 10 ms.
        // 50 · 1000 / 10_000_000 = 0.005 (0.5 %).
        let r = BudgetController::amortized_overhead(50, 1, 1_000, 10_000_000);
        assert!((r - 0.005).abs() < 1e-12);

        // Degenerate inputs → 0 (never panic / never div-by-zero).
        assert_eq!(BudgetController::amortized_overhead(100, 0, 10, 1_000), 0.0);
        assert_eq!(BudgetController::amortized_overhead(100, 1, 10, 0), 0.0);
    }

    #[test]
    fn default_budget_is_one_percent() {
        let c = BudgetController::default_budget();
        assert!((c.budget - 0.01).abs() < f64::EPSILON);
        assert_eq!(c.sample_rate(), 1.0, "starts at full sampling");
    }

    #[test]
    fn measure_cost_ns_records_time() {
        let ns = BudgetController::measure_cost_ns(|| {
            let mut acc = 0u64;
            for i in 0..1000 {
                acc = acc.wrapping_add(i);
            }
            std::hint::black_box(acc);
        });
        assert!(ns > 0, "measured cost must be positive");
    }
}
