// SPDX-License-Identifier: GPL-2.0
#include <linux/atomic.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/math64.h>
#include <linux/overflow.h>
#include <linux/sched.h>
#include <linux/spinlock.h>
#include <linux/string.h>
#include <linux/workqueue.h>

#include "include/kineplex.h"

struct kineplex_geometry_worker {
	struct workqueue_struct *queue;
	struct work_struct work;
	spinlock_t lock;
	struct kineplex_telemetry telemetry;
	struct kineplex_geometry_snapshot snapshot;
};

static struct kineplex_geometry_worker kineplex_worker;

static s64 kineplex_ratio(u64 numerator, u64 denominator)
{
	u64 scaled;

	if (unlikely(!denominator))
		return KINEPLEX_Q_ONE;
	if (check_mul_overflow(numerator, (u64)KINEPLEX_Q_ONE, &scaled))
		return KINEPLEX_Q_ONE;
	return min_t(u64, div64_u64(scaled, denominator),
			(u64)KINEPLEX_Q_ONE * 100);
}

static void kineplex_metric_from_telemetry(const struct kineplex_telemetry *telemetry,
						struct kineplex_metric *metric)
{
	s64 diagonal[KINEPLEX_GEOMETRY_DIM];

	diagonal[0] = KINEPLEX_Q_ONE + kineplex_ratio(telemetry->latency_us,
								100000);
	diagonal[1] = KINEPLEX_Q_ONE + kineplex_ratio(telemetry->queued_bytes,
								1024 * 1024);
	diagonal[2] = KINEPLEX_Q_ONE + kineplex_ratio(telemetry->cpu_percent,
								100);
	memset(metric, 0, sizeof(*metric));
	metric->components[0][0] = diagonal[0];
	metric->components[1][1] = diagonal[1];
	metric->components[2][2] = diagonal[2];
}

static void kineplex_recompute_symbols(const struct kineplex_metric *metric,
						struct kineplex_christoffel *symbols)
{
	unsigned int axis;
	s64 numerator;
	s64 denominator;

	memset(symbols, 0, sizeof(*symbols));
	for (axis = 0; axis < KINEPLEX_GEOMETRY_DIM; axis++) {
		/* Telemetry supplies a bounded local metric gradient proxy. */
		numerator = metric->components[axis][axis] - KINEPLEX_Q_ONE;
		denominator = 2 * metric->components[axis][axis];
		if (kineplex_q16_div(numerator, denominator,
					&symbols->values[axis][axis][axis]))
			symbols->values[axis][axis][axis] = 0;
	}
	symbols->revision = metric->revision;
}

static int kineplex_geodesic_step(const struct kineplex_christoffel *symbols,
					struct kineplex_geodesic_state *state,
					s64 step_size)
{
	s64 acceleration[KINEPLEX_GEOMETRY_DIM] = { 0 };
	s64 tangent_product;
	s64 contribution;
	s64 delta;
	unsigned int upper;
	unsigned int first;
	unsigned int second;
	int ret;

	for (upper = 0; upper < KINEPLEX_GEOMETRY_DIM; upper++) {
		for (first = 0; first < KINEPLEX_GEOMETRY_DIM; first++) {
			for (second = 0; second < KINEPLEX_GEOMETRY_DIM; second++) {
				ret = kineplex_q16_mul(state->tangent[first],
						state->tangent[second], &tangent_product);
				if (ret)
					return ret;
				ret = kineplex_q16_mul(symbols->values[upper][first][second],
						tangent_product, &contribution);
				if (ret)
					return ret;
				if (check_add_overflow(acceleration[upper], -contribution,
						&acceleration[upper]))
					return -ERANGE;
			}
		}
	}
	for (upper = 0; upper < KINEPLEX_GEOMETRY_DIM; upper++) {
		ret = kineplex_q16_mul(state->tangent[upper], step_size, &delta);
		if (ret || check_add_overflow(state->position[upper], delta,
					&state->position[upper]))
			return ret ? ret : -ERANGE;
		ret = kineplex_q16_mul(acceleration[upper], step_size, &delta);
		if (ret || check_add_overflow(state->tangent[upper], delta,
					&state->tangent[upper]))
			return ret ? ret : -ERANGE;
	}
	return 0;
}

int kineplex_geometry_integrate(const struct kineplex_christoffel *symbols,
				struct kineplex_geodesic_state *state,
				s64 step_size, u32 steps)
{
	u32 step;

	if (unlikely(!symbols || !state || !steps ||
			steps > KINEPLEX_MAX_GEODESIC_STEPS || !step_size))
		return -EINVAL;
	for (step = 0; step < steps; step++) {
		int ret = kineplex_geodesic_step(symbols, state, step_size);

		if (ret)
			return ret;
		/* A long tensor solve must never monopolize a CPU or trigger a watchdog. */
		cond_resched();
	}
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_geometry_integrate);

static void kineplex_geometry_work(struct work_struct *work)
{
	struct kineplex_geometry_worker *worker =
		container_of(work, struct kineplex_geometry_worker, work);
	struct kineplex_telemetry telemetry;
	struct kineplex_geometry_snapshot next;
	struct kineplex_geodesic_state state = { 0 };
	u64 revision;

	spin_lock_bh(&worker->lock);
	telemetry = worker->telemetry;
	next = worker->snapshot;
	revision = next.metric.revision + 1;
	spin_unlock_bh(&worker->lock);

	kineplex_metric_from_telemetry(&telemetry, &next.metric);
	next.metric.revision = revision;
	kineplex_recompute_symbols(&next.metric, &next.symbols);
	state.tangent[0] = KINEPLEX_Q_ONE;
	if (!kineplex_geometry_integrate(&next.symbols, &state,
				KINEPLEX_Q_ONE / 256, 256))
		next.active_geodesics = 1;

	spin_lock_bh(&worker->lock);
	worker->snapshot = next;
	spin_unlock_bh(&worker->lock);
}

void kineplex_geometry_update_telemetry(const struct kineplex_telemetry *sample)
{
	if (unlikely(!sample || !kineplex_worker.queue))
		return;
	spin_lock_bh(&kineplex_worker.lock);
	kineplex_worker.telemetry = *sample;
	spin_unlock_bh(&kineplex_worker.lock);
	schedule_work(&kineplex_worker.work);
}
EXPORT_SYMBOL_GPL(kineplex_geometry_update_telemetry);

void kineplex_geometry_get_snapshot(struct kineplex_geometry_snapshot *snapshot)
{
	if (unlikely(!snapshot))
		return;
	spin_lock_bh(&kineplex_worker.lock);
	*snapshot = kineplex_worker.snapshot;
	spin_unlock_bh(&kineplex_worker.lock);
}
EXPORT_SYMBOL_GPL(kineplex_geometry_get_snapshot);

int kineplex_geometry_init(void)
{
	memset(&kineplex_worker, 0, sizeof(kineplex_worker));
	spin_lock_init(&kineplex_worker.lock);
	INIT_WORK(&kineplex_worker.work, kineplex_geometry_work);
	kineplex_worker.queue = alloc_workqueue("kineplex_geometry",
			WQ_HIGHPRI | WQ_UNBOUND | WQ_MEM_RECLAIM, 1);
	if (!kineplex_worker.queue)
		return -ENOMEM;
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_geometry_init);

void kineplex_geometry_exit(void)
{
	if (!kineplex_worker.queue)
		return;
	cancel_work_sync(&kineplex_worker.work);
	destroy_workqueue(kineplex_worker.queue);
	kineplex_worker.queue = NULL;
}
EXPORT_SYMBOL_GPL(kineplex_geometry_exit);
