// SPDX-License-Identifier: GPL-2.0
/* FT-094: lock-free per-CPU telemetry aggregation. */

#include <linux/percpu.h>
#include <linux/smp.h>

#include "kineplex_geo_internal.h"

static struct kineplex_geo_cpu_telemetry __percpu *kineplex_geo_cpu_stats;

int kineplex_geo_telemetry_init(void)
{
	int cpu;

	kineplex_geo_cpu_stats = alloc_percpu(struct kineplex_geo_cpu_telemetry);
	if (!kineplex_geo_cpu_stats)
		return -ENOMEM;

	for_each_possible_cpu(cpu) {
		struct kineplex_geo_cpu_telemetry *stats;

		stats = per_cpu_ptr(kineplex_geo_cpu_stats, cpu);
		memset(stats, 0, sizeof(*stats));
	}
	return 0;
}

void kineplex_geo_telemetry_destroy(void)
{
	if (!kineplex_geo_cpu_stats)
		return;

	free_percpu(kineplex_geo_cpu_stats);
	kineplex_geo_cpu_stats = NULL;
}

void kineplex_geo_telemetry_record(bool dropped, u64 bytes, u64 latency_ns)
{
	struct kineplex_geo_cpu_telemetry *stats;

	if (unlikely(!kineplex_geo_cpu_stats))
		return;

	/* This is the RX/XDP write path: only the current CPU cache line is touched. */
	stats = this_cpu_ptr(kineplex_geo_cpu_stats);
	stats->rx_packets++;
	stats->rx_bytes += bytes;
	if (dropped)
		stats->rx_drops++;
	if (latency_ns) {
		stats->latency_ns += latency_ns;
		stats->latency_samples++;
	}
}

void kineplex_geo_telemetry_snapshot(
		struct kineplex_geo_telemetry_snapshot *snapshot)
{
	int cpu;

	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->abi_version = KINEPLEX_GEO_ABI_VERSION;
	snapshot->cpu_count = num_online_cpus();
	if (!kineplex_geo_cpu_stats)
		return;

	/* The background/control path is the only reader. RX workers never lock. */
	for_each_possible_cpu(cpu) {
		const struct kineplex_geo_cpu_telemetry *stats;

		stats = per_cpu_ptr(kineplex_geo_cpu_stats, cpu);
		snapshot->rx_packets += READ_ONCE(stats->rx_packets);
		snapshot->rx_drops += READ_ONCE(stats->rx_drops);
		snapshot->rx_bytes += READ_ONCE(stats->rx_bytes);
		snapshot->latency_ns += READ_ONCE(stats->latency_ns);
		snapshot->latency_samples += READ_ONCE(stats->latency_samples);
	}
}

void kineplex_geo_telemetry_reset(void)
{
	int cpu;

	if (!kineplex_geo_cpu_stats)
		return;

	for_each_possible_cpu(cpu) {
		struct kineplex_geo_cpu_telemetry *stats;

		stats = per_cpu_ptr(kineplex_geo_cpu_stats, cpu);
		memset(stats, 0, sizeof(*stats));
	}
}
