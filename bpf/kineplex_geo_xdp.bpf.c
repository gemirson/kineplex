// SPDX-License-Identifier: GPL-2.0
/* FT-094: per-CPU XDP telemetry. */

#include <linux/bpf.h>
#include <linux/types.h>

#include "bpf_helpers.h"

struct kineplex_geo_xdp_cpu_stats {
	__u64 rx_packets;
	__u64 rx_drops;
	__u64 rx_bytes;
	__u64 latency_ns;
	__u64 latency_samples;
};

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, 1);
	__type(key, __u32);
	__type(value, struct kineplex_geo_xdp_cpu_stats);
} geo_stats SEC(".maps");

SEC("xdp")
int kineplex_geo_xdp(struct xdp_md *ctx)
{
	__u32 key = 0;
	struct kineplex_geo_xdp_cpu_stats *stats;
	void *data = (void *)(long)ctx->data;
	void *data_end = (void *)(long)ctx->data_end;
	__u64 packet_bytes;
	__u64 start_ns = bpf_ktime_get_ns();

	stats = bpf_map_lookup_elem(&geo_stats, &key);
	if (!stats)
		return XDP_PASS;

	if (data_end < data) {
		stats->rx_drops++;
		return XDP_DROP;
	}

	packet_bytes = (__u64)(data_end - data);
	/* Per-CPU map updates avoid cache-line bouncing between RX queues. */
	stats->rx_packets++;
	stats->rx_bytes += packet_bytes;
	stats->latency_ns += bpf_ktime_get_ns() - start_ns;
	stats->latency_samples++;
	return XDP_PASS;
}
