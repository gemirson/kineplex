/* SPDX-License-Identifier: GPL-2.0 */
#ifndef KINEPLEX_GEO_INTERNAL_H
#define KINEPLEX_GEO_INTERNAL_H

#include <linux/capability.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/mutex.h>
#include <linux/netdevice.h>
#include <linux/percpu.h>
#include <linux/slab.h>
#include <linux/types.h>

#include "uapi/linux/kineplex_geo.h"

#define KINEPLEX_GEO_TENSOR_BYTES PAGE_SIZE
#define KINEPLEX_GEO_TENSOR_CACHE_NAME "kineplex_geo_tensor"

struct kineplex_geo_context {
	struct device *nic_device;
	int numa_node;
	struct kmem_cache *tensor_cache;
	void *tensor_object;
	struct page *tensor_page;
	unsigned int tensor_order;
	struct mutex lock;
};

extern struct kineplex_geo_context kineplex_geo_ctx;

/* FT-093: every allocation is tied to the NIC's NUMA node. */
int kineplex_geo_numa_init(struct kineplex_geo_context *ctx,
			   struct device *nic_device);
void kineplex_geo_numa_destroy(struct kineplex_geo_context *ctx);
int kineplex_geo_numa_info(const struct kineplex_geo_context *ctx,
			   struct kineplex_geo_device_info *info);
int kineplex_geo_numa_mmap(struct kineplex_geo_context *ctx,
			   struct vm_area_struct *vma);

/* FT-094: per-CPU counters have no global write-side lock. */
void kineplex_geo_telemetry_record(bool dropped, u64 bytes, u64 latency_ns);
void kineplex_geo_telemetry_snapshot(
		struct kineplex_geo_telemetry_snapshot *snapshot);
void kineplex_geo_telemetry_reset(void);

/* FT-096: all userspace entry points use this exact policy. */
static inline int kineplex_geo_require_net_admin(void)
{
	return capable(CAP_NET_ADMIN) ? 0 : -EPERM;
}

#endif /* KINEPLEX_GEO_INTERNAL_H */
