/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */
#ifndef _UAPI_LINUX_KINEPLEX_GEO_H
#define _UAPI_LINUX_KINEPLEX_GEO_H

#include <linux/ioctl.h>
#include <linux/types.h>

#define KINEPLEX_GEO_ABI_VERSION 1U
#define KINEPLEX_GEO_DEVICE_NAME "kinegeo"
#define KINEPLEX_GEO_DEFAULT_IFACE "eth0"

#define KINEPLEX_GEO_IOC_MAGIC 'K'

struct kineplex_geo_device_info {
	__u32 abi_version;
	__u32 numa_node;
	__u32 tensor_order;
	__u32 reserved;
	__u64 tensor_bytes;
};

struct kineplex_geo_cpu_telemetry {
	__u64 rx_packets;
	__u64 rx_drops;
	__u64 rx_bytes;
	__u64 latency_ns;
	__u64 latency_samples;
};

struct kineplex_geo_telemetry_snapshot {
	__u32 abi_version;
	__u32 cpu_count;
	__u64 rx_packets;
	__u64 rx_drops;
	__u64 rx_bytes;
	__u64 latency_ns;
	__u64 latency_samples;
};

#define KINEPLEX_GEO_IOC_GET_INFO \
	_IOR(KINEPLEX_GEO_IOC_MAGIC, 0x01, struct kineplex_geo_device_info)
#define KINEPLEX_GEO_IOC_GET_TELEMETRY \
	_IOR(KINEPLEX_GEO_IOC_MAGIC, 0x02, struct kineplex_geo_telemetry_snapshot)
#define KINEPLEX_GEO_IOC_RESET_TELEMETRY \
	_IO(KINEPLEX_GEO_IOC_MAGIC, 0x03)

/* FT-096: the only accepted io_uring command in this ABI. */
#define KINEPLEX_GEO_URING_CMD_NOP 0x01U

#endif /* _UAPI_LINUX_KINEPLEX_GEO_H */
