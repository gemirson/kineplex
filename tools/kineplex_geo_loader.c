// SPDX-License-Identifier: GPL-2.0
/*
 * FT-095: deterministic libbpf XDP lifecycle manager.
 *
 * The link is deliberately not pinned. Its lifetime is owned by this process;
 * a normal exit, signal, or process crash closes the bpf_link FD and the kernel
 * detaches the program automatically. The loader also holds /dev/kinegeo open,
 * so an unsafe rmmod is rejected by the kernel module while the link is live.
 */

#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <net/if.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "linux/kineplex_geo.h"

static volatile sig_atomic_t stop_requested;

static void handle_signal(int signal_number)
{
	(void)signal_number;
	stop_requested = 1;
}

static int install_signal_handlers(void)
{
	struct sigaction action = {
		.sa_handler = handle_signal,
	};

	sigemptyset(&action.sa_mask);
	if (sigaction(SIGINT, &action, NULL) < 0)
		return -errno;
	if (sigaction(SIGTERM, &action, NULL) < 0)
		return -errno;
	if (sigaction(SIGHUP, &action, NULL) < 0)
		return -errno;
	return 0;
}

static int print_snapshot(int map_fd, int possible_cpus)
{
	struct kineplex_geo_cpu_telemetry *values;
	struct kineplex_geo_telemetry_snapshot total = {
		.abi_version = KINEPLEX_GEO_ABI_VERSION,
		.cpu_count = (uint32_t)possible_cpus,
	};
	uint32_t key = 0;
	int cpu;

	values = calloc((size_t)possible_cpus, sizeof(*values));
	if (!values)
		return -ENOMEM;

	if (bpf_map_lookup_elem(map_fd, &key, values) < 0) {
		int ret = -errno;
		free(values);
		return ret;
	}

	for (cpu = 0; cpu < possible_cpus; cpu++) {
		total.rx_packets += values[cpu].rx_packets;
		total.rx_drops += values[cpu].rx_drops;
		total.rx_bytes += values[cpu].rx_bytes;
		total.latency_ns += values[cpu].latency_ns;
		total.latency_samples += values[cpu].latency_samples;
	}

	printf("rx_packets=%llu rx_drops=%llu rx_bytes=%llu "
	       "xdp_latency_ns=%llu samples=%llu cpus=%u\n",
	       (unsigned long long)total.rx_packets,
	       (unsigned long long)total.rx_drops,
	       (unsigned long long)total.rx_bytes,
	       (unsigned long long)total.latency_ns,
	       (unsigned long long)total.latency_samples,
	       total.cpu_count);
	free(values);
	return 0;
}

static int device_is_alive(int device_fd)
{
	struct kineplex_geo_device_info info = { 0 };

	if (ioctl(device_fd, KINEPLEX_GEO_IOC_GET_INFO, &info) < 0)
		return -errno;
	if (info.abi_version != KINEPLEX_GEO_ABI_VERSION)
		return -EPROTONOSUPPORT;
	return 0;
}

int main(int argc, char **argv)
{
	const char *ifname = argc > 1 ? argv[1] : KINEPLEX_GEO_DEFAULT_IFACE;
	const char *object_path = argc > 2 ? argv[2] : "bpf/kineplex_geo_xdp.bpf.o";
	struct bpf_object *object = NULL;
	struct bpf_program *program;
	struct bpf_map *stats_map;
	struct bpf_link *link = NULL;
	struct kineplex_geo_device_info info = { 0 };
	int device_fd = -1;
	int ifindex;
	int map_fd;
	int possible_cpus;
	int ret;

	ret = install_signal_handlers();
	if (ret) {
		fprintf(stderr, "signal setup failed: %s\n", strerror(-ret));
		return EXIT_FAILURE;
	}

	ifindex = if_nametoindex(ifname);
	if (!ifindex) {
		fprintf(stderr, "interface %s not found: %s\n", ifname, strerror(errno));
		return EXIT_FAILURE;
	}

	/* Keep the module's device reference alive for the whole BPF link lease. */
	device_fd = open("/dev/" KINEPLEX_GEO_DEVICE_NAME, O_RDWR | O_CLOEXEC);
	if (device_fd < 0) {
		fprintf(stderr, "open /dev/%s failed: %s\n",
			KINEPLEX_GEO_DEVICE_NAME, strerror(errno));
		return EXIT_FAILURE;
	}
	if (ioctl(device_fd, KINEPLEX_GEO_IOC_GET_INFO, &info) < 0) {
		fprintf(stderr, "kineplex_geo ABI query failed: %s\n", strerror(errno));
		ret = -errno;
		goto out;
	}

	libbpf_set_strict_mode(LIBBPF_STRICT_ALL);
	object = bpf_object__open_file(object_path, NULL);
	if (libbpf_get_error(object)) {
		ret = (int)libbpf_get_error(object);
		object = NULL;
		fprintf(stderr, "open BPF object %s failed: %s\n",
			object_path, strerror(-ret));
		goto out;
	}

	ret = bpf_object__load(object);
	if (ret < 0) {
		fprintf(stderr, "load BPF object failed: %s\n", strerror(-ret));
		goto out;
	}

	program = bpf_object__find_program_by_name(object, "kineplex_geo_xdp");
	if (!program) {
		fprintf(stderr, "BPF program kineplex_geo_xdp not found\n");
		ret = -ENOENT;
		goto out;
	}

	link = bpf_program__attach_xdp(program, ifindex);
	if (libbpf_get_error(link)) {
		ret = (int)libbpf_get_error(link);
		link = NULL;
		fprintf(stderr, "XDP attach failed: %s\n", strerror(-ret));
		goto out;
	}

	stats_map = bpf_object__find_map_by_name(object, "geo_stats");
	if (!stats_map) {
		ret = -ENOENT;
		fprintf(stderr, "BPF map geo_stats not found\n");
		goto out;
	}
	map_fd = bpf_map__fd(stats_map);
	possible_cpus = libbpf_num_possible_cpus();
	if (possible_cpus <= 0) {
		ret = -EINVAL;
		goto out;
	}

	printf("attached kineplex_geo_xdp to %s (NUMA node %u); "
	       "unpin-free link is active\n", ifname, info.numa_node);
	while (!stop_requested) {
		ret = device_is_alive(device_fd);
		if (ret) {
			fprintf(stderr, "device lease lost: %s; detaching XDP\n",
				strerror(-ret));
			break;
		}
		ret = print_snapshot(map_fd, possible_cpus);
		if (ret) {
			fprintf(stderr, "telemetry read failed: %s\n", strerror(-ret));
			break;
		}
		sleep(1);
	}
	ret = 0;

out:
	/* Destroying the link before closing the object is the required detach order. */
	bpf_link__destroy(link);
	bpf_object__close(object);
	if (device_fd >= 0)
		close(device_fd);
	return ret ? EXIT_FAILURE : EXIT_SUCCESS;
}
