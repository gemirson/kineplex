/* SPDX-License-Identifier: GPL-2.0 OR BSD-2-Clause */
#ifndef KINEPLEX_BPF_HELPERS_H
#define KINEPLEX_BPF_HELPERS_H

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name

static void *(*bpf_map_lookup_elem)(void *map, const void *key) =
	(void *)1;
static __u64 (*bpf_ktime_get_ns)(void) = (void *)5;

char LICENSE[] SEC("license") = "GPL";

#endif /* KINEPLEX_BPF_HELPERS_H */
