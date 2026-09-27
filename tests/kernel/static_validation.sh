#!/bin/sh
# SPDX-License-Identifier: GPL-2.0
# Validates the FT-093..FT-096 source contract without loading a kernel module.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

require() {
    file=$1
    pattern=$2
    if ! grep -Eq "$pattern" "$ROOT/$file"; then
        echo "missing contract: $pattern in $file" >&2
        exit 1
    fi
}

# FT-093: NUMA locality and no remote fallback.
require kineplex-kernel/src/kineplex_geo_numa.c 'dev_to_node'
require kineplex-kernel/src/kineplex_geo_numa.c 'kmem_cache_alloc_node'
require kineplex-kernel/src/kineplex_geo_numa.c 'alloc_pages_node'
require kineplex-kernel/src/kineplex_geo_numa.c '__GFP_THISNODE'
require kineplex-kernel/src/kineplex_geo_numa.c 'remap_pfn_range'

# FT-094: per-CPU writes and background aggregation.
require kineplex-kernel/src/kineplex_geo_telemetry.c 'alloc_percpu'
require kineplex-kernel/src/kineplex_geo_telemetry.c 'this_cpu_ptr'
require bpf/kineplex_geo_xdp.bpf.c 'BPF_MAP_TYPE_PERCPU_ARRAY'

# FT-095: link ownership and deterministic detach.
require tools/kineplex_geo_loader.c 'bpf_program__attach_xdp'
require tools/kineplex_geo_loader.c 'bpf_link__destroy'
require tools/kineplex_geo_loader.c 'bpf_object__close'

# FT-096: all process-controlled entry points require CAP_NET_ADMIN.
require kineplex-kernel/src/kineplex_geo_main.c 'kineplex_geo_require_net_admin'
require kineplex-kernel/src/kineplex_geo_internal.h 'capable\(CAP_NET_ADMIN\)'
require kineplex-kernel/src/kineplex_geo_main.c '\.uring_cmd'
require kineplex-kernel/src/kineplex_geo_main.c 'return -EPERM'

sh -n "$ROOT/tests/kernel/capability_smoke.sh"
cc -std=c11 -Wall -Wextra -Werror \
    "$ROOT/tests/kernel/capability_smoke.c" \
    -o "$TMP/capability_smoke"

ARCH_INC=""
if [ -d "/usr/include/$(uname -m)-linux-gnu" ]; then
    ARCH_INC="-I/usr/include/$(uname -m)-linux-gnu"
fi

if command -v clang >/dev/null 2>&1; then
    clang -O2 -g -target bpf -D__TARGET_ARCH_x86 \
        $ARCH_INC \
        -I"$ROOT/bpf" \
        -c "$ROOT/bpf/kineplex_geo_xdp.bpf.c" \
        -o "$TMP/kineplex_geo_xdp.bpf.o"
fi

echo "FT-093..FT-096 static validation: PASS"
