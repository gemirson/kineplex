#!/bin/sh
# SPDX-License-Identifier: GPL-2.0
# Run after an administrator has loaded kineplex_geo.ko.
set -eu

DEVICE=${1:-/dev/kinegeo}
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
BINARY=${TMPDIR:-/tmp}/kineplex-capability-smoke.$$
trap 'rm -f "$BINARY"' EXIT

if [ "$(id -u)" -eq 0 ]; then
    echo "Refusing to run as root: the test must exercise the no-CAP_NET_ADMIN path" >&2
    exit 2
fi

if [ ! -e "$DEVICE" ]; then
    echo "Device $DEVICE is absent; load the module in a privileged setup step" >&2
    exit 2
fi

cc -std=c11 -Wall -Wextra -Werror \
    "$ROOT/tests/kernel/capability_smoke.c" -o "$BINARY"

if command -v getcap >/dev/null 2>&1 && [ -n "$(getcap "$BINARY")" ]; then
    echo "Refusing a test binary with file capabilities" >&2
    exit 2
fi

"$BINARY" "$DEVICE"
