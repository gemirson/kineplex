# KinePlex kernel driver

This directory contains the Linux out-of-tree driver for the v0.3 kernel features. It is deliberately separate from the Rust userspace workspace: Linux kernel code cannot link against `std`, Tokio, Arrow, or Wasmtime.

## FT-066 — asynchronous geodesic resolution

The geometry worker consumes XDP-facing telemetry through `kineplex_geometry_update_telemetry()`. It recalculates a bounded Q16.16 metric and Christoffel symbols on a `WQ_HIGHPRI | WQ_UNBOUND | WQ_MEM_RECLAIM` workqueue. Every integration step calls `cond_resched()` and rejects jobs larger than `KINEPLEX_MAX_GEODESIC_STEPS`.

Build against the target kernel headers:

```text
make -C /lib/modules/$(uname -r)/build M=$PWD CONFIG_KINEPLEX=m CONFIG_KINEPLEX_KUNIT_TEST=m modules
```

KUnit can be enabled with `CONFIG_KUNIT=y` and `CONFIG_KINEPLEX_KUNIT_TEST=m` in the target kernel configuration. No kernel headers are installed in the development sandbox, so the Kbuild command must run on a kernel build host.
