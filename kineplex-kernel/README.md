# KinePlex kernel driver

This directory contains the Linux out-of-tree driver for the v0.3 kernel features. It is deliberately separate from the Rust userspace workspace: Linux kernel code cannot link against `std`, Tokio, Arrow, or Wasmtime.

## FT-066 — asynchronous geodesic resolution

The geometry worker consumes XDP-facing telemetry through `kineplex_geometry_update_telemetry()`. It recalculates a bounded Q16.16 metric and Christoffel symbols on a `WQ_HIGHPRI | WQ_UNBOUND | WQ_MEM_RECLAIM` workqueue. Every integration step calls `cond_resched()` and rejects jobs larger than `KINEPLEX_MAX_GEODESIC_STEPS`.

Build against the target kernel headers:

```text
make -C /lib/modules/$(uname -r)/build M=$PWD CONFIG_KINEPLEX=m CONFIG_KINEPLEX_KUNIT_TEST=m modules
```

KUnit can be enabled with `CONFIG_KUNIT=y` and `CONFIG_KINEPLEX_KUNIT_TEST=m` in the target kernel configuration. No kernel headers are installed in the development sandbox, so the Kbuild command must run on a kernel build host.

## FT-067 — homotopy slab allocation

Transient deformation objects are allocated from the custom `kmem_cache` named `kineplex_homotopy_cache` and are zeroed before publication. Allocation, free, and in-use counters are exposed to KUnit; the test performs 10,000 allocate/free cycles and requires zero objects in use before cache destruction. `kineplex_homotopy_exit()` refuses to destroy a cache with outstanding objects, preventing unload-time leaks.

## FT-068 — anti-panic mathematical sandbox

All divisions used by the driver pass through checked Q16.16 helpers. The 3x3 metric inverse calculates a fixed-point determinant and cofactors, rejects a zero or sub-resolution determinant with `-EDOM`, and reports arithmetic overflow instead of allowing undefined behavior. The KUnit suite injects a singular tensor and verifies that it is rejected without a fault; it also verifies diagonal inversion and null/zero-denominator guards.

## FT-069 — io_uring route command

The module registers `/dev/kineplex` with a `.uring_cmd` file-operation. Userspace opens this device and submits `IORING_OP_URING_CMD` SQEs with `sqe->cmd_op = IORING_OP_KINEPLEX_ROUTE`; the route payload is copied into the command PDU, deferred with `io_uring_cmd_complete_in_task()`, and completed with `io_uring_cmd_done()` directly into the ring CQ.

A loadable module cannot safely mutate the upstream kernel's private opcode dispatch table. The command selector therefore uses the supported `IORING_OP_URING_CMD` extension point while retaining the stable KinePlex selector `IORING_OP_KINEPLEX_ROUTE`. This avoids an invasive kernel fork and preserves the single SQ/CQ notification path.

## FT-070 — global topology debugfs

With debugfs mounted, the module creates `/sys/kernel/debug/kineplex/curvature_tensor` and `/sys/kernel/debug/kineplex/active_geodesics`. Both files use `seq_file`; each read copies the current RCU snapshot before formatting, so the read-side critical section never sleeps. `curvature_tensor` prints the revision, Q16.16 metric matrix, and a Christoffel slice; `active_geodesics` prints the current count. Module teardown removes the files, waits for readers and pending RCU callbacks, and frees the final snapshot.
