/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _KINEPLEX_H
#define _KINEPLEX_H

#include <linux/gfp_types.h>
#include <linux/types.h>

#define KINEPLEX_Q_SHIFT 16
#define KINEPLEX_Q_ONE ((s64)1 << KINEPLEX_Q_SHIFT)
#define KINEPLEX_GEOMETRY_DIM 3
#define KINEPLEX_MAX_GEODESIC_STEPS 4096
#define KINEPLEX_HOMOTOPY_MAX_WAYPOINTS 64
#define IORING_OP_KINEPLEX_ROUTE 0x4b50

/* All geometry values use signed Q16.16 fixed-point representation. */
struct kineplex_telemetry {
	u32 latency_us;
	u32 queued_bytes;
	u16 cpu_percent;
};

struct kineplex_metric {
	s64 components[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM];
	u64 revision;
};

struct kineplex_christoffel {
	s64 values[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM]
		[KINEPLEX_GEOMETRY_DIM];
	u64 revision;
};

struct kineplex_geodesic_state {
	s64 position[KINEPLEX_GEOMETRY_DIM];
	s64 tangent[KINEPLEX_GEOMETRY_DIM];
};

struct kineplex_geometry_snapshot {
	struct kineplex_metric metric;
	struct kineplex_christoffel symbols;
	u32 active_geodesics;
};

struct kineplex_homotopy {
	u16 waypoint_count;
	s64 waypoints[KINEPLEX_HOMOTOPY_MAX_WAYPOINTS][KINEPLEX_GEOMETRY_DIM];
};

struct kineplex_route_request {
	u64 edge_id;
	u64 target_node;
	s64 resistance_q16;
};

int kineplex_uring_init(void);
void kineplex_uring_exit(void);

int kineplex_homotopy_init(void);
void kineplex_homotopy_exit(void);
struct kineplex_homotopy *kineplex_homotopy_alloc(gfp_t flags);
void kineplex_homotopy_free(struct kineplex_homotopy *homotopy);
void kineplex_homotopy_stats(u64 *allocations, u64 *frees, u32 *in_use);

int kineplex_geometry_init(void);
void kineplex_geometry_exit(void);
void kineplex_geometry_update_telemetry(const struct kineplex_telemetry *sample);
int kineplex_geometry_integrate(const struct kineplex_christoffel *symbols,
				struct kineplex_geodesic_state *state,
				s64 step_size, u32 steps);
void kineplex_geometry_get_snapshot(struct kineplex_geometry_snapshot *snapshot);
void kineplex_debugfs_publish(const struct kineplex_geometry_snapshot *snapshot);
int kineplex_debugfs_init(void);
void kineplex_debugfs_exit(void);

/* Shared by the geometry worker and the anti-panic matrix tests. */
int kineplex_q16_div(s64 numerator, s64 denominator, s64 *result);
int kineplex_q16_mul(s64 left, s64 right, s64 *result);
int kineplex_metric_inverse(const s64 metric[KINEPLEX_GEOMETRY_DIM]
					[KINEPLEX_GEOMETRY_DIM],
					s64 inverse[KINEPLEX_GEOMETRY_DIM]
					[KINEPLEX_GEOMETRY_DIM]);

#endif /* _KINEPLEX_H */
