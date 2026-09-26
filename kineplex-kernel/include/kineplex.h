/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _KINEPLEX_H
#define _KINEPLEX_H

#include <linux/types.h>

#define KINEPLEX_Q_SHIFT 16
#define KINEPLEX_Q_ONE ((s64)1 << KINEPLEX_Q_SHIFT)
#define KINEPLEX_GEOMETRY_DIM 3
#define KINEPLEX_MAX_GEODESIC_STEPS 4096

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

int kineplex_geometry_init(void);
void kineplex_geometry_exit(void);
void kineplex_geometry_update_telemetry(const struct kineplex_telemetry *sample);
int kineplex_geometry_integrate(const struct kineplex_christoffel *symbols,
				struct kineplex_geodesic_state *state,
				s64 step_size, u32 steps);
void kineplex_geometry_get_snapshot(struct kineplex_geometry_snapshot *snapshot);

/* Shared by the geometry worker and the anti-panic matrix tests. */
int kineplex_q16_div(s64 numerator, s64 denominator, s64 *result);
int kineplex_q16_mul(s64 left, s64 right, s64 *result);
int kineplex_metric_inverse(const s64 metric[KINEPLEX_GEOMETRY_DIM]
					[KINEPLEX_GEOMETRY_DIM],
					s64 inverse[KINEPLEX_GEOMETRY_DIM]
					[KINEPLEX_GEOMETRY_DIM]);

#endif /* _KINEPLEX_H */
