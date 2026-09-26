// SPDX-License-Identifier: GPL-2.0
#include <linux/errno.h>
#include <linux/gfp.h>
#include <linux/module.h>
#include <kunit/test.h>

#include "include/kineplex.h"

static void flat_geodesic_converges_without_acceleration(struct kunit *test)
{
	struct kineplex_christoffel symbols = { 0 };
	struct kineplex_geodesic_state state = {
		.tangent = { KINEPLEX_Q_ONE, 0, 0 },
	};
	int ret;

	ret = kineplex_geometry_integrate(&symbols, &state,
				KINEPLEX_Q_ONE / 10, 10);
	KUNIT_EXPECT_EQ(test, state.tangent[0], KINEPLEX_Q_ONE);
}

static void inverse_rejects_singular_metric(struct kunit *test)
{
	s64 singular[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM] = {
		{ KINEPLEX_Q_ONE, 0, 0 },
		{ 0, KINEPLEX_Q_ONE, 0 },
		{ 0, 0, 0 },
	};
	s64 inverse[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM];

	KUNIT_EXPECT_EQ(test, kineplex_metric_inverse(singular, inverse), -EDOM);
}

static void inverse_uses_fixed_point_and_checks_result(struct kunit *test)
{
	s64 metric[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM] = {
		{ 2 * KINEPLEX_Q_ONE, 0, 0 },
		{ 0, 4 * KINEPLEX_Q_ONE, 0 },
		{ 0, 0, 8 * KINEPLEX_Q_ONE },
	};
	s64 inverse[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM] = { 0 };

	KUNIT_ASSERT_EQ(test, kineplex_metric_inverse(metric, inverse), 0);
	KUNIT_EXPECT_EQ(test, inverse[0][0], KINEPLEX_Q_ONE / 2);
	KUNIT_EXPECT_EQ(test, inverse[1][1], KINEPLEX_Q_ONE / 4);
	KUNIT_EXPECT_EQ(test, inverse[2][2], KINEPLEX_Q_ONE / 8);
}

static void null_and_zero_denominator_are_rejected(struct kunit *test)
{
	s64 result;

	KUNIT_EXPECT_EQ(test, kineplex_q16_div(1, 0, &result), -EDOM);
	KUNIT_EXPECT_EQ(test, kineplex_q16_div(1, 1, NULL), -EINVAL);
}

static void invalid_job_is_rejected(struct kunit *test)
{
	struct kineplex_christoffel symbols = { 0 };
	struct kineplex_geodesic_state state = { 0 };

	KUNIT_EXPECT_EQ(test,
		kineplex_geometry_integrate(&symbols, &state, KINEPLEX_Q_ONE, 0),
		-EINVAL);
}

static void homotopy_slab_reuses_objects_without_leaking(struct kunit *test)
{
	struct kineplex_homotopy *homotopy;
	u64 allocations_before;
	u64 frees_before;
	u64 allocations_after;
	u64 frees_after;
	u32 in_use;
	unsigned int index;

	kineplex_homotopy_stats(&allocations_before, &frees_before, &in_use);
	KUNIT_EXPECT_EQ(test, in_use, (u32)0);
	/* The production module owns cache lifetime; KUnit must not destroy it. */
	homotopy = kineplex_homotopy_alloc(GFP_KERNEL);
	if (!homotopy) {
		KUNIT_SKIP(test, "kineplex_homotopy_cache is not loaded");
		return;
	}
	kineplex_homotopy_free(homotopy);
	for (index = 0; index < 9999; index++) {
		homotopy = kineplex_homotopy_alloc(GFP_KERNEL);
		KUNIT_ASSERT_NOT_NULL(test, homotopy);
		homotopy->waypoint_count = 1;
		kineplex_homotopy_free(homotopy);
	}
	kineplex_homotopy_stats(&allocations_after, &frees_after, &in_use);
	KUNIT_EXPECT_EQ(test, in_use, (u32)0);
	KUNIT_EXPECT_GE(test, allocations_after - allocations_before, (u64)10000);
	KUNIT_EXPECT_GE(test, frees_after - frees_before, (u64)10000);
}

static struct kunit_case kineplex_geometry_cases[] = {
	KUNIT_CASE(flat_geodesic_converges_without_acceleration),
	KUNIT_CASE(inverse_rejects_singular_metric),
	KUNIT_CASE(inverse_uses_fixed_point_and_checks_result),
	KUNIT_CASE(null_and_zero_denominator_are_rejected),
	KUNIT_CASE(homotopy_slab_reuses_objects_without_leaking),
	KUNIT_CASE(invalid_job_is_rejected),
	{}
};

static struct kunit_suite kineplex_geometry_suite = {
	.name = "kineplex-geometry",
	.test_cases = kineplex_geometry_cases,
};

kunit_test_suite(kineplex_geometry_suite);
MODULE_LICENSE("GPL");
