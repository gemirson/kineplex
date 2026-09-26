// SPDX-License-Identifier: GPL-2.0
#include <linux/errno.h>
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
		KUNIT_ASSERT_EQ(test, ret, 0);
		KUNIT_EXPECT_EQ(test, state.position[0], KINEPLEX_Q_ONE);
		KUNIT_EXPECT_EQ(test, state.position[1], (s64)0);
		KUNIT_EXPECT_EQ(test, state.tangent[0], KINEPLEX_Q_ONE);
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
