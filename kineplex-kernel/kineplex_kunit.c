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

static struct kunit_case kineplex_geometry_cases[] = {
	KUNIT_CASE(flat_geodesic_converges_without_acceleration),
	KUNIT_CASE(invalid_job_is_rejected),
	{}
};

static struct kunit_suite kineplex_geometry_suite = {
	.name = "kineplex-geometry",
	.test_cases = kineplex_geometry_cases,
};

kunit_test_suite(kineplex_geometry_suite);
MODULE_LICENSE("GPL");
