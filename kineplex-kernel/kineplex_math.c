// SPDX-License-Identifier: GPL-2.0
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/math64.h>
#include <linux/overflow.h>

#include "include/kineplex.h"

int kineplex_q16_mul(s64 left, s64 right, s64 *result)
{
	s64 product;

	if (unlikely(!result))
		return -EINVAL;
	if (check_mul_overflow(left, right, &product))
		return -ERANGE;
	*result = div_s64(product, KINEPLEX_Q_ONE);
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_q16_mul);

int kineplex_q16_div(s64 numerator, s64 denominator, s64 *result)
{
	s64 scaled;

	if (unlikely(!result))
		return -EINVAL;
	if (unlikely(!denominator))
		return -EDOM;
	if (check_mul_overflow(numerator, KINEPLEX_Q_ONE, &scaled))
		return -ERANGE;
	*result = div_s64(scaled, denominator);
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_q16_div);

static int kineplex_q16_sub(s64 left, s64 right, s64 *result)
{
	if (unlikely(!result))
		return -EINVAL;
	if (check_sub_overflow(left, right, result))
		return -ERANGE;
	return 0;
}

static int kineplex_q16_product3(s64 first, s64 second, s64 third,
				 s64 *result)
{
	s64 product;
	int ret;

	ret = kineplex_q16_mul(first, second, &product);
	if (ret)
		return ret;
	return kineplex_q16_mul(product, third, result);
}

static int kineplex_metric_determinant(
	const s64 metric[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM],
	s64 *determinant)
{
	s64 positive;
	s64 negative;
	s64 term;
	int ret;

	ret = kineplex_q16_product3(metric[0][0], metric[1][1], metric[2][2],
				    &positive);
	if (ret)
		return ret;
	ret = kineplex_q16_product3(metric[0][1], metric[1][2], metric[2][0],
				    &term);
	if (ret)
		return ret;
	if (check_add_overflow(positive, term, &positive))
		return -ERANGE;
	ret = kineplex_q16_product3(metric[0][2], metric[1][0], metric[2][1],
				    &term);
	if (ret)
		return ret;
	if (check_add_overflow(positive, term, &positive))
		return -ERANGE;

	ret = kineplex_q16_product3(metric[0][2], metric[1][1], metric[2][0],
				    &negative);
	if (ret)
		return ret;
	ret = kineplex_q16_product3(metric[0][0], metric[1][2], metric[2][1],
				    &term);
	if (ret)
		return ret;
	if (check_add_overflow(negative, term, &negative))
		return -ERANGE;
	ret = kineplex_q16_product3(metric[0][1], metric[1][0], metric[2][2],
				    &term);
	if (ret)
		return ret;
	if (check_add_overflow(negative, term, &negative))
		return -ERANGE;

	return kineplex_q16_sub(positive, negative, determinant);
}

int kineplex_metric_inverse(
	const s64 metric[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM],
	s64 inverse[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM])
{
	s64 cofactors[KINEPLEX_GEOMETRY_DIM][KINEPLEX_GEOMETRY_DIM];
	s64 determinant;
	s64 left;
	s64 right;
	unsigned int row;
	unsigned int column;
	int ret;

	if (unlikely(!metric || !inverse))
		return -EINVAL;
	memset(inverse, 0, sizeof(cofactors));
	ret = kineplex_metric_determinant(metric, &determinant);
	if (ret)
		return ret;
	/* Reject singular and sub-resolution tensors before any division. */
	if (unlikely(!determinant || determinant == S64_MIN ||
			(det > -2 && determinant < 2)))
		return -EDOM;

	/* Cofactor matrix; every product is checked fixed-point arithmetic. */
	ret = kineplex_q16_mul(metric[1][1], metric[2][2], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[1][2], metric[2][1], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[0][0]))
		return ret ? ret : -ERANGE;

	ret = kineplex_q16_mul(metric[1][2], metric[2][0], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[1][0], metric[2][2], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[0][1]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[1][0], metric[2][1], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[1][1], metric[2][0], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[0][2]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][2], metric[2][1], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][1], metric[2][2], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[1][0]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][0], metric[2][2], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][2], metric[2][0], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[1][1]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][1], metric[2][0], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][0], metric[2][1], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[1][2]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][1], metric[1][2], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][2], metric[1][1], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[2][0]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][2], metric[1][0], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][0], metric[1][2], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[2][1]))
		return ret ? ret : -ERANGE;
	ret = kineplex_q16_mul(metric[0][0], metric[1][1], &left);
	if (ret)
		return ret;
	ret = kineplex_q16_mul(metric[0][1], metric[1][0], &right);
	if (ret || kineplex_q16_sub(left, right, &cofactors[2][2]))
		return ret ? ret : -ERANGE;

	for (row = 0; row < KINEPLEX_GEOMETRY_DIM; row++) {
		for (column = 0; column < KINEPLEX_GEOMETRY_DIM; column++) {
			/* Adjugate transpose and checked determinant division. */
			ret = kineplex_q16_div(cofactors[column][row], determinant,
					       &inverse[row][column]);
			if (ret)
				return ret;
		}
	}
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_metric_inverse);
