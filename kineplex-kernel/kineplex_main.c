// SPDX-License-Identifier: GPL-2.0
#include <linux/init.h>
#include <linux/module.h>

#include "include/kineplex.h"

static int __init kineplex_init(void)
{
	int ret;

	ret = kineplex_geometry_init();
	if (ret)
		return ret;

	pr_info("kineplex: geometry workqueue initialized\n");
	return 0;
}

static void __exit kineplex_exit(void)
{
	kineplex_geometry_exit();
	pr_info("kineplex: unloaded\n");
}

module_init(kineplex_init);
module_exit(kineplex_exit);

MODULE_DESCRIPTION("KinePlex asynchronous geometric kernel driver");
MODULE_AUTHOR("KinePlex Engineering Team");
MODULE_LICENSE("GPL");
MODULE_VERSION("0.3");
