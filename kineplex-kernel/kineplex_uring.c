// SPDX-License-Identifier: GPL-2.0
#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/io_uring.h>
#include <linux/kernel.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/spinlock.h>
#include <linux/string.h>

#include "include/kineplex.h"

struct kineplex_uring_pdu {
	struct kineplex_route_request request;
};

static DEFINE_SPINLOCK(kineplex_route_lock);
static struct kineplex_route_request kineplex_last_route;
static u64 kineplex_route_revision;

static int kineplex_apply_route(const struct kineplex_route_request *request)
{
	if (unlikely(!request || request->resistance_q16 < 0 ||
			request->resistance_q16 > 1000 * KINEPLEX_Q_ONE))
		return -EINVAL;
	spin_lock(&kineplex_route_lock);
	kineplex_last_route = *request;
	kineplex_route_revision++;
	spin_unlock(&kineplex_route_lock);
	return 0;
}

static void kineplex_route_complete(struct io_uring_cmd *ioucmd)
{
	struct kineplex_uring_pdu *pdu = (struct kineplex_uring_pdu *)ioucmd->pdu;
	int ret;

	ret = kineplex_apply_route(&pdu->request);
	/* This completion is written directly to the submitting ring's CQ. */
	io_uring_cmd_done(ioucmd, ret, 0);
}

static int kineplex_route_uring_cmd(struct io_uring_cmd *ioucmd,
					unsigned int issue_flags)
{
	const struct kineplex_route_request *request = ioucmd->cmd;
	struct kineplex_uring_pdu *pdu = (struct kineplex_uring_pdu *)ioucmd->pdu;

	if (unlikely(!request || ioucmd->cmd_op != IORING_OP_KINEPLEX_ROUTE))
		return -EINVAL;
	if (issue_flags & IO_URING_F_IOPOLL)
		return -EOPNOTSUPP;

	/* Read the SQE command into the in-command PDU before deferring. */
	pdu->request.edge_id = READ_ONCE(request->edge_id);
	pdu->request.target_node = READ_ONCE(request->target_node);
	pdu->request.resistance_q16 = READ_ONCE(request->resistance_q16);
	io_uring_cmd_complete_in_task(ioucmd, kineplex_route_complete);
	return -EIOCBQUEUED;
}

static const struct file_operations kineplex_uring_fops = {
	.owner = THIS_MODULE,
	.uring_cmd = kineplex_route_uring_cmd,
};

static struct miscdevice kineplex_misc_device = {
	.minor = MISC_DYNAMIC_MINOR,
	.name = "kineplex",
	.fops = &kineplex_uring_fops,
	.mode = 0600,
};

int kineplex_uring_init(void)
{
	spin_lock_init(&kineplex_route_lock);
	kineplex_route_revision = 0;
	return misc_register(&kineplex_misc_device);
}
EXPORT_SYMBOL_GPL(kineplex_uring_init);

void kineplex_uring_exit(void)
{
	misc_deregister(&kineplex_misc_device);
}
EXPORT_SYMBOL_GPL(kineplex_uring_exit);
