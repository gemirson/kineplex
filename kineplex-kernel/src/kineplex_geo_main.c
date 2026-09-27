// SPDX-License-Identifier: GPL-2.0
/* KinePlex geometric engine kernel module. */

#include <linux/init.h>
#include <linux/module.h>
#include <linux/netdevice.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#if IS_ENABLED(CONFIG_IO_URING)
#include <linux/io_uring.h>
#endif

#include "kineplex_geo_internal.h"

struct kineplex_geo_context kineplex_geo_ctx = {
	.numa_node = NUMA_NO_NODE,
};

static char *nic_ifname = KINEPLEX_GEO_DEFAULT_IFACE;
module_param(nic_ifname, charp, 0400);
MODULE_PARM_DESC(nic_ifname, "NIC whose NUMA node owns metric tensor memory");

static dev_t kineplex_geo_dev;
static struct cdev kineplex_geo_cdev;
static struct class *kineplex_geo_class;
static struct device *kineplex_geo_device;

static int kineplex_geo_open(struct inode *inode, struct file *file)
{
	int ret = kineplex_geo_require_net_admin();

	if (ret)
		return ret;
	file->private_data = &kineplex_geo_ctx;
	return 0;
}

static int kineplex_geo_release(struct inode *inode, struct file *file)
{
	file->private_data = NULL;
	return 0;
}

static long kineplex_geo_ioctl(struct file *file,
			       unsigned int cmd,
			       unsigned long arg)
{
	struct kineplex_geo_device_info info = { 0 };
	int ret;

	ret = kineplex_geo_require_net_admin();
	if (ret)
		return ret;

	switch (cmd) {
	case KINEPLEX_GEO_IOC_GET_INFO:
		ret = kineplex_geo_numa_info(&kineplex_geo_ctx, &info);
		if (ret)
			return ret;
		if (copy_to_user((void __user *)arg, &info, sizeof(info)))
			return -EFAULT;
		return 0;
	case KINEPLEX_GEO_IOC_GET_TELEMETRY: {
		struct kineplex_geo_telemetry_snapshot snapshot;

		kineplex_geo_telemetry_snapshot(&snapshot);
		if (copy_to_user((void __user *)arg, &snapshot, sizeof(snapshot)))
			return -EFAULT;
		return 0;
	}
	case KINEPLEX_GEO_IOC_RESET_TELEMETRY:
		kineplex_geo_telemetry_reset();
		return 0;
	default:
		return -ENOTTY;
	}
}

static int kineplex_geo_mmap(struct file *file, struct vm_area_struct *vma)
{
	int ret = kineplex_geo_require_net_admin();

	if (ret)
		return ret;
	return kineplex_geo_numa_mmap(&kineplex_geo_ctx, vma);
}

#if IS_ENABLED(CONFIG_IO_URING)
static int kineplex_geo_uring_cmd(struct io_uring_cmd *cmd,
				  unsigned int issue_flags)
{
	/* FT-096: reject every uring command from callers without CAP_NET_ADMIN. */
	(void)issue_flags;
	if (kineplex_geo_require_net_admin())
		return -EPERM;
	if (!cmd)
		return -EINVAL;

	/* The ABI is intentionally deny-by-default until a command is specified. */
	return -EOPNOTSUPP;
}
#endif

static const struct file_operations kineplex_geo_fops = {
	.owner = THIS_MODULE,
	.open = kineplex_geo_open,
	.release = kineplex_geo_release,
	.unlocked_ioctl = kineplex_geo_ioctl,
	.mmap = kineplex_geo_mmap,
#if IS_ENABLED(CONFIG_IO_URING)
	.uring_cmd = kineplex_geo_uring_cmd,
#endif
	.llseek = no_llseek,
};

static int __init kineplex_geo_init(void)
{
	struct net_device *nic;
	int ret;

	nic = dev_get_by_name(&init_net, nic_ifname);
	if (!nic) {
		pr_err("kineplex_geo: NIC %s not found; refusing non-local allocation\n",
		       nic_ifname);
		return -ENODEV;
	}

	ret = kineplex_geo_numa_init(&kineplex_geo_ctx, &nic->dev);
	if (ret) {
		pr_err("kineplex_geo: NIC %s has no usable NUMA node: %d\n",
		       nic_ifname, ret);
		dev_put(nic);
		return ret;
	}

	ret = kineplex_geo_telemetry_init();
	if (ret)
		goto err_numa;

	ret = alloc_chrdev_region(&kineplex_geo_dev, 0, 1, KINEPLEX_GEO_DEVICE_NAME);
	if (ret)
		goto err_numa;

	cdev_init(&kineplex_geo_cdev, &kineplex_geo_fops);
	kineplex_geo_cdev.owner = THIS_MODULE;
	ret = cdev_add(&kineplex_geo_cdev, kineplex_geo_dev, 1);
	if (ret)
		goto err_chrdev;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
	kineplex_geo_class = class_create(KINEPLEX_GEO_DEVICE_NAME);
#else
	kineplex_geo_class = class_create(THIS_MODULE, KINEPLEX_GEO_DEVICE_NAME);
#endif
	if (IS_ERR(kineplex_geo_class)) {
		ret = PTR_ERR(kineplex_geo_class);
		kineplex_geo_class = NULL;
		goto err_cdev;
	}

	kineplex_geo_device = device_create(kineplex_geo_class, NULL,
					    kineplex_geo_dev, NULL,
					    KINEPLEX_GEO_DEVICE_NAME);
	if (IS_ERR(kineplex_geo_device)) {
		ret = PTR_ERR(kineplex_geo_device);
		kineplex_geo_device = NULL;
		goto err_class;
	}

	pr_info("kineplex_geo: loaded on %s, NUMA node %d\n",
		nic_ifname, kineplex_geo_ctx.numa_node);
	return 0;

err_class:
	class_destroy(kineplex_geo_class);
	kineplex_geo_class = NULL;
err_cdev:
	cdev_del(&kineplex_geo_cdev);
err_chrdev:
	unregister_chrdev_region(kineplex_geo_dev, 1);
err_numa:
	kineplex_geo_telemetry_destroy();
	dev_put(nic);
	kineplex_geo_numa_destroy(&kineplex_geo_ctx);
	return ret;
}

static void __exit kineplex_geo_exit(void)
{
	struct device *nic = kineplex_geo_ctx.nic_device;

	device_destroy(kineplex_geo_class, kineplex_geo_dev);
	class_destroy(kineplex_geo_class);
	cdev_del(&kineplex_geo_cdev);
	unregister_chrdev_region(kineplex_geo_dev, 1);
	kineplex_geo_telemetry_destroy();
	kineplex_geo_numa_destroy(&kineplex_geo_ctx);
	if (nic)
		dev_put(nic);
	pr_info("kineplex_geo: unloaded\n");
}

module_init(kineplex_geo_init);
module_exit(kineplex_geo_exit);

MODULE_AUTHOR("KinePlex Team");
MODULE_DESCRIPTION("NUMA-local geometric tensor engine");
MODULE_LICENSE("GPL");
MODULE_VERSION("0.3.0");
