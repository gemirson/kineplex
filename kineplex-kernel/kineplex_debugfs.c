// SPDX-License-Identifier: GPL-2.0
#include <linux/debugfs.h>
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/rcupdate.h>
#include <linux/seq_file.h>
#include <linux/slab.h>
#include <linux/string.h>

#include "include/kineplex.h"

struct kineplex_debug_snapshot {
	struct rcu_head rcu;
	struct kineplex_geometry_snapshot geometry;
};

static struct dentry *kineplex_debug_root;
static struct kineplex_debug_snapshot __rcu *kineplex_debug_state;

static void kineplex_debug_snapshot_free(struct rcu_head *rcu)
{
	struct kineplex_debug_snapshot *snapshot =
		container_of(rcu, struct kineplex_debug_snapshot, rcu);

	kfree(snapshot);
}

void kineplex_debugfs_publish(const struct kineplex_geometry_snapshot *geometry)
{
	struct kineplex_debug_snapshot *next;
	struct kineplex_debug_snapshot *old;

	if (unlikely(!geometry))
		return;
	next = kmalloc(sizeof(*next), GFP_KERNEL);
	if (!next)
		return;
	next->geometry = *geometry;
	old = rcu_dereference_protected(kineplex_debug_state, 1);
	rcu_assign_pointer(kineplex_debug_state, next);
	if (old)
		call_rcu(&old->rcu, kineplex_debug_snapshot_free);
}
EXPORT_SYMBOL_GPL(kineplex_debugfs_publish);

static bool kineplex_debug_copy_snapshot(struct kineplex_geometry_snapshot *geometry)
{
	struct kineplex_debug_snapshot *snapshot;
	bool available = false;

	rcu_read_lock();
	snapshot = rcu_dereference(kineplex_debug_state);
	if (snapshot) {
		*geometry = snapshot->geometry;
		available = true;
	}
	rcu_read_unlock();
	return available;
}

static int kineplex_curvature_tensor_show(struct seq_file *file, void *unused)
{
	struct kineplex_geometry_snapshot snapshot;
	unsigned int row;
	unsigned int column;

	if (!kineplex_debug_copy_snapshot(&snapshot)) {
		seq_puts(file, "unavailable\n");
		return 0;
	}
	seq_printf(file, "revision: %llu\n",
		   (unsigned long long)snapshot.metric.revision);
	seq_puts(file, "metric_g_ij_q16_16:\n");
	for (row = 0; row < KINEPLEX_GEOMETRY_DIM; row++) {
		for (column = 0; column < KINEPLEX_GEOMETRY_DIM; column++)
			seq_printf(file, "%lld%c", (long long)
				   snapshot.metric.components[row][column],
				   column == KINEPLEX_GEOMETRY_DIM - 1 ? '\n' : ' ');
	}
	seq_puts(file, "christoffel_upper_0_q16_16:\n");
	for (row = 0; row < KINEPLEX_GEOMETRY_DIM; row++) {
		for (column = 0; column < KINEPLEX_GEOMETRY_DIM; column++)
			seq_printf(file, "%lld%c", (long long)
				   snapshot.symbols.values[0][row][column],
				   column == KINEPLEX_GEOMETRY_DIM - 1 ? '\n' : ' ');
	}
	return 0;
}

static int kineplex_active_geodesics_show(struct seq_file *file, void *unused)
{
	struct kineplex_geometry_snapshot snapshot;

	if (!kineplex_debug_copy_snapshot(&snapshot)) {
		seq_puts(file, "0\n");
		return 0;
	}
	seq_printf(file, "%u\n", snapshot.active_geodesics);
	return 0;
}

static int kineplex_curvature_tensor_open(struct inode *inode, struct file *file)
{
	return single_open(file, kineplex_curvature_tensor_show, inode->i_private);
}

static int kineplex_active_geodesics_open(struct inode *inode, struct file *file)
{
	return single_open(file, kineplex_active_geodesics_show, inode->i_private);
}

static const struct file_operations kineplex_curvature_tensor_fops = {
	.owner = THIS_MODULE,
	.open = kineplex_curvature_tensor_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

static const struct file_operations kineplex_active_geodesics_fops = {
	.owner = THIS_MODULE,
	.open = kineplex_active_geodesics_open,
	.read = seq_read,
	.llseek = seq_lseek,
	.release = single_release,
};

int kineplex_debugfs_init(void)
{
	struct kineplex_debug_snapshot *initial;

	kineplex_debug_root = debugfs_create_dir("kineplex", NULL);
	if (IS_ERR_OR_NULL(kineplex_debug_root)) {
		kineplex_debug_root = NULL;
		return -ENODEV;
	}
	initial = kzalloc(sizeof(*initial), GFP_KERNEL);
	if (!initial) {
		debugfs_remove_recursive(kineplex_debug_root);
		kineplex_debug_root = NULL;
		return -ENOMEM;
	}
	RCU_INIT_POINTER(kineplex_debug_state, initial);
	if (!debugfs_create_file("curvature_tensor", 0444, kineplex_debug_root,
				 NULL, &kineplex_curvature_tensor_fops) ||
	    !debugfs_create_file("active_geodesics", 0444, kineplex_debug_root,
				 NULL, &kineplex_active_geodesics_fops)) {
		kineplex_debugfs_exit();
		return -ENOMEM;
	}
	return 0;
}
EXPORT_SYMBOL_GPL(kineplex_debugfs_init);

void kineplex_debugfs_exit(void)
{
	struct kineplex_debug_snapshot *snapshot;

	debugfs_remove_recursive(kineplex_debug_root);
	kineplex_debug_root = NULL;
	snapshot = rcu_dereference_protected(kineplex_debug_state, 1);
	RCU_INIT_POINTER(kineplex_debug_state, NULL);
	synchronize_rcu();
	rcu_barrier();
	kfree(snapshot);
}
EXPORT_SYMBOL_GPL(kineplex_debugfs_exit);
