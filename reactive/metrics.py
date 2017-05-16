from charmhelpers.core.hookenv import add_metric
import os.path


def collect_metrics():
    """
    Gather metrics about gluster mount and log them to juju metrics
    """
    p = os.path.join(os.sep, "mnt", "glusterfs")
    mount_stats = os.statvfs(p)
    # block size * total blocks
    total_space = mount_stats.f_blocks * mount_stats.f_bsize
    free_space = mount_stats.f_bfree * mount_stats.f_bsize
    # capsize only operates on i64 values
    used_space = total_space - free_space
    gb_used = used_space / 1024 / 1024 / 1024

    # log!(format!("Collecting metric gb-used {}", gb_used), Info)
    add_metric("gb-used", "{}".format(gb_used))
