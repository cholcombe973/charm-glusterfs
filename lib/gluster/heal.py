import os
from gluster.volume import Brick


def get_self_heal_count(brick: Brick) -> int:
    """
    Find the self heal count for a given brick.

    :param brick: the brick to probe for the self heal count.
    :return int: the number of files that need healing
    """
    brick_path = "{}/.glusterfs/indices/xattrop".format(brick.path)

    # The gfids which need healing are those files which do not start
    # with 'xattrop'.
    count = 0
    for f in os.listdir(brick_path):
        if not f.startswith('xattrop'):
            count += 1

    return count
