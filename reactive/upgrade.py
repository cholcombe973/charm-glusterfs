import json
import os
import random
from result import Err, Ok, Result
import time
from typing import Optional
import uuid

from lib.gluster.lib import run_command
from lib.gluster.peer import Peer
from lib.gluster.volume import volume_info
from charmhelpers.fetch import apt_install
from charmhelpers.core.host import service_start, service_stop
from charmhelpers.core.hookenv import config, log, status_set


def get_glusterfs_version() -> Result:
    """

    :return:
    """
    cmd = ["-s", "glusterfs-server"]
    output = run_command("dpkg", cmd, True, False)
    if output.is_ok():
        for line in output.value:
            if line.startswith("Version"):
                # return the version
                parts = line.split(" ")
                if len(parts) is 2:
                    return Ok(parts[1])
                else:
                    return Err(
                        "apt-cache Version string is invalid: {}".format(line))
    else:
        return Err(output)
    return Err("Unable to find glusterfs-server version")


def get_local_uuid() -> Result:
    """
    File looks like this:
    UUID=30602134-698f-4e53-8503-163e175aea85
    operating-version=30800

    :return: 
    """
    with open("/var/lib/glusterd/glusterd.info", "r") as f:
        lines = f.readlines()
        for line in lines:
            if "UUID" in line:
                parts = line.split("=")
                gluster_uuid = uuid.UUID(parts[1].strip())
                return Ok(gluster_uuid)
    return Err("Unable to find UUID")


def roll_cluster(new_version: str) -> Result:
    """
    Edge cases:
    1. Previous node dies on upgrade, can we retry
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous peer is upgraded yet.

    :param new_version: 
    :return: 
    """
    log("roll_cluster called with {}".format(new_version))
    volume_name = config["volume_name"]
    my_uuid = get_local_uuid()
    if my_uuid.is_err():
        return Err(my_uuid.value)

    # volume_name always has a default
    volume_bricks = volume_info(volume_name)
    if volume_bricks.is_err():
        return Err(volume_bricks.value)
    peer_list = volume_bricks.value.bricks.peers
    log("peer_list: {}".format(peer_list))

    # Sort by UUID
    peer_list.sort()
    # We find our position by UUID
    position = [i for i, x in enumerate(peer_list) if x == my_uuid.value]
    if len(position) == 0:
        return Err("Unable to determine upgrade position")
    log("upgrade position: {}".format(position))

    if position[0] == 0:
        # I'm first!  Roll
        # First set a key to inform others I'm about to roll
        lock_and_roll(my_uuid.value, new_version)
    else:
        # Check if the previous node has finished
        status_set(workload_state="waiting",
                   message="Waiting on {} to finish upgrading".format(
                       peer_list[position[0] - 1]))
        wait_on_previous_node(peer_list[position[0] - 1], new_version)
        lock_and_roll(my_uuid.value, new_version)
    return Ok(())


def upgrade_peer(new_version: str) -> Result:
    """

    :param new_version: 
    :return: 
    """
    from .main import update_status

    current_version = get_glusterfs_version()
    status_set(workload_state="maintenance", message="Upgrading peer")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    service_stop("glusterfs-server")
    apt_install(["glusterfs-server", "glusterfs-common", "glusterfs-client"])
    service_start("glusterfs-server")
    update_status()
    return Ok(())


def lock_and_roll(my_uuid: uuid.UUID, version: str) -> Result:
    """

    :param my_uuid: 
    :param version: 
    :return: 
    """
    start_timestamp = time.time()

    log("gluster_key_set {}_{}_start {}".format(my_uuid, version,
                                                start_timestamp))
    gluster_key_set("{}_{}_start".format(my_uuid, version), start_timestamp)
    log("Rolling")

    # This should be quick
    upgrade_peer(version)
    log("Done")

    stop_timestamp = time.time()
    # Set a key to inform others I am finished
    log("gluster_key_set {}_{}_done {}".format(my_uuid, version,
                                               stop_timestamp))
    gluster_key_set("{}_{}_done".format(my_uuid, version), stop_timestamp)

    return Ok(())


def gluster_key_get(key: str) -> Optional[float]:
    """

    :param key: 
    :return: 
    """
    upgrade_key = os.path.join(os.sep, "mnt", "glusterfs", ".upgrade", key)
    if not os.path.exists(upgrade_key):
        return None

    try:
        with open(upgrade_key, "r") as f:
            s = f.readlines()
            log("gluster_key_get read {} bytes".format(len(s)))
            try:
                decoded = json.loads(s)
                return float(decoded)
            except ValueError:
                log("Failed to decode json file in "
                    "gluster_key_get(): {}".format(s))
                return None
    except IOError as e:
        log("gluster_key_get failed to read file /mnt/glusterfs/.upgraded/.{} "
            "Error: {}".format(key, e.strerror))
        return None


def gluster_key_set(key: str, timestamp: float) -> Result:
    """

    :param key: 
    :param timestamp: 
    :return: 
    """
    p = os.path.join(os.sep, "mnt", "glusterfs", ".upgrade")
    if os.path.exists(p):
        os.makedirs(p)

    try:
        with open(os.path.join(p, key), "w") as file:
            encoded = json.dumps(timestamp)
            file.write(encoded)
            return Ok(())
    except IOError as e:
        return Err(e.strerror)


def gluster_key_exists(key: str) -> bool:
    location = "/mnt/glusterfs/.upgrade/{}".format(key)
    return os.path.exists(location)


def wait_on_previous_node(previous_node: Peer, version: str) -> Result:
    """

    :param previous_node: 
    :param version: 
    :return: 
    """
    log("Previous node is: {}".format(previous_node))
    previous_node_finished = gluster_key_exists(
        "{}_{}_done".format(previous_node.uuid, version))

    while not previous_node_finished:
        log("{} is not finished. Waiting".format(previous_node.uuid))
        # Has this node been trying to upgrade for longer than
        # 10 minutes
        # If so then move on and consider that node dead.

        # NOTE: This assumes the clusters clocks are somewhat accurate
        # If the hosts clock is really far off it may cause it to skip
        # the previous node even though it shouldn't.
        current_timestamp = time.time()

        previous_node_start_time = gluster_key_get("{}_{}_start".format(
            previous_node.uuid, version))
        if previous_node_start_time is not None:
            if float(current_timestamp - 600) > previous_node_start_time:
                # Previous node is probably dead.  Lets move on
                if previous_node_start_time is not None:
                    log("Waited 10 mins on node {}. current time: {} > "
                        "previous node start time: {} Moving on".format(
                        previous_node.uuid, (current_timestamp - 600),
                        previous_node_start_time))
                    return Ok(())
            else:
                # I have to wait.  Sleep a random amount of time and then
                # check if I can lock,upgrade and roll.
                wait_time = random.randrange(5, 30)
                log("waiting for {} seconds".format(wait_time))
                time.sleep(wait_time)
                previous_node_finished = gluster_key_exists(
                    "{}_{}_done".format(previous_node.uuid, version))
        else:
            # TODO: There is no previous start time.  What should we do?
            return Ok(())
