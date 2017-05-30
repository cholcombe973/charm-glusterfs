from enum import Enum
from ipaddress import ip_address
from result import Err, Ok, Result
from typing import Dict, List
import uuid
import xml.etree.ElementTree as etree

from .peer import get_peer
from .peer import Peer
from .lib import BitrotOption, get_local_ip, GlusterError, \
    GlusterOption, resolve_to_ip, run_command


# A Gluster Brick consists of a Peer and a path to the mount point
class Brick(object):
    def __init__(self, brick_uuid: uuid.UUID, peer: Peer, path,
                 is_arbiter: bool):
        """
        A Gluster brick
        :param brick_uuid: uuid.  Uuid of the host this brick is located on
        :param peer: Peer.  Optional information about the Peer this brick
          is located on.
        :param path: String.  The filesystem path the brick is located at
        :param is_arbiter:  bool.  Whether this brick is an arbiter or not
        """
        self.uuid = brick_uuid
        self.peer = peer
        self.path = path
        self.is_arbiter = is_arbiter

    # Returns a String representation of the selected enum variant.
    def __str__(self):
        return "{}:{}".format(self.peer.hostname, self.path)


class Quota(object):
    def __init__(self, path: str, hard_limit: int, soft_limit: int,
                 soft_limit_percentage: str,
                 used: int, avail: int, soft_limit_exceeded: bool,
                 hard_limit_exceeded: bool):
        """
        A Quota can be used set limits on the pool usage.
        All limits are set in bytes.
        :param path: String. Filesystem path of the quota
        :param hard_limit: int. Hard byte limit
        :param soft_limit: int. Soft byte limit
        :param soft_limit_percentage: int. Soft limit percentage
        :param used: int.  Amount of bytes used of the quota amount
        :param avail: int.  Amount of bytes left of the quota amount
        :param soft_limit_exceeded: bool.  Soft limit has been exceeded
        :param hard_limit_exceeded: bool.  Hard limit has been exceeded.
        """
        self.path = path
        self.hard_limit = int(hard_limit)
        self.soft_limit = int(soft_limit)
        self.soft_limit_percentage = soft_limit_percentage
        self.used = int(used)
        self.avail = int(avail)
        if soft_limit_exceeded == "No":
            self.soft_limit_exceeded = False
        else:
            self.soft_limit_exceeded = True
        if hard_limit_exceeded == "No":
            self.hard_limit_exceeded = False
        else:
            self.hard_limit_exceeded = True

    """
    def __eq__(self, other):
        return self.path == other.path and \
               self.hard_limit == other.hard_limit and \
               self.soft_limit == other.soft_limit and \
               self.soft_limit_percentage == other.soft_limit_percentage and \
               self.used == other.used and self.avail == other.avail and \
               self.soft_limit_exceeded == other.soft_limit_exceeded and \
               self.hard_limit_exceeded == other.hard_limit_exceeded
    """

    def __str__(self):
        return "path:{path} hard limit:{hard_limit} " \
               "soft limit percentage: {soft_limit_percentage} " \
               "soft limit: {soft_limit} used: {used} " \
               "available: {avail} soft limit exceeded: {soft_exceeded} " \
               "hard limit exceeded: {hard_exceeded}" \
               "".format(path=self.path, hard_limit=self.hard_limit,
                         soft_limit_percentage=self.soft_limit_percentage,
                         soft_limit=self.soft_limit,
                         used=self.used,
                         avail=self.avail,
                         soft_exceeded=self.soft_limit_exceeded,
                         hard_exceeded=self.hard_limit_exceeded)


class BrickStatus(object):
    def __init__(self, brick: Brick, tcp_port: int, rdma_port: int,
                 online: bool, pid: int):
        """
        brick: Brick,
        tcp_port: u16.  The tcp port
        rdma_port: u16. The rdma port
        online: bool. Whether the Brick is online or not
        pid: u16.  The process id of the Brick
        """
        self.brick = brick
        self.tcp_port = tcp_port
        self.rdma_port = rdma_port
        self.online = online
        self.pid = pid

    def __eq__(self, other):
        return self.brick.peer == other.brick.peer

    def __str__(self):
        return "BrickStatus {} tcp port: {} rdma port: {} " \
               "online: {} pid: ".format(self.brick,
                                         self.tcp_port,
                                         self.rdma_port,
                                         self.online,
                                         self.pid)


class Transport(Enum):
    """
        An enum to select the transport method Gluster should import
        for the Volume
    """
    Tcp = "tcp"
    TcpAndRdma = "tcp,rdma"
    Rdma = "rdma"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(transport):
        if transport == "tcp":
            return Transport.Tcp
        elif transport == "tcp,rdma":
            return Transport.TcpAndRdma
        elif transport == "rdma":
            return Transport.Rdma
        elif transport == "0":
            return Transport.Tcp
        else:
            return None


class VolumeTranslator(Enum):
    Arbiter = "arbiter"
    Disperse = "disperse"
    Replica = "replica"
    Redundancy = "redundancy"
    Stripe = "stripe"

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(translator):
        if translator == "arbiter":
            return VolumeTranslator.Arbiter
        elif translator == "disperse":
            return VolumeTranslator.Disperse
        elif translator == "replica":
            return VolumeTranslator.Replica
        elif translator == "redundancy":
            return VolumeTranslator.Redundancy
        elif translator == "stripe":
            return VolumeTranslator.Stripe
        else:
            return None


# These are all the different Volume types that are possible in Gluster
# Note: Tier is not represented here becaimport I'm waiting for it to become
# more stable
# For more information about these types see: [Gluster Volume]
# (https:#gluster.readthedocs.
# org/en/latest/Administrator%20Guide/Setting%20Up%20Volumes/)
class VolumeType(Enum):
    Arbiter = "arbiter"
    Distribute = "distribute"
    Stripe = "stripe"
    Replicate = "replicate"
    StripedAndReplicate = "stripd-replicate"
    Disperse = "disperse"
    # Tier,
    DistributedAndStripe = "distributed-stripe"
    DistributedAndReplicate = "distributed-replicate"
    DistributedAndStripedAndReplicate = "distributed-striped-replicate"
    DistributedAndDisperse = "distributed-disperse"

    def __str__(self):
        return "{}".format(self.value)

    # Returns a enum variant of the given String
    @staticmethod
    def from_str(vol_type: str):
        if vol_type == "Arbiter":
            return VolumeType.Arbiter
        elif vol_type == "Distribute":
            return VolumeType.Distribute
        elif vol_type == "Stripe":
            return VolumeType.Stripe,
        elif vol_type == "Replicate":
            return VolumeType.Replicate
        elif vol_type == "Striped-Replicate":
            return VolumeType.StripedAndReplicate
        elif vol_type == "Disperse":
            return VolumeType.Disperse
            # TODO: Waiting for this to become stable
            # VolumeType::Tier => "Tier",
        elif vol_type == "Distributed-Stripe":
            return VolumeType.DistributedAndStripe
        elif vol_type == "Distributed-Replicate":
            return VolumeType.DistributedAndReplicate
        elif vol_type == "Distributed-Striped-Replicate":
            return VolumeType.DistributedAndStripedAndReplicate
        elif vol_type == "Distrubted-Disperse":
            return VolumeType.DistributedAndDisperse
        else:
            return None


class Volume(object):
    """
    A volume is a logical collection of bricks. Most of the gluster management
    operations happen on the volume.
    """

    def __init__(self, name: str, vol_type: VolumeType, vol_id: uuid.UUID,
                 status: str,
                 snapshot_count: int,
                 dist_count: int,
                 stripe_count: int, replica_count: int, arbiter_count: int,
                 disperse_count: int,
                 redundancy_count: int,
                 transport: Transport, bricks: List[Brick],
                 options: Dict[str, str]):
        """
        :param name: String.  Name of the volume
        :param vol_type: VolumeType.
        :param vol_id: uuid
        :param status: String.  Status of the volume
        :param transport: Transport.  Transport protocol
        :param bricks: list.  List of Brick
        :param options: dict.  String:String mapping of volume options
        """
        self.name = name
        self.vol_type = vol_type
        self.vol_id = vol_id
        self.status = status
        self.snapshot_count = snapshot_count
        self.dist_count = dist_count
        self.stripe_count = stripe_count
        self.replica_count = replica_count
        self.arbiter_count = arbiter_count
        self.disperse_count = disperse_count
        self.redundancy_count = redundancy_count
        self.transport = transport
        self.bricks = bricks
        self.options = options

    def __str__(self):
        return "name:{name} type:{type} id:{id} status:{status} " \
               "snap_count:{snap_count} distribute_count:{dist_count} " \
               "stripe_count:{stripe_count} replica_count:{replica_count} " \
               "arbiter_count:{arbiter_count} " \
               "disperse_count:{disperse_count} " \
               "redundancy_count:{redundancy_count} transport:{transport}" \
               "bricks: {bricks} options: {options}" \
               "".format(name=self.name, type=self.vol_type, id=self.vol_id,
                         status=self.status, snap_count=self.snapshot_count,
                         dist_count=self.dist_count,
                         redundancy_count=self.redundancy_count,
                         distribute_count=self.dist_count,
                         stripe_count=self.stripe_count,
                         replica_count=self.replica_count,
                         arbiter_count=self.arbiter_count,
                         disperse_count=self.disperse_count,
                         transport=self.transport, bricks=self.bricks,
                         options=self.options)


def volume_list() -> Result:
    """
    # Lists all available volume names.
    # # Failures
    # Will return None if the Volume list command failed or if volume could not
    # be transformed
    # into a String from utf8
    """
    arg_list = ["volume", "list", "--xml"]
    output = run_command("gluster", arg_list, True, False)
    if output.is_err():
        return Err(output.value)
    return parse_volume_list(output.value)


def parse_volume_list(volume_xml: str) -> Result:
    # Parse XML output
    tree = etree.fromstring(volume_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)
    volumes = tree.find('volList')
    parsed_volumes = []
    for vol in volumes:
        if vol.tag == 'volume':
            parsed_volumes.append(vol.text)
    return Ok(parsed_volumes)


def parse_volume_info(volume_xml: str) -> Result:
    """
    # Variables we will return in a class
    :param volume_xml: The output of the cli command with --xml flag
    :return list of Volume objects
    """
    tree = etree.fromstring(volume_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)

    volume_info_list = []
    volumes = tree.findall('./volInfo/volumes/volume')

    name = None
    vol_type = None
    vol_id = None
    status = None
    snapshot_count = None
    dist_count = None
    stripe_count = None
    replica_count = None
    arbiter_count = None
    disperse_count = None
    redundancy_count = None
    transport = None
    bricks = []
    options = {}

    for volume in volumes:
        for vol_info in volume:
            if vol_info.tag == 'name':
                name = vol_info.text
            elif vol_info.tag == 'id':
                vol_id = vol_info.text
            elif vol_info.tag == 'status':
                status = vol_info.text
            elif vol_info.tag == 'statusStr':
                pass
            elif vol_info.tag == 'snapshotCount':
                snapshot_count = vol_info.text
            elif vol_info.tag == 'distCount':
                dist_count = vol_info.text
            elif vol_info.tag == 'stripeCount':
                stripe_count = vol_info.text
            elif vol_info.tag == 'replicaCount':
                replica_count = vol_info.text
            elif vol_info.tag == 'arbiterCount':
                arbiter_count = vol_info.text
            elif vol_info.tag == 'disperseCount':
                disperse_count = vol_info.text
            elif vol_info.tag == 'redundancyCount':
                redundancy_count = vol_info.text
            elif vol_info.tag == 'typeStr':
                vol_type = VolumeType.from_str(vol_info.text)
            elif vol_info.tag == 'transport':
                transport = Transport.from_str(vol_info.text)
            elif vol_info.tag == 'options':
                for option in vol_info:
                    option_info = list(option.iter())
                    name = None
                    value = None
                    for entry in option_info:
                        if entry.tag == 'name':
                            name = entry.text
                        if entry.tag == 'value':
                            value = entry.text
                    options[name] = value
            elif vol_info.tag == 'bricks':
                for brick in vol_info:
                    name = None
                    brick_uuid = None
                    is_arbiter = None
                    for brick_info in brick:
                        if brick_info.tag == 'name':
                            name = brick_info.text
                        elif brick_info.tag == 'hostUuid':
                            brick_uuid = brick_info.text
                        elif brick_info.tag == 'isArbiter':
                            is_arbiter = brick_info.text
                    hostname = name.split(":")[0]
                    # Translate back into an IP address if needed
                    try:
                        ip_address(hostname)
                    except ValueError:
                        hostname = resolve_to_ip(hostname)
                    path = name.split(":")[1]
                    peer = get_peer(hostname)
                    bricks.append(
                        Brick(
                            brick_uuid=brick_uuid,
                            peer=peer,
                            path=path,
                            is_arbiter=is_arbiter))
        volume_info_list.append(
            Volume(name=name, vol_type=vol_type, vol_id=vol_id,
                   status=status,
                   snapshot_count=snapshot_count,
                   dist_count=dist_count,
                   stripe_count=stripe_count,
                   replica_count=replica_count,
                   disperse_count=disperse_count,
                   arbiter_count=arbiter_count,
                   redundancy_count=redundancy_count,
                   bricks=bricks,
                   options=options,
                   transport=transport))
    return Ok(volume_info_list)


def volume_info(volume: str) -> Result:
    """
    Returns a Volume with all available information on the volume
    volume: String.  The volume to gather info about
    :return: List[Volume].  The volume information
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "info", volume, '--xml']
    output = run_command("gluster", arg_list, True, False)

    if output.is_err():
        # TODO: What is the appropriate error to report here?
        # The client is using this to figure out if it should make a volume
        return Err("Volume info get cmd failed: {}".format(output.value))

    return parse_volume_info(output.value)


def quota_list(volume: str) -> Result:
    """
    Return a list of quotas on the volume if any
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: List of Quota's on the volume
    :raises: GlusterError if the command fails to run
    """
    args_list = ["volume", "quota", volume, "list", "--xml"]
    output = run_command("gluster", args_list, True, False)

    if output.is_err():
        return Err(
            "Volume quota list command failed with error: {}".format(
                output.value))

    quota_list_result = parse_quota_list(output.value)

    return quota_list_result


def parse_quota_list(output_xml: str) -> Result:
    """
    Return a list of quotas on the volume if any
    :param output_xml:
    """
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)

    parsed_quotas = []
    limits = tree.findall('./volQuota/limit')
    for limit in limits:
        path = None
        hard_limit = None
        soft_limit_percent = None
        soft_limit = None
        used_space = None
        avail_space = None
        soft_limit_exceeded = None
        hard_limit_exceeded = None
        for limit_info in limit:
            if limit_info.tag == 'path':
                path = limit_info.text
            elif limit_info.tag == 'hard_limit':
                hard_limit = int(limit_info.text)
            elif limit_info.tag == 'soft_limit_percent':
                soft_limit_percent = limit_info.text
            elif limit_info.tag == 'soft_limit_value':
                soft_limit = int(limit_info.text)
            elif limit_info.tag == 'used_space':
                used_space = int(limit_info.text)
            elif limit_info.tag == 'avail_space':
                avail_space = int(limit_info.text)
            elif limit_info.tag == 'sl_exceeded':
                soft_limit_exceeded = limit_info.text
            elif limit_info.tag == 'hl_exceeded':
                hard_limit_exceeded = limit_info.text
        quota = Quota(path=path, hard_limit=hard_limit,
                      soft_limit=soft_limit,
                      soft_limit_percentage=soft_limit_percent,
                      used=used_space, avail=avail_space,
                      soft_limit_exceeded=soft_limit_exceeded,
                      hard_limit_exceeded=hard_limit_exceeded)
        parsed_quotas.append(quota)

    return Ok(parsed_quotas)


def volume_enable_bitrot(volume: str) -> Result:
    """
    Enable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, "enable"]
    return run_command("gluster", arg_list, True, False)


def volume_disable_bitrot(volume: str) -> Result:
    """
    Disable bitrot detection and remediation on the volume
    volume: String.  The volume to operate on.
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, "disable"]
    return run_command("gluster", arg_list, True, False)


def volume_set_bitrot_option(volume: str, setting: BitrotOption) -> Result:
    """
    Set a bitrot option on the volume
    volume: String.  The volume to operate on.
    setting: BitrotOption.  The option to set on the bitrot daemon
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "bitrot", volume, "{}".format(setting),
                setting.option.value]
    return run_command("gluster", arg_list, True, True)


def volume_enable_quotas(volume: str) -> Result:
    """
    Enable quotas on the volume
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "enable"]
    return run_command("gluster", arg_list, True, False)


def volume_quotas_enabled(volume: str) -> bool:
    """
     Check if quotas are already enabled on a volume
    :return: bool.  True/False if quotas are enabled
    :raises: GlusterError if the command fails to run
    """
    vol_info = volume_info(volume)
    if vol_info.is_err():
        raise GlusterError(message=vol_info.value)
    for vol in vol_info.value:
        if vol.name == volume:
            quota = vol.options.get("features.quota")
            if quota is None:
                return False
            else:
                if quota == "off":
                    return False
                elif quota == "on":
                    return True
                else:
                    # No idea what this is
                    raise GlusterError(
                        "Unknown features.quota setting: {}.  Cannot discern "
                        "if quota is enabled or not".format(
                            quota))
    raise GlusterError(
        "Unknown volume: {}.  Failed to get quota information".format(volume))


def volume_disable_quotas(volume: str) -> Result:
    """
    Disable quotas on the volume
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "disable"]
    return run_command("gluster", arg_list, True, False)


def volume_remove_quota(volume: str, path: str) -> Result:
    """
    Removes a size quota to the volume and path.
    path: String.  Path of the directory to remove a quota on
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "remove", path]
    return run_command("gluster", arg_list, True, False)


def volume_add_quota(volume: str, path: str, size: int) -> Result:
    """
    Adds a size quota to the volume and path.
    volume: String Volume to add a quota to
    path: String.  Path of the directory to apply a quota on
    size: int. Size in bytes of the quota to apply
    :return: 0 on success
    :raises: GlusterError if the command fails to run
    """
    arg_list = ["volume", "quota", volume, "limit-usage", path,
                str(size)]

    return run_command("gluster", arg_list, True, False)


def ok_to_remove(volume: str, brick: Brick) -> Result:
    """
    Based on the replicas or erasure bits that are still available in the
    volume this will return
    True or False as to whether you can remove a Brick. This should be called
    before volume_remove_brick()
    volume: String. Volume to check if the brick is ok to remove
    brick: Brick. Brick to check
    :param volume: str.  Volume to check
    :param brick: Brick.  Brick to check if it is ok to remove
    :return: bool.  True/False if the Brick is safe to remove from the volume
    """
    arg_list = ["vol", "status", volume]

    output = run_command("gluster", arg_list, True, False)
    if output.is_err():
        return Err("vol status cmd failed with error: {}".format(output.value))

    parse_volume_status(output.value)

    # The redundancy requirement is needed here.
    # The code needs to understand what
    # volume type it's operating on.
    return Ok(True)


#  def volume_shrink_replicated(volume: str,
# replica_count: usize,
# bricks: Vec<Brick>,
# force) -> Result<i32,String>
# volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
# <start|stop|status|c
# ommit|force> - remove brick from volume <VOLNAME>
#
#


def parse_volume_status(output_xml: str) -> Result:
    tree = etree.fromstring(output_xml)

    return_code = 0
    err_string = ""

    for child in tree:
        if child.tag == 'opRet':
            return_code = int(child.text)
        elif child.tag == 'opErrstr':
            err_string = child.text

    if return_code != 0:
        return Err(err_string)

    status_list = []

    volumes = tree.findall('./volStatus/volumes/volume')
    for volume in volumes:
        for vol_info in volume:
            if vol_info.tag == 'node':
                hostname = None
                path = None
                peer_id = None
                status = None
                tcp_port = None
                rdma_port = None
                pid = None
                for node_info in vol_info:
                    if node_info.tag == 'hostname':
                        hostname = node_info.text
                    elif node_info.tag == 'path':
                        path = node_info.text
                    elif node_info.tag == 'peerid':
                        peer_id = node_info.text
                    elif node_info.tag == 'status':
                        status = node_info.text
                    elif node_info.tag == 'ports':
                        for port_info in node_info:
                            if port_info.tag == 'rdma':
                                rdma_port = port_info.text
                            elif port_info.tag == 'tcp':
                                tcp_port = port_info.text
                    elif node_info.tag == 'pid':
                        pid = node_info.text
                peer = Peer(uuid=peer_id, hostname=hostname, status=status)
                # The is_arbiter field isn't known yet so we'll leave
                # it as False
                brick = Brick(brick_uuid=peer_id, peer=peer, path=path,
                              is_arbiter=False)
                # The online field isn't known yet so we'll leave it as False
                brick_status = BrickStatus(brick=brick,
                                           tcp_port=tcp_port,
                                           rdma_port=rdma_port,
                                           online=False,
                                           pid=pid)
                status_list.append(brick_status)
    return Ok(status_list)


def volume_status(volume: str) -> Result:
    """
        Query the status of the volume given.
        :return: list.  List of BrickStatus
        :raise: Raises GlusterError on exception
    """
    arg_list = ["vol", "status", volume, "--xml"]

    output = run_command("gluster", arg_list, True, False)
    if output.is_err():
        return Err(output.value)
    bricks = parse_volume_status(output.value)
    if bricks.is_err():
        return Err(bricks.value)

    return Ok(bricks.value)

    #  def volume_shrink_replicated(volume: str,
    # replica_count: usize,
    # bricks: Vec<Brick>,
    # force) -> Result<i32,String>
    # volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
    # <start|stop|status|c
    # ommit|force> - remove brick from volume <VOLNAME>
    #
    #


# This will remove a brick from the volume
# # Failures
# Will return GlusterError if the command fails to run
def volume_remove_brick(volume: str, bricks: List[Brick],
                        force: bool) -> Result:
    """
    This will remove bricks from the volume
    :param volume: String of the volume to remove bricks from.
    :param bricks:  list.  List of bricks to remove from the volume
    :param force:  bool.  Force remove brick
    :return: int.  Negative number on error
    """

    if len(bricks) == 0:
        return Err("The brick list is empty.  Not removing brick")

    for brick in bricks:
        ok = ok_to_remove(volume, brick)
        if ok:
            arg_list = ["volume", "remove-brick", volume, "{}".format(brick)]
            if force:
                arg_list.append("force")

            arg_list.append("start")
            output = run_command("gluster", arg_list, True, True)
            if output.is_err():
                return Err(
                    "Remove brick failed with error: {}".format(output.value))
        else:
            return Err(
                "Unable to remove brick due to redundancy failure")


# Will return GlusterError if the command fails to run
def volume_add_brick(volume: str, bricks: List[Brick], force: bool) -> Result:
    """
    volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
    <NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
    This adds a new brick to the volume
    :param volume: String of the volume to add bricks to.
    :param bricks:  list.  List of bricks to add to the volume
    :param force:  bool.  Force add brick
    :return: Result.  Ok or Err
    """

    if len(bricks) == 0:
        raise GlusterError("The brick list is empty.  Not expanding volume")
    arg_list = ["volume", "add-brick", volume]
    for brick in bricks:
        arg_list.append(str(brick))

    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, True)


def volume_start(volume: str, force: bool) -> Result:
    # Should I check the volume exists first?
    """
    Once a volume is created it needs to be started.  This starts the volume
    :param volume: String of the volume to start.
    :param force:  bool.  Force start
    :return: Result.  Ok or Err
    """
    arg_list = ["volume", "start", volume]

    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, True)


def volume_stop(volume: str, force: bool) -> Result:
    """
    This stops a running volume
    :param volume:  String of the volume to stop
    :param force:  bool. Force stop.
    :return: Result.  Ok or Err
    """
    arg_list = ["volume", "stop", volume]

    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, True)


def volume_delete(volume: str) -> Result:
    """
    This deletes a stopped volume
    :param volume:  String of the volume name to delete
    :return: Result.  Ok or Err
    """
    arg_list = ["volume", "delete", volume]
    return run_command("gluster", arg_list, True, True)


def volume_rebalance(volume: str) -> Result:
    """
    # This function doesn't do anything yet.  It is a place holder because
    # volume_rebalance is a long running command and I haven't decided how to
    # poll for completion yet
    # Usage: volume rebalance <VOLNAME> fix-layout start | start
    # [force]|stop|status
    :param volume: str.  The name of the volume to start rebalancing
    :return: Result.  Ok or Err
    """
    arg_list = ["volume", "rebalance", volume, "start"]
    return run_command("gluster", arg_list, True, True)


def volume_create(volume: str, options: Dict[VolumeTranslator, str],
                  transport: Transport, bricks: List[Brick],
                  force: bool) -> Result:
    """

    :param volume: String.  Name of the volume to create
    :param options:  dict of VolumeTranslator:String mappings.
    :param transport: Transport.  The transport to use
    :param bricks: list of Brick.  Bricks to put into the volume
    :param force.  Should volume creation be forced or not
    :return: Result.  Ok or Err
    :raise GlusterError:
    """
    if len(bricks) == 0:
        raise GlusterError("The brick list is empty. Not creating volume")

    arg_list = ["volume", "create", volume]
    for key, value in options.items():
        arg_list.append(str(key))
        arg_list.append(value)

    arg_list.append("transport")
    arg_list.append(str(transport))

    for brick in bricks:
        arg_list.append(str(brick))

    if force:
        arg_list.append("force")

    return run_command("gluster", arg_list, True, True)


def vol_set(volume: str, option: GlusterOption) -> Result:
    """
    :param volume: String. Volume name to set the option on
    :param option: GlusterOption
    :return: Result.  Return code and output of cmd
    """
    arg_list = ["volume", "set", volume, option.option, str(option.value)]
    return run_command("gluster", arg_list, True, True)


def volume_set_options(volume: str, settings: List[GlusterOption]) -> Result:
    """
    Set an option on the volume
    :param volume: String. Volume name to set the option on
    :param settings: list of GlusterOption
    """
    # # Failures
    # Will return GlusterError if the command fails to run
    error_list = []
    for setting in settings:
        result = vol_set(volume, setting)
        if result.is_err():
            error_list.append(result.value)

    if len(error_list) > 0:
        return Err("\n".join(error_list))
    return Ok()


def volume_create_replicated(volume: str, replica_count: int,
                             transport: Transport, bricks: List[Brick],
                             force: bool) -> Result:
    """
    This creates a new replicated volume
    :param replica_count:
    :param transport:
    :param bricks:
    :param force:
    :param volume: String. Volume name to set the option on
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    volume_translators = {VolumeTranslator.Replica: str(replica_count)}

    return volume_create(volume, volume_translators, transport, bricks, force)


def volume_create_arbiter(volume: str, replica_count: int, arbiter_count: int,
                          transport: Transport,
                          bricks: List[Brick], force: bool) -> Result:
    """
    The arbiter volume is special subset of replica volumes that is aimed at
    preventing split-brains and providing the same consistency guarantees
    as a normal replica 3 volume without consuming 3x space.
    :param volume:
    :param replica_count:
    :param arbiter_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    volume_translators = {VolumeTranslator.Replica: str(replica_count),
                          VolumeTranslator.Arbiter: str(arbiter_count)}

    return volume_create(volume, volume_translators, transport, bricks, force)


def volume_create_striped(volume: str, stripe_count: int, transport: Transport,
                          bricks: List[Brick], force: bool) -> Result:
    """
    This creates a new striped volume
    :param volume:
    :param stripe_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    volume_translators = {VolumeTranslator.Stripe: str(stripe_count)}

    return volume_create(volume, volume_translators, transport, bricks, force)


def volume_create_striped_replicated(volume: str, stripe_count: int,
                                     replica_count: int,
                                     transport: Transport, bricks: List[Brick],
                                     force: bool) -> Result:
    """
    This creates a new striped and replicated volume
    :param volume:
    :param stripe_count:
    :param replica_count:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    volume_translators = {VolumeTranslator.Stripe: str(stripe_count),
                          VolumeTranslator.Replica: str(replica_count)}

    return volume_create(volume, volume_translators, transport, bricks, force)


def volume_create_distributed(volume: str, transport: Transport,
                              bricks: List[Brick], force: bool) -> Result:
    """
    This creates a new distributed volume
    :param volume:
    :param transport:
    :param bricks:
    :param force:
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    return volume_create(volume, {}, transport, bricks,
                         force)


def volume_create_erasure(volume: str, disperse_count: int,
                          redundancy_count: int,
                          transport: Transport,
                          bricks, force: bool) -> Result:
    """
    This creates a new erasure coded volume
    :param volume: String
    :param disperse_count: int
    :param redundancy_count: int
    :param transport: Transport
    :param bricks: list of Brick
    :param force: bool
    :return: Result.  If Ok() stdout is returned.  Err returns stderr
    """
    volume_translators = {VolumeTranslator.Disperse: str(disperse_count),
                          VolumeTranslator.Redundancy: str(redundancy_count)}

    return volume_create(volume, volume_translators, transport, bricks,
                         force)


def get_local_bricks(volume: str) -> Result:
    """
        Return all bricks that are being served locally in the volume
        volume: Name of the volume to get local bricks for
    """
    vol_info = volume_info(volume)
    if vol_info.is_err():
        return Err(vol_info.value)
    local_ip = get_local_ip()
    local_brick_list = []
    for volume in vol_info.value:
        for brick in volume.bricks:
            if brick.peer.hostname == local_ip:
                local_brick_list.append(brick)
    return Ok(local_brick_list)
