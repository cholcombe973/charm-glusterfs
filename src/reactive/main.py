import apt_pkg
import itertools
import os
import time
from enum import Enum
from typing import List, Optional

from charms.reactive import when, when_not, when_file_changed, set_state
from charmhelpers.contrib.storage.linux.ceph import filesystem_mounted
from charmhelpers.core import hookenv, sysctl
from charmhelpers.core.hookenv import relation_get, \
    application_version_set, relation_id
from charmhelpers.core.hookenv import config, ERROR, INFO, is_leader, \
    log, related_units, relation_set, status_set
from charmhelpers.core.host import add_to_updatedb_prunepath, umount
from charmhelpers.core.unitdata import kv
from charmhelpers.fetch import apt_update, add_source, apt_install
from result import Err, Ok, Result

from lib.gluster.lib import GlusterOption, SplitBrainPolicy, Toggle, run_command
# from .ctdb import VirtualIp
# from .nfs_relation_joined import nfs_relation_joined
from lib.gluster.peer import State, peer_list, peer_probe, peer_status, Peer
from lib.gluster.volume import Brick, Transport, volume_create_arbiter, \
    get_local_bricks, Volume, \
    volume_create_distributed, volume_create_striped, \
    volume_create_replicated, volume_create_striped_replicated, \
    volume_add_brick, volume_create_erasure, volume_info, VolumeType, \
    volume_enable_bitrot, volume_start, volume_set_options, volume_remove_brick
from .block import BrickDevice, Btrfs, Ext4, format_block_device, \
    FilesystemType, get_juju_bricks, get_manual_bricks, \
    get_device_info, MetadataProfile, mount_device, set_elevator, Scheduler, \
    weekly_defrag, \
    Xfs, Zfs
from .apt import get_candidate_package_version
# from .brick_detached import brick_detached
from .fstab import FsTab, FsEntry
# from .fuse_relation_joined import fuse_relation_joined
# from .metrics import collect_metrics
# from .server_removed import server_removed
from .upgrade import roll_cluster


class Status(Enum):
    """
    Need more expressive return values so we can wait on peers
    """
    Created = 0,
    WaitForMorePeers = 1,
    InvalidConfig = 2,
    FailedToCreate = 3,
    FailedToStart = 4,


"""
#TODO: Deferred
def get_cluster_networks() -> Result: # -> Result<Vec<ctdb.VirtualIp>, str>:
    # Return all the virtual ip networks that will be used
    cluster_networks = []#: Vec<ctdb.VirtualIp> = Vec.new()
    config_value = config["virtual_ip_addresses"]
    if config_value is None:
        config_value = cluster_networks
    virtual_ips = config_value.split(" ")
    for vip in virtual_ips:
        if len(vip) is 0:
            continue
        network = ctdb.ipnetwork_from_str(vip)
        interface = ctdb.get_interface_for_address(network)
            # .ok_or("Failed to find interface for network {}".format(network))
        cluster_networks.append(VirtualIp(cidr=network,interface=interface))
    return Ok(cluster_networks)
"""


@when_not('gluster.installed')
def install():
    add_source(config('source'), config('key'))
    apt_update(fatal=True)
    apt_install(
        packages=["ctdb", "nfs-common", "glusterfs-server", "glusterfs-common",
                  "glusterfs-client"], fatal=True)
    set_state("gluster.installed")


@when_file_changed('config.yaml')
def config_changed() -> Result:
    """

    :return:
    """
    r = check_for_new_devices()
    if r.is_err():
        log("Checking for new devices failed with error: {".format(r.value),
            ERROR)
    r = check_for_sysctl()
    if r.is_err():
        log("Setting sysctl's failed with error: {".format(r.value), ERROR)
    # If fails we fail the hook
    check_for_upgrade()
    return Ok(())


def check_for_new_devices() -> Result:
    """

    :return:
    """
    log("Checking for new devices", INFO)
    log("Checking for ephemeral unmount")
    ephemeral_unmount()
    # if config.changed("brick_devices"))
    brick_devices = []
    # Get user configured storage devices
    manual_brick_devices = get_manual_bricks()
    if manual_brick_devices.is_err():
        return Err(manual_brick_devices.value)
    brick_devices.extend(manual_brick_devices.value)

    # Get the juju storage block devices
    juju_config_brick_devices = get_juju_bricks()
    if juju_config_brick_devices.is_err():
        return Err(juju_config_brick_devices.value)
    brick_devices.extend(juju_config_brick_devices.value)

    log("storage devices: {}".format(brick_devices))

    format_handles = []
    brick_paths = []
    # Format all drives in parallel
    for device in brick_devices:
        if not device.initialized:
            log("Calling initialize_storage for {}".format(device.dev_path))
            # Spawn all format commands in the background
            format_handles.append(initialize_storage(device))
        else:
            # The device is already initialized, lets add it to our
            # usable paths list
            log("{} is already initialized".format(device.dev_path))
            brick_paths.append(device.mount_path)
    # Wait for all children to finish formatting their drives
    for handle in format_handles:
        output_result = handle.format_child.wait_with_output()
        if output_result.is_ok():
            # success
            # 1. Run any post setup commands if needed
            finish_initialization(handle.device.dev_path)
            brick_paths.append(handle.device.mount_path)
        else:
            # Failed
            log("Device {} formatting failed with error: {}. Skipping".format(
                handle.device.dev_path, output_result.value), ERROR)
    log("Usable brick paths: {}".format(brick_paths))
    return Ok(())


def brick_and_server_cartesian_product(peers: List[Peer], paths: List[str]) -> \
        List[Brick]:
    """

    :param peers: A list of peers to match up against brick paths
    :param paths: A list of brick mount paths to match up against peers
    :return:
    """
    product = []  # : Vec<gluster.volume.Brick> = []
    it = itertools.product(paths,
                           peers)  # .iter().cartesian_product(peers.iter())
    for path, host in it:
        brick = Brick(peer=host, path=path, is_arbiter=False,
                      brick_uuid=None)
        product.append(brick)
    return product


@when_not("volume.started")
def server_changed() -> Result:
    """

    :return:
    """
    volume_name = config("volume_name")

    if is_leader():
        log("I am the leader: {}".format(relation_id()))
        log("Loading config", INFO)
        status_set(workload_state="maintenance",
                   message="Checking for new peers to probe")
        peers = peer_list()
        if peers.is_err():
            return Err(peers.value)
        log("peer list: {}".format(peers))
        probe_in_units(peers.value, related_units())
        # Update our peer list
        peers = peer_list()
        if peers.is_err():
            return Err(peers.value)

        # Everyone is in.  Lets see if a volume exists
        vol_info = volume_info(volume_name)
        existing_volume = False
        if vol_info.is_err():
            return Err("Volume info command failed: {}".format(vol_info.value))
        if len(vol_info.value) > 0:
            log("Expanding volume {}".format(volume_name), INFO)
            status_set(workload_state="maintenance",
                       message="Expanding volume {}".format(
                           volume_name))
            expand_vol = expand_volume(peers.value, vol_info.value)
            if expand_vol.is_ok():
                log("Expand volume succeeded.", INFO)
                status_set(workload_state="active",
                           message="Expand volume succeeded.")
                # Poke the other peers to update their status
                relation_set("expanded", "True")
                # Ensure the cluster is mounted
                # mount_cluster(volume_name)
                # setup_ctdb()
                # setup_samba(volume_name)
                return Ok(())
            else:
                log("Expand volume failed with output: {}".format(
                    expand_vol.value), ERROR)
                status_set(workload_state="blocked",
                           message="Expand volume failed.  Please check juju "
                                   "debug-log.")
                return Err(expand_vol.value)
        if not existing_volume:
            log("Creating volume {}".format(volume_name), INFO)
            status_set(workload_state="maintenance",
                       message="Creating volume {}".format(volume_name))
            create_gluster_volume(volume_name, peers.value)
            # mount_cluster(volume_name)
            # setup_ctdb()
            # setup_samba(volume_name)
        return Ok(())
        # else:
        #    # Non leader units
        #    vol_started = relation_get("started")
        #    if vol_started is not None:
        #        mount_cluster(volume_name)
        # Setup ctdb and samba after the volume comes up on non leader units
        #        # setup_ctdb()
        #        # setup_samba(volume_name)
        #    return Ok(())


def create_gluster_volume(volume_name: str, peers: List[Peer]) -> Result:
    """
    Create a new gluster volume with a name and a list of peers
    :param volume_name: str.  Name of the volume to create
    :param peers: List[Peer].  List of the peers to use in this volume
    :return:
    """
    create_vol = create_volume(peers, None)
    if create_vol.is_ok():
        if create_vol.value == Status.Created:
            log("Create volume succeeded.", INFO)
            status_set(workload_state="maintenance",
                       message="Create volume succeeded")
            start_gluster_volume(volume_name)
            # Poke the other peers to update their status
            relation_set("started", "True")
            return Ok(())
        elif create_vol.value == Status.WaitForMorePeers:
            log("Waiting for all peers to enter the Peer in Cluster status")
            status_set(workload_state="maintenance",
                       message="Waiting for all peers to enter "
                               "the \"Peer in Cluster status\"")
            return Ok(())
        else:
            # Status is failed
            # What should I return here
            return Ok(())
    else:
        log("Create volume failed with output: {}".format(create_vol.value),
            ERROR)
        status_set(workload_state="blocked",
                   message="Create volume failed.  Please check "
                           "juju debug-log.")
        return Err(create_vol.value)


def create_volume(peers: List[Peer], volume_info: Optional[Volume]) -> Result:
    """
        Create a new volume if enough peers are available
        :param peers:
        :param volume_info:
        :return:
    """
    cluster_type_config = config("cluster_type")
    cluster_type = VolumeType(cluster_type_config)
    volume_name = config("volume_name")
    replicas = 3
    try:
        replicas = int(config("replication_level"))
    except ValueError:
        log(format("Invalid integer {} for replicas.  "
                   "Defaulting to 3.".format(config("replication_level"))))

    extra = 1
    try:
        extra = int(config("extra_level"))
    except ValueError:
        log("Invalid integer {} for extra_level.  Defaulting to 1.".format(
            config("extra_level")))
        extra = 1
    # Make sure all peers are in the cluster
    # spin lock
    wait_for_peers()

    # Build the brick list
    brick_list = get_brick_list(peers, volume_info)
    if brick_list.is_err():
        if brick_list.value is Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            status_set(workload_state="maintenance",
                       message="Waiting for more peers")
            return Ok(Status.WaitForMorePeers)
        elif brick_list.value is Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err("Unknown error in create volume: {}".format(
                brick_list.value))

    log("Got brick list: {}".format(brick_list.value))
    log("Creating volume of type {} with brick list {}".format(
        cluster_type, brick_list.value), INFO)

    if cluster_type is VolumeType.Distribute:
        result = volume_create_distributed(
            volume_name, Transport.Tcp, brick_list.value, True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.Stripe:
        result = volume_create_striped(
            volume_name, replicas, Transport.Tcp, brick_list.value, True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.Replicate:
        result = volume_create_replicated(
            volume_name, replicas, Transport.Tcp, brick_list.value, True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.Arbiter:
        result = volume_create_arbiter(volume_name,
                                       replicas,
                                       extra,
                                       Transport.Tcp,
                                       brick_list.value,
                                       True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.StripedAndReplicate:
        result = volume_create_striped_replicated(volume_name,
                                                  extra,
                                                  replicas,
                                                  Transport.Tcp,
                                                  brick_list.value,
                                                  True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.Disperse:
        result = volume_create_erasure(volume_name,
                                       replicas,
                                       extra,
                                       Transport.Tcp,
                                       brick_list.value,
                                       True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.DistributedAndStripe:
        result = volume_create_striped(volume_name,
                                       replicas, Transport.Tcp,
                                       brick_list.value, True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.DistributedAndReplicate:
        result = volume_create_replicated(volume_name,
                                          replicas,
                                          Transport.Tcp, brick_list.value, True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.DistributedAndStripedAndReplicate:
        result = volume_create_striped_replicated(volume_name,
                                                  extra,
                                                  replicas,
                                                  Transport.Tcp,
                                                  brick_list.value,
                                                  True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)
    elif cluster_type is VolumeType.DistributedAndDisperse:
        result = volume_create_erasure(
            volume_name,
            # TODO: This number has to be lower than the brick length
            # brick_list.len()-1,
            replicas,
            extra,
            Transport.Tcp,
            brick_list.value,
            True)
        if result.is_err():
            log("Failed to create volume: {}".format(result.value), ERROR)
            return Err(Status.FailedToCreate)
        return Ok(Status.Created)


def expand_volume(peers: List[Peer], vol_info: Optional[Volume]) -> Result:
    # Expands the volume by X servers+bricks
    # Adds bricks and then runs a rebalance
    """

    :param peers:
    :param vol_info:
    :return:
    """
    volume_name = config("volume_name")
    # Are there new peers
    log("Checking for new peers to expand the volume named {}".format(
        volume_name))
    # Build the brick list
    brick_list = get_brick_list(peers, vol_info)
    if brick_list.is_ok():
        log("Expanding volume with brick list: {}".format(
            brick_list.value), INFO)
        return volume_add_brick(volume_name, brick_list.value, True)
    else:
        if brick_list.value is Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            return Ok(0)
        elif brick_list.value is Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err(
                "Unknown error in expand volume: {}".format(brick_list.value))


def get_brick_list(peers: List[Peer], volume: Optional[Volume]) -> Result:
    """
    This function will take into account the replication level and
    try its hardest to produce a list of bricks that satisfy this:
    1. Are not already in the volume
    2. Sufficient hosts to satisfy replication level
    3. Stripped across the hosts
    If insufficient hosts exist to satisfy this replication level this will
    return no new bricks to add
    Default to 3 replicas if the parsing fails

    :param peers:
    :param volume:
    :return:
    """
    brick_devices = []
    replica_config = config("replication_level")
    replicas = 3
    try:
        replicas = int(replica_config)
    except ValueError:
        # Use default
        pass

    # TODO: Should this fail the hook or just keep going
    log("Checking for ephemeral unmount")
    ephemeral_unmount()

    # Get user configured storage devices
    manual_brick_devices = get_manual_bricks()
    if manual_brick_devices.is_err():
        return Err(manual_brick_devices.value)
    brick_devices.extend(manual_brick_devices.value)

    # Get the juju storage block devices
    juju_config_brick_devices = get_juju_bricks()
    if juju_config_brick_devices.is_err():
        return Err(juju_config_brick_devices.value)
    brick_devices.extend(juju_config_brick_devices.value)

    log("storage devices: {}".format(brick_devices))

    format_handles = []  #: Vec<block.AsyncInit> = []
    brick_paths = []
    # Format all drives in parallel
    for device in brick_devices:
        if not device.initialized:
            log("Calling initialize_storage for {}".format(device.dev_path))
            # Spawn all format commands in the background
            format_handles.append(initialize_storage(device))
        else:
            # The device is already initialized,
            # lets add it to our usable paths list
            log("{} is already initialized".format(device.dev_path))
            brick_paths.append(device.mount_path)
    # Wait for all children to finish formatting their drives
    for handle in format_handles:
        output_result = handle.format_child.wait_with_output()
        if output_result.is_ok():
            # success
            # 1. Run any post setup commands if needed
            finish_result = finish_initialization(handle.device.dev_path)
            if finish_result.is_err():
                return Err(Status.InvalidConfig)

            brick_paths.append(handle.device.mount_path)
        else:
            # Failed
            log("Device {} formatting failed with error: {}. "
                "Skipping".format(handle.device.dev_path, output_result.value),
                ERROR)
    log("Usable brick paths: {}".format(brick_paths))

    if volume is None:
        log("Volume is none")
        # number of bricks % replicas == 0 then we're ok to proceed
        if len(peers) < replicas:
            # Not enough peers to replicate across
            log("Not enough peers to satisfy the replication level for the Gluster \
                        volume.  Waiting for more peers to join.")
            return Err(Status.WaitForMorePeers)
        elif len(peers) == replicas:
            # Case 1: A perfect marriage of peers and number of replicas
            log("Number of peers and number of replicas match")
            return Ok(brick_and_server_cartesian_product(peers, brick_paths))
        else:
            # Case 2: We have a mismatch of replicas and hosts
            # Take as many as we can and leave the rest for a later time
            count = len(peers) - (len(peers) % replicas)
            new_peers = peers
            # Drop these peers off the end of the list
            new_peers = new_peers[count:]
            log("Too many new peers.  Dropping {} peers off the list".format(
                count))
            return Ok(brick_and_server_cartesian_product(
                new_peers, brick_paths))

    else:
        # Existing volume.  Build a differential list.
        log("Existing volume.  Building differential brick list")
        new_peers = find_new_peers(peers, volume)

        if len(new_peers) < replicas:
            log("New peers found are less than needed by the replica count")
            return Err(Status.WaitForMorePeers)
        elif len(new_peers) == replicas:
            log("New peers and number of replicas match")
            return Ok(brick_and_server_cartesian_product(
                new_peers, brick_paths))
        else:
            count = len(new_peers) - (len(new_peers) % replicas)
            # Drop these peers off the end of the list
            log("Too many new peers.  Dropping {} peers off the list".format(
                count))
            new_peers = new_peers[count:]
            return Ok(brick_and_server_cartesian_product(
                new_peers, brick_paths))


"""
# TODO: Deferred
# Add all the peers in the gluster cluster to the ctdb cluster

def setup_ctdb() -> Result:
    if config["virtual_ip_addresses"] is None:
        # virtual_ip_addresses isn't set.  Skip setting ctdb up
        return Ok(())

    log("setting up ctdb")
    peers = peer_list()
    log("Got ctdb peer list: {}".format(peers))
    cluster_addresses: Vec<IpAddr> = []
    for peer in peers:
        address = IpAddr.from_str(peer.hostname).map_err(|e| e)
        cluster_addresses.append(address)
    log("writing /etc/default/ctdb")
    ctdb_conf = File.create("/etc/default/ctdb").map_err(|e| e)
    ctdb.render_ctdb_configuration(ctdb_conf).map_err(|e| e)
    cluster_networks = get_cluster_networks()
    log("writing /etc/ctdb/public_addresses")
    public_addresses =
        File.create("/etc/ctdb/public_addresses").map_err(|e| e)
    ctdb.render_ctdb_public_addresses(public_addresses, cluster_networks)
        .map_err(|e| e)

    log("writing /etc/ctdb/nodes")
    cluster_nodes = File.create("/etc/ctdb/nodes").map_err(|e| e)
    ctdb.render_ctdb_cluster_nodes(cluster_nodes, cluster_addresses)
        .map_err(|e| e)

    # Start the ctdb service
    log("Starting ctdb")
    apt.service_start("ctdb")

    return Ok(())
"""


def shrink_volume(peer: Peer, vol_info: Optional[Volume]):
    """

    :param peer:
    :param vol_info:
    :return:
    """
    volume_name = config("volume_name")
    log("Shrinking volume named  {}".format(volume_name), INFO)
    peers = [peer]

    # Build the brick list
    brick_list = get_brick_list(peers, vol_info)
    if brick_list.is_ok():
        log("Shrinking volume with brick list: {}".format(brick_list), INFO)
        return volume_remove_brick(volume_name, brick_list.value, True)
    else:
        if brick_list.value == Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            return Ok(0)
        elif brick_list.value == Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err("Unknown error in shrink volume: {}".format(
                brick_list.value))


@when('glusterfs.mounted')
@when_not("volume-options.set")
def set_volume_options():
    """
    Set any options needed on the volume.
    :return:
    """
    status_set(workload_state="maintenance", message="Setting volume options")
    volume_name = config('volume_name')
    settings = [
        # Starting in gluster 3.8 NFS is disabled in favor of ganesha.
        # I'd like to stick with the legacy version a bit longer.
        GlusterOption(option=GlusterOption.NfsDisable, value=Toggle.Off),
        GlusterOption(option=GlusterOption.DiagnosticsLatencyMeasurement,
                      value=Toggle.On),
        GlusterOption(option=GlusterOption.DiagnosticsCountFopHits,
                      value=Toggle.On),
        # Dump FOP stats every 5 seconds.
        # NOTE: On slow main drives this can severely impact them
        GlusterOption(option=GlusterOption.DiagnosticsFopSampleInterval,
                      value=5),
        GlusterOption(option=GlusterOption.DiagnosticsStatsDumpInterval,
                      value=30),
        # 1HR DNS timeout
        GlusterOption(option=GlusterOption.DiagnosticsStatsDnscacheTtlSec,
                      value=3600),
        # Set parallel-readdir on.  This has a very nice performance
        # benefit as the number of bricks/directories grows
        GlusterOption(option=GlusterOption.PerformanceParallelReadDir,
                      value=Toggle.On),
        GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                      value=Toggle.On),
        # Start with 20MB and go from there
        GlusterOption(
            option=GlusterOption.PerformanceReadDirAheadCacheLimit,
            value=1024 * 1024 * 20)]

    # Set the split brain policy if requested
    splitbrain_policy = config("splitbrain_policy")
    if splitbrain_policy is not None:
        # config.yaml has a default here.  Should always have a value
        try:
            policy = SplitBrainPolicy(splitbrain_policy)
            settings.append(
                GlusterOption(option=GlusterOption.FavoriteChildPolicy,
                              value=policy))
        except ValueError:
            log("Failed to parse splitbrain_policy config setting: \
                                          {}.".format(splitbrain_policy), ERROR)
    else:
        volume_set_options(volume_name, settings)

    # The has a default.  Should be safe
    bitrot_config = bool(config("bitrot_detection"))
    if bitrot_config:
        log("Enabling bitrot detection")
        status_set(workload_state="active",
                   message="Enabling bitrot detection.")
        _ = volume_enable_bitrot(volume_name)
    # Tell reactive we're all set here
    set_state("volume-options.set")


def start_gluster_volume(volume_name: str) -> Result:
    """

    :param volume_name:
    :return:
    """
    start_vol_result = volume_start(volume_name, False)
    if start_vol_result.is_ok():
        log("Starting volume succeeded.", INFO)
        status_set(workload_state="active",
                   message="Starting volume succeeded.")
        return Ok(())
    else:
        log("Start volume failed with output: {}".format(
            start_vol_result.value), ERROR)
        status_set(workload_state="blocked",
                   message="Start volume failed.  Please check juju debug-log.")
        return Err(start_vol_result.value)


def check_for_sysctl() -> Result:
    """

    :return:
    """
    if config.changed("sysctl"):
        config_path = os.path.join(os.sep, "etc", "sysctl.d",
                                   "50-gluster-charm.conf")
        sysctl_dict = config("sysctl")
        if sysctl_dict is not None:
            sysctl.create(sysctl_dict, config_path)
    return Ok(())


def check_for_upgrade() -> Result:
    """
    If the config has changed this will initiated a rolling upgrade

    :return:
    """
    config = hookenv.config()
    if not config.changed("source"):
        # No upgrade requested
        log("No upgrade requested")
        return Ok(())

    log("Getting current_version")
    current_version = get_glusterfs_version()

    log("Adding new source line")
    source = config("source")
    if not source.is_some():
        # No upgrade requested
        log("Source not set.  Cannot continue with upgrade")
        return Ok(())
    add_source(source)
    log("Calling apt update")
    apt_update()

    log("Getting proposed_version")
    apt_pkg.init_system()
    proposed_version = get_candidate_package_version("glusterfs-server")
    if proposed_version.is_err():
        return Err(proposed_version.value)
    version_compare = apt_pkg.version_compare(a=proposed_version.value,
                                              b=current_version)

    # Using semantic versioning if the new version is greater
    # than we allow the upgrade
    if version_compare > 0:
        log("current_version: {}".format(current_version))
        log("new_version: {}".format(proposed_version.value))
        log("{} to {} is a valid upgrade path.  Proceeding.".format(
            current_version, proposed_version.value))
        return roll_cluster(proposed_version.value)
    else:
        # Log a helpful error message
        log("Invalid upgrade path from {} to {}. The new version needs to be \
                            greater than the old version".format(
            current_version, proposed_version.value), ERROR)
        return Ok(())


def peers_are_ready(peer_list: Result) -> bool:
    """

    :param peer_list: 
    :return: 
    """
    if peer_list.is_ok():
        log("Got peer status: {}".format(peer_list.value))
        return all(
            peer.status == State.PeerInCluster for peer in peer_list.value)
    else:
        log("peers_are_ready failed to get peer status: {}".format(
            peer_list.value), ERROR)
        return False


def wait_for_peers() -> Result:
    """
    HDD's are so slow that sometimes the peers take long to join the cluster.
    This will loop and wait for them ie spinlock

    :return: 
    """
    log("Waiting for all peers to enter the Peer in Cluster status")
    status_set(workload_state="maintenance",
               message="Waiting for all peers to enter the "
                       "\"Peer in Cluster status\"")
    iterations = 0
    while not peers_are_ready(peer_status()):
        time.sleep(1)
        iterations += 1
        if iterations > 600:
            return Err("Gluster peers failed to connect after 10 minutes")
    return Ok(())


def probe_in_units(existing_peers: List[Peer], related_units: List) -> Result:
    """
    Probe in a unit if they haven't joined yet
    This function is confusing because Gluster has weird behavior.
    1. If you probe in units by their IP address it works.
    The CLI will show you their resolved
    hostnames however
    2. If you probe in units by their hostname instead it'll still work
    but gluster client mount
    commands will fail if it can not resolve the hostname.
    For example: Probing in containers by hostname will cause the glusterfs
    client to fail to mount on the container host.  :(
    3. To get around this I'm converting hostnames to ip addresses in the
    gluster library to mask this from the callers.
    #
    :param existing_peers:
    :param related_units:
    :return:
    """
    log("Adding in related_units: {}".format(related_units))
    for unit in related_units:
        address = relation_get("private-address", unit)
        if address is None:
            log("unit {} private-address was blank, skipping.".format(unit))
            continue

        address_trimmed = address.strip()
        already_probed = any(peer.hostname == address_trimmed for peer in
                             existing_peers)
        # .iter().any(|peer| peer.hostname == address_trimmed)

        # Probe the peer in
        if not already_probed:
            log("Adding {} to cluster".format(address_trimmed))
            probe_result = peer_probe(address_trimmed)
            if probe_result.is_ok():
                log("Gluster peer probe was successful")
            else:
                log("Gluster peer probe failed: {}".format(probe_result.value),
                    ERROR)
                return Err(probe_result.value)
    return Ok(())


def find_new_peers(peers: List[Peer], volume_info: Volume) -> List[Peer]:
    """

    :param peers: 
    :param volume_info: 
    :return: 
    """
    new_peers = []
    for peer in peers:
        # If this peer is already in the volume, skip it
        existing_peer = any(brick.peer.uuid == peer.uuid for brick in
                            volume_info.bricks)
        if not existing_peer:
            new_peers.append(peer)
    return new_peers


def ephemeral_unmount() -> Result:
    """

    :return:
    """
    mountpoint = config("ephemeral_unmount")
    if mountpoint is None:
        return Ok(())
    # Remove the entry from the fstab if it's set
    fstab = FsTab(os.path.join(os.sep, "etc", "fstab"))
    log("Removing ephemeral mount from fstab")
    fstab.remove_entry(mountpoint)

    if filesystem_mounted(mountpoint):
        result = umount(mountpoint=mountpoint)
        if not result:
            return Err("unmount of {} failed".format(mountpoint))
        # Unmounted Ok
        return Ok(())
    # Not mounted
    return Ok(())


def finish_initialization(device_path: os.path) -> Result:
    """

    :param device_path: 
    :return: 
    """
    filesystem_config_value = config("filesystem_type")
    defrag_interval = config("defragmentation_interval")
    disk_elevator = config("disk_elevator")
    scheduler = Scheduler(disk_elevator)
    filesystem_type = FilesystemType(filesystem_config_value)
    mount_path = "/mnt/{}".format(device_path.file_name().unwrap())
    unit_storage = kv()
    device_info = get_device_info(device_path)
    if device_info.is_err():
        return Err(device_info.value)
    log("device_info: {}".format(device_info), INFO)

    # Zfs automatically handles mounting the device
    if filesystem_type != Zfs:
        log("Mounting block device {} at {}".format(device_path, mount_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Mounting block device {} at {}".format(
                       device_path, mount_path))

        if not os.path.exists(mount_path):
            log("Creating mount directory: {}".format(mount_path), INFO)
            os.makedirs(mount_path)

        mount_device(device_info.value, mount_path)
        fstab_entry = FsEntry(
            fs_spec="UUID={}".format(device_info.value.id),
            mountpoint=os.path.join(mount_path),
            vfs_type=device_info.value.fs_type,
            mount_options=["noatime", "inode64"],
            dump=False,
            fsck_order=2)
        log("Adding {} to fstab".format(fstab_entry))
        fstab = FsTab(os.path.join("/etc/fstab"))
        fstab.add_entry(fstab_entry)
    unit_storage.set(device_path, True)
    log("Removing mount path from updatedb {}".format(mount_path), INFO)
    add_to_updatedb_prunepath(mount_path, os.path.join("/etc/updatedb.conf"))
    weekly_defrag(mount_path, filesystem_type, defrag_interval)
    set_elevator(device_path, scheduler)
    return Ok(())


def initialize_storage(device: BrickDevice) -> Result:
    """
    Format and mount block devices to ready them for consumption by Gluster
    Return an Initialization struct

    :param device: 
    :return: 
    """
    filesystem_config_value = config("filesystem_type")
    # Custom params
    stripe_width = config("raid_stripe_width")
    stripe_size = config("raid_stripe_size")
    inode_size = config("inode_size")

    filesystem_type = FilesystemType(filesystem_config_value)
    init = None

    # Format with the default XFS unless told otherwise
    if filesystem_type is Xfs:
        log("Formatting block device with XFS: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with XFS: {}".format(
                       device.dev_path))
        filesystem_type = Xfs(
            block_size=None,
            force=True,
            inode_size=inode_size,
            stripe_size=stripe_size,
            stripe_width=stripe_width,
        )
        init = format_block_device(device, filesystem_type)
    elif filesystem_type is Ext4:
        log("Formatting block device with Ext4: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with Ext4: {}".format(
                       device.dev_path))

        filesystem_type = Ext4(
            inode_size=inode_size,
            reserved_blocks_percentage=0,
            stride=stripe_size,
            stripe_width=stripe_width,
        )
        init = format_block_device(device, filesystem_type)

    elif filesystem_type is Btrfs:
        log("Formatting block device with Btrfs: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with Btrfs: {}".format(
                       device.dev_path))

        filesystem_type = Btrfs(
            leaf_size=0,
            node_size=0,
            metadata_profile=MetadataProfile.Single)
        init = format_block_device(device, filesystem_type)
    elif filesystem_type is Zfs:
        log("Formatting block device with ZFS: {:}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with ZFS: {:}".format(
                       device.dev_path))
        filesystem_type = Zfs(
            compression=None,
            block_size=None,
        )
        init = format_block_device(device, filesystem_type)
    else:
        log("Formatting block device with XFS: {}".format(device.dev_path),
            INFO)
        status_set(workload_state="maintenance",
                   message="Formatting block device with XFS: {}".format(
                       device.dev_path))

        filesystem_type = Xfs(
            block_size=None,
            force=True,
            inode_size=inode_size,
            stripe_width=stripe_width,
            stripe_size=stripe_size)
        init = format_block_device(device, filesystem_type)
    return Ok(init)


"""
def resolve_first_vip_to_dns() -> Result:
    cluster_networks = get_cluster_networks()
    if cluster_networks.is_ok():
    match cluster_networks.first() {
        Some(cluster_network) => {
            match cluster_network.cidr {
                IpNetwork.V4(ref v4_network) => {
                    # Resolve the ipv4 address back to a dns string
                    Ok(address_name(.std.net.IpAddr.V4(v4_network.ip())))
                }
                IpNetwork.V6(ref v6_network) => {
                    # Resolve the ipv6 address back to a dns string
                    Ok(address_name(.std.net.IpAddr.V6(v6_network.ip())))
        None => {
            # No vips were set
            return Err("virtual_ip_addresses has no addresses set")
"""


@when('volume.started')
@when_not('glusterfs.mounted')
def mount_cluster() -> Result:
    """
    Mount the cluster at /mnt/glusterfs using fuse

    :return: Result.  Ok or Err depending on the outcome of mount
    """
    volume_name = config('volume_name')
    if not os.path.exists("/mnt/glusterfs"):
        os.makedirs("/mnt/glusterfs")
    if not filesystem_mounted("/mnt/glusterfs"):
        cmd = ["-t", "glusterfs", "localhost:/{}".format(volume_name),
               "/mnt/glusterfs"]
        output = run_command("mount", cmd, True, False)
        if output.is_ok():
            log("Removing /mnt/glusterfs from updatedb", INFO)
            add_to_updatedb_prunepath("/mnt/glusterfs")
            set_state("glusterfs.mounted")
            return Ok(())
        else:
            return Err(output.value)
    return Ok(())


def get_glusterfs_version() -> Result:
    """
    Get the current glusterfs version that is installed
    :return: Result.  Ok(str) or Err(str)
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


def update_status() -> Result:
    """
    Update the juju status information

    :return: 
    """
    version = get_glusterfs_version()
    application_version_set("{}".format(version))
    volume_name = config("volume_name")

    local_bricks = get_local_bricks(volume_name)
    if local_bricks.is_ok():
        status_set(workload_state="active",
                   message="Unit is ready ({} bricks)".format(
                       len(local_bricks.value)))
        return Ok(())
    else:
        status_set(workload_state="blocked",
                   message="No bricks found")
        return Ok(())
