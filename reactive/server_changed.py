from charmhelpers.core.hookenv import config, ERROR, INFO, is_leader, \
    log, related_units, relation_set, status_set
from result import Err, Ok, Result
from typing import List, Optional

from .block import get_juju_bricks, get_manual_bricks
from lib.gluster.lib import GlusterOption, SplitBrainPolicy, Toggle
from lib.gluster.peer import peer_list, Peer
from lib.gluster.volume import Transport, volume_create_arbiter, \
    volume_create_distributed, volume_create_striped, \
    volume_create_replicated, volume_create_striped_replicated, \
    volume_add_brick, volume_create_erasure, volume_info, Volume, VolumeType, \
    volume_enable_bitrot, volume_start, volume_set_options, volume_remove_brick
from .main import brick_and_server_cartesian_product, finish_initialization, \
    find_new_peers, ephemeral_unmount, initialize_storage, mount_cluster, \
    Status, wait_for_peers


def server_changed() -> Result:
    context = juju.Context.new_from_env()
    volume_name = config["volume_name"]

    if is_leader():
        log("I am the leader: {}".format(context.relation_id))
        log("Loading config", INFO)
        status_set(workload_state="maintenance",
                   message="Checking for new peers to probe")

        peers = peer_list()
        log("peer list: {}".format(peers))
        probe_in_units(peers, related_units())
        # Update our peer list
        peers = peer_list()

        # Everyone is in.  Lets see if a volume exists
        vol_info = volume_info(volume_name)
        existing_volume = False
        if vol_info.is_ok():
            log("Expanding volume {}".format(volume_name), INFO)
            status_set(workload_state="maintenance",
                       message="Expanding volume {}".format(
                           volume_name))
            expand_vol = expand_volume(peers, vol_info.value)
            if expand_vol.is_ok():
                log("Expand volume succeeded.  Return code: {}".format(v), INFO)
                status_set(workload_state="active",
                           message="Expand volume succeeded.")
                # Poke the other peers to update their status
                relation_set("expanded", "True")
                # Ensure the cluster is mounted
                mount_cluster(volume_name)
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
            Err(gluster.GlusterERROR.NoVolumesPresent) = > {
                existing_volume = False
            else:
            return Err("Volume info command failed")
        if not existing_volume:
            log("Creating volume {}".format(volume_name), INFO)
            status_set(workload_state="maintenance",
                       message="Creating volume {}".format(volume_name))
            create_gluster_volume(volume_name, peers)
            mount_cluster(volume_name)
            # setup_ctdb()
            # setup_samba(volume_name)
        return Ok(())
    else:
        # Non leader units
        vol_started = juju.relation_get("started")
        if vol_started is not None:
            mount_cluster(volume_name)
            # Setup ctdb and samba after the volume comes up on non leader units
            # setup_ctdb()
            # setup_samba(volume_name)
        return Ok(())


def create_gluster_volume(volume_name: str, peers: List[Peer]) -> Result:
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
                   message="Create volume failed.  Please check juju debug-log.")
        return Err(create_vol.value)


def create_volume(peers: List[Peer], volume_info: Optional[Volume]) -> Result:
    """
        Create a new volume if enough peers are available
    """
    cluster_type_config = config["cluster_type"]
    cluster_type = VolumeType(cluster_type_config)
    volume_name = config["volume_name"]
    replicas = 3
    try:
        replicas = int(config["replication_level"])
    except ValueError:
        log(format("Invalid integer {} for replicas.  "
                   "Defaulting to 3.".format(config["replication_level"]))

        extra = 1
    try:
        extra = int(config["extra_level"])
    except ValueError:
        log("Invalid integer {} for extra_level.  Defaulting to 1. ".format(
            config["extra_level"]))
        extra = 1
    # Make sure all peers are in the cluster
    # spinlock
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
            # brick_list.len()-1, #TODO: This number has to be lower than the brick length
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
    volume_name = config["volume_name"]
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
            return Err("Unknown error in expand volume: {}".format(e))


# This function will take into account the replication level and
# try its hardest to produce a list of bricks that satisfy this:
# 1. Are not already in the volume
# 2. Sufficient hosts to satisfy replication level
# 3. Stripped across the hosts
# If insufficient hosts exist to satisfy this replication level this will return no new bricks
# to add
def get_brick_list(peers: List[Peer], volume: Optional[Volume]) -> Result:
    # Default to 3 replicas if the parsing fails
    brick_devices = []
    replica_config = config["replication_level"]
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
            # The device is already initialized, lets add it to our usable paths list
            log("{} is already initialized".format(device.dev_path))
            brick_paths.append(device.mount_path)
    # Wait for all children to finish formatting their drives
    for handle in format_handles:
        output_result = handle.format_child.wait_with_output()
        if output_result.is_ok():
            process_result = block.process_output(output_result.value)
            if process_result.is_ok():
                # success
                # 1. Run any post setup commands if needed
                finish_result = finish_initialization(handle.device.dev_path)
                if finish_result.is_err():
                    return Err(Status.InvalidConfig)

                brick_paths.append(handle.device.mount_path)
            else:
                # Failed
                log("Device {} formatting failed with error: {}. "
                    "Skipping".format(
                    handle.device.dev_path, process_result.value), ERROR)
        else:
            # Failed
            log("Device {} formatting failed with error: {}. "
                "Skipping".format(handle.device.dev_path, e), ERROR)
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
            new_peers.truncate(count)
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
            new_peers.truncate(count)
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
    volume_name = config["volume_name"]
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


def start_gluster_volume(volume_name: str) -> Result:
    start_vol_result = volume_start(volume_name, False)
    if start_vol_result.is_ok():
        log("Starting volume succeeded.", INFO)
        status_set(workload_state="active",
                   message="Starting volume succeeded.")

        mount_cluster(volume_name)
        settings = [
            # Starting in gluster 3.8 NFS is disabled in favor of ganesha.
            # I'd like to stick with the legacy version a bit longer.
            GlusterOption.NfsDisable(Toggle.Off),
            GlusterOption.DiagnosticsLatencyMeasurement(Toggle.On),
            GlusterOption.DiagnosticsCountFopHits(Toggle.On),
            # Dump FOP stats every 5 seconds.
            # NOTE: On slow main drives this can severely impact them
            GlusterOption.DiagnosticsFopSampleInterval(5),
            GlusterOption.DiagnosticsStatsDumpInterval(30),
            # 1HR DNS timeout
            GlusterOption.DiagnosticsStatsDnscacheTtlSec(3600),
            # Set parallel-readdir on.  This has a very nice performance
            # benefit as the number of bricks/directories grows
            GlusterOption.PerformanceParallelReadDir(Toggle.On),
            GlusterOption.PerformanceReadDirAhead(Toggle.On),
            # Start with 20MB and go from there
            GlusterOption.PerformanceReadDirAheadCacheLimit(
                1024 * 1024 * 20)]

        # Set the split brain policy if requested
        splitbrain_policy = config["splitbrain_policy"]
        if splitbrain_policy is not None:
            # config.yaml has a default here.  Should always have a value
            try:
                policy = SplitBrainPolicy(splitbrain_policy)
                settings.append(GlusterOption.FavoriteChildPolicy(policy))
            except ValueError:
                log("Failed to parse splitbrain_policy config setting: \
                                        {}.".format(splitbrain_policy), ERROR)
        else:
            volume_set_options(volume_name, settings)

        # The has a default.  Should be safe
        bitrot_config = config["bitrot_detection"]
        bitrot_detection = serde_yaml.from_str(bitrot_config)
        if bitrot_detection:
            log("Enabling bitrot detection")
            status_set(workload_statue="active",
                       message="Enabling bitrot detection.")
            _ = volume_enable_bitrot(volume_name)
        return Ok(())
    else:
        log("Start volume failed with output: {}".format(
            start_vol_result.value), ERROR)
        status_set(workload_state="blocked",
                   message="Start volume failed.  Please check juju debug-log.")
        return Err(start_vol_result.value)
