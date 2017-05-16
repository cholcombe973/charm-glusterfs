from charmhelpers.core.hookenv import is_leader
from result import Err, Ok, Result
from typing import List, Optional

"""
use gluster.{GlusterOption, SplitBrainPolicy, Toggle
use gluster.peer.{peer_list, Peer
use gluster.volume.*
use super.super.apt
use super.super.block
use super.super.ctdb
use super.super.samba.setup_samba
use super.super.{brick_and_server_cartesian_product, ephemeral_unmount, find_new_peers,
                   finish_initialization, get_cluster_networks, get_config_value,
                   initialize_storage, mount_cluster, probe_in_units, Status, wait_for_peers
"""

def server_changed() -> Result:
    context = juju.Context.new_from_env()
    volume_name = get_config_value("volume_name")

    if is_leader():
        log("I am the leader: {}".format(context.relation_id))
        log("Loading config", INFO)

        f = File.open("config.yaml").map_err(|e| e)
        s = str.new()
        f.read_to_string(s).map_err(|e| e)

        status_set(Maintenance, "Checking for new peers to probe")

        peers = peer_list()
        log("peer list: {}".format(peers))
        related_units = juju.relation_list()
        probe_in_units(peers, related_units)
        # Update our peer list
        peers = peer_list()

        # Everyone is in.  Lets see if a volume exists
        volume_info = volume_info(volume_name)
        existing_volume: bool
        if volume_info.is_ok():
            log("Expanding volume {", volume_name), INFO)
            status_set(Maintenance, "Expanding volume {}".format(volume_name))
            expand_vol = expand_volume(peers, volume_info.value())
            if expand_vol.is_ok():
                log("Expand volume succeeded.  Return code: {}".format(v), INFO)
                status_set(Active, "Expand volume succeeded.")
                # Poke the other peers to update their status
                juju.relation_set("expanded", "True")
                # Ensure the cluster is mounted
                mount_cluster(volume_name)
                setup_ctdb()
                setup_samba(volume_name)
                return Ok(())
            else:
                log("Expand volume failed with output: {}".format(expand_vol.value), Error)
                status_set(Blocked, "Expand volume failed.  Please check juju debug-log.")
                return Err(expand_vol.value)
            Err(gluster.GlusterError.NoVolumesPresent) => {
                existing_volume = False
        else:
            return Err("Volume info command failed")
        if not existing_volume:
            log("Creating volume {}".format(volume_name), INFO)
            status_set(Maintenance, "Creating volume {}".format(volume_name))
            create_gluster_volume(volume_name, peers)
            mount_cluster(volume_name)
            setup_ctdb()
            setup_samba(volume_name)
        return Ok(())
     else:
        # Non leader units
        vol_started = juju.relation_get("started")
        if vol_started.is_some():
            mount_cluster(volume_name)
            # Setup ctdb and samba after the volume comes up on non leader units
            setup_ctdb()
            setup_samba(volume_name)
        return Ok(())


def create_gluster_volume(volume_name: str, peers: List[Peer]) -> Result:
    create_vol = create_volume(peers, None)
    if create_vol.is_ok():
        if create_vol.value == Status.Created:
            log("Create volume succeeded.", INFO)
            status_set(Maintenance "Create volume succeeded")
            start_gluster_volume(volume_name)
            # Poke the other peers to update their status
            juju.relation_set("started", "True").map_err(|e| e)
            return Ok(())
        elif create_vol.value == Status.WaitForMorePeers:
            log("Waiting for all peers to enter the Peer in Cluster status")
            status_set(Maintenance, "Waiting for all peers to enter the \"Peer in Cluster status\"")
            return Ok(())
        else:
            # Status is failed
            # What should I return here
            return Ok(())
    else:
        log("Create volume failed with output: {}".format(create_vol.value), Error)
        status_set(Blocked, "Create volume failed.  Please check juju debug-log.")
        return Err(create_vol.value)

def create_volume(peers: List[Peer], volume_info: Optional[Volume]) -> Result<Status, str> {
    """
        Create a new volume if enough peers are available
    """
    cluster_type_config = get_config_value("cluster_type")
    cluster_type = VolumeType.from_str(cluster_type_config)
    volume_name = get_config_value("volume_name")
    replicas = match get_config_value("replication_level").parse() {
        Ok(r) => r,
        Err(e) => {
            log(format("Invalid config value for replicas.  Defaulting to 3. Error was \
                                {",
                         e),
                 Error)
            3
    extra = match get_config_value("extra_level").parse() {
        Ok(r) => r,
        Err(e) => {
            log("Invalid integer for extra_level.  Defaulting to 1. Error was {}".format(e),Error)
            1
    # Make sure all peers are in the cluster
    # spinlock
    wait_for_peers()

    # Build the brick list
    brick_list = match get_brick_list(peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status.WaitForMorePeers => {
                    log("Waiting for more peers", INFO)
                    status_set(Maintenance "Waiting for more peers")
                    return Ok(Status.WaitForMorePeers)

                Status.InvalidConfig(config_err) => {
                    return Err(config_err)

                _ => {
                    # Some other error
                    return Err("Unknown error in create volume: {}".format(e))

    log("Got brick list: {}".format(brick_list))
    log("Creating volume of type {} with brick list {}".format(cluster_type,brick_list),INFO)

    if cluster_type is Volume.Distribute:
        result = volume_create_distributed(volume_name, Transport.Tcp, brick_list, True)
        Ok(Status.Created)
    elif cluster_type is Volume.Stripe:
        result = volume_create_striped(volume_name, replicas, Transport.Tcp, brick_list, True)
        Ok(Status.Created)
    elif cluster_type is Volume.Replicate:
        result =
            volume_create_replicated(volume_name, replicas, Transport.Tcp, brick_list, True)
        Ok(Status.Created)
    elif cluster_type is Volume.Arbiter:
        result= = volume_create_arbiter(volume_name,
                                      replicas,
                                      extra,
                                      Transport.Tcp,
                                      brick_list,
                                      True)
        Ok(Status.Created)
    elif cluster_type is Volume.StripedAndReplicate:
        result = volume_create_striped_replicated(volume_name,
                                                 extra,
                                                 replicas,
                                                 Transport.Tcp,
                                                 brick_list,
                                                 True)
        Ok(Status.Created)
    elif cluster_type is Volume.Disperse:
        result = volume_create_erasure(volume_name,
                                      replicas,
                                      extra,
                                      Transport.Tcp,
                                      brick_list,
                                      True)
        Ok(Status.Created)
    elif cluster_type is Volume.DistributedAndStripe:
        result = volume_create_striped(volume_name, replicas, Transport.Tcp, brick_list, True)
        Ok(Status.Created)
    elif cluster_type is Volume.DistributedAndReplicate:
        result = volume_create_replicated(volume_name, replicas, Transport.Tcp, brick_list, True)
        Ok(Status.Created)
    elif cluster_type is Volume.DistributedAndStripedAndReplicate:
        result = volume_create_striped_replicated(volume_name,
                                                 extra,
                                                 replicas,
                                                 Transport.Tcp,
                                                 brick_list,
                                                 True)
        Ok(Status.Created)
    elif cluster_type is Volume.DistributedAndDisperse:
        result = volume_create_erasure(
            volume_name,
            #brick_list.len()-1, #TODO: This number has to be lower than the brick length
            replicas,
            extra,
            Transport.Tcp,
            brick_list,
            True)
        Ok(Status.Created)

def expand_volume(peers: List[Peer], volume_info: Option[Volume]) -> Result:#<i32, str> {
    # Expands the volume by X servers+bricks
    # Adds bricks and then runs a rebalance
    volume_name = get_config_value("volume_name")
    # Are there new peers
    log("Checking for new peers to expand the volume named {}".format(volume_name))
    # Build the brick list
    brick_list = get_brick_list(peers, volume_info)
    if brick_list.is_ok():
        log("Expanding volume with brick list: {}".format(brick_list.value), INFO)
        return volume_add_brick(volume_name, brick_list.value, True)
    else:
        Ok(list) => list,
        Err(e) => {
            match e {
                Status.WaitForMorePeers => {
                    log("Waiting for more peers", INFO)
                    return Ok(0)

                Status.InvalidConfig(config_err) => {
                    return Err(config_err)

                _ => {
                    # Some other error
                    return Err(format("Unknown error in expand volume: {:", e))

# This function will take into account the replication level and
# try its hardest to produce a list of bricks that satisfy this:
# 1. Are not already in the volume
# 2. Sufficient hosts to satisfy replication level
# 3. Stripped across the hosts
# If insufficient hosts exist to satisfy this replication level this will return no new bricks
# to add
def get_brick_list(peers: Vec<Peer>,
                  volume: Option<Volume>)
                  -> Result<Vec<gluster.volume.Brick>, Status>:
    # Default to 3 replicas if the parsing fails
    brick_devices: Vec<block.BrickDevice> = []

    replica_config = get_config_value("replication_level").unwrap_or("3")
    replicas = replica_config.parse().unwrap_or(3)

    # TODO: Should this fail the hook or just keep going
    log("Checking for ephemeral unmount")
    ephemeral_unmount().map_err(|e| Status.InvalidConfig(e))

    # Get user configured storage devices
    manual_brick_devices = block.get_manual_bricks().map_err(|e| Status.InvalidConfig(e))
    brick_devices.extend(manual_brick_devices)

    # Get the juju storage block devices
    juju_config_brick_devices = block.get_juju_bricks().map_err(|e| Status.InvalidConfig(e))
    brick_devices.extend(juju_config_brick_devices)

    log("storage devices: {}".format(brick_devices))

    format_handles: Vec<block.AsyncInit> = []
    brick_paths = []
    # Format all drives in parallel
    for device in brick_devices:
        if not device.initialized:
            log("Calling initialize_storage for {}".format(device.dev_path))
            # Spawn all format commands in the background
            format_handles.append(
                initialize_storage(device).map_err(|e| Status.FailedToCreate(e)))
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
                    finish_initialization(handle.device.dev_path)
                        .map_err(|e| Status.FailedToCreate(e))
                    brick_paths.append(handle.device.mount_path)
                else:
                    # Failed
                    log("Device {} formatting failed with error: {}. Skipping".format(handle.device.dev_path,e), Error)
        else:
            #Failed
            log("Device {: formatting failed with error: {}. Skipping".format(handle.device.dev_path,e),Error)
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
         else {
            # Case 2: We have a mismatch of replicas and hosts
            # Take as many as we can and leave the rest for a later time
            count = peers.len() - (peers.len() % replicas)
            new_peers = peers

            # Drop these peers off the end of the list
            new_peers.truncate(count)
            log(format("Too many new peers.  Dropping { peers off the list", count))
            return Ok(brick_and_server_cartesian_product(new_peers, brick_paths))

     else:
        # Existing volume.  Build a differential list.
        log("Existing volume.  Building differential brick list")
        new_peers = find_new_peers(peers, volume.unwrap())

        if new_peers.len() < replicas:
            log("New peers found are less than needed by the replica count")
            return Err(Status.WaitForMorePeers)
        elif new_peers.len() == replicas:
            log("New peers and number of replicas match")
            return Ok(brick_and_server_cartesian_product(new_peers, brick_paths))
         else:
            count = new_peers.len() - (new_peers.len() % replicas)
            # Drop these peers off the end of the list
            log("Too many new peers.  Dropping {} peers off the list".format(count))
            new_peers.truncate(count)
            return Ok(brick_and_server_cartesian_product(new_peers, brick_paths))

# Add all the peers in the gluster cluster to the ctdb cluster
def setup_ctdb() -> Result:
    if config_get["virtual_ip_addresses"] is None():
        # virtual_ip_addresses isn't set.  Skip setting ctdb up
        return Ok(())

    log("setting up ctdb")
    peers = peer_list().map_err(|e| e)
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


def shrink_volume(peer: Peer, volume_info: Option<Volume>):
    volume_name = get_config_value("volume_name")
    log("Shrinking volume named  {}".format(volume_name), INFO)
    peers = [peer]

    # Build the brick list
    brick_list = get_brick_list(peers, volume_info)
    if brick_list.is_ok():
        log("Shrinking volume with brick list: {}".format(brick_list), INFO)
        return gluster.volume.volume_remove_brick(volume_name, brick_list.value, True)
    else:
        if brick_list.value == Status.WaitForMorePeers:
            log("Waiting for more peers", INFO)
            return Ok(0)
        elif brick_list.value == Status.InvalidConfig:
            return Err(brick_list.value)
        else:
            # Some other error
            return Err("Unknown error in shrink volume: {}".format(brick_list.value))

def start_gluster_volume(volume_name: str) -> Result:
    start_vol_result  = gluster.volume.volume_start(volume_name, False)
    if start_vol_result.is_ok():
            log("Starting volume succeeded.", INFO)
            status_set(Active, "Starting volume succeeded.")

            mount_cluster(volume_name)
            settings = []
            # Starting in gluster 3.8 NFS is disabled in favor of ganesha.  I'd like to stick
            # with the legacy version a bit longer.
            settings.append(GlusterOption.NfsDisable(Toggle.Off))
            settings.append(GlusterOption.DiagnosticsLatencyMeasurement(Toggle.On))
            settings.append(GlusterOption.DiagnosticsCountFopHits(Toggle.On))
            settings.append(GlusterOption.DiagnosticsFopSampleInterval(5))
            # Dump FOP stats every 5 seconds.
            # NOTE: On slow main drives this can severely impact them
            settings.append(GlusterOption.DiagnosticsStatsDumpInterval(30))
            # 1HR DNS timeout
            settings.append(GlusterOption.DiagnosticsStatsDnscacheTtlSec(3600))

            # Set parallel-readdir on.  This has a very nice performance benefit
            # as the number of bricks/directories grows
            settings.append(GlusterOption.PerformanceParallelReadDir(Toggle.On))

            settings.append(GlusterOption.PerformanceReadDirAhead(Toggle.On))
            # Start with 20MB and go from there
            settings.append(GlusterOption.PerformanceReadDirAheadCacheLimit(1024 * 1024 * 20))

            # Set the split brain policy if requested
            if Ok(splitbrain_policy) = juju.config_get("splitbrain_policy"):
                # config.yaml has a default here.  Should always have a value
                split_policy = splitbrain_policy.unwrap()
                match SplitBrainPolicy.from_str(split_policy) {
                    Ok(policy) => {
                        settings.append(GlusterOption.FavoriteChildPolicy(policy))

                    Err(_) => {
                        log("Failed to parse splitbrain_policy config setting: \
                                            {}.".format(split_policy),
                             Error)
            _ = volume_set_options(volume_name, settings).map_err(|e| e)

            # The has a default.  Should be safe
            bitrot_config = juju.config_get("bitrot_detection").unwrap().unwrap()
            bitrot_detection: bool =
                serde_yaml.from_str(bitrot_config).map_err(|e| e)
            if bitrot_detection {
                log("Enabling bitrot detection")
                status_set(Active "Enabling bitrot detection.")
                _ = volume_enable_bitrot(volume_name).map_err(|e| e)
            return Ok(())
    else:
        log("Start volume failed with output: {}".format(start_vol_result.value), Error)
        status_set(Blocked, "Start volume failed.  Please check juju debug-log.")
        return Err(start_vol_result.value)



