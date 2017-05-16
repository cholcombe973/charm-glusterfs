from charmhelpers.core import hookenv, sysctl
from charmhelpers.core.host import updatedb
from charmhelpers.core.hookenv import ERROR, INFO, Hooks, log
from charmhelpers.contrib.storage.linux.ceph import filesystem_mounted

from .actions import disable_bitrot_scan, disable_volume_quota, \
    list_volume_quotas, set_bitrot_scan_frequency, set_bitrot_throttle, \
    pause_bitrot_scan, resume_bitrot_scan, \
    set_volume_options, enable_bitrot_scan, enable_volume_quota
from .brick_detached import brick_detached
from .server_changed import server_changed
from .server_removed import server_removed

config = hookenv.config()
from .ctdb import VirtualIp
from enum import Enum
import itertools
import os
from result import Err, Ok, Result
import sys
from typing import List, Optional

"""
#[cfg(test)]
mod tests {
    use std.collections.BTreeMap
    use std.path.PathBuf

    use super.gluster.volume.{Brick, Transport, Volume, VolumeType}
    use super.gluster.peer.{Peer, State}
    use super.uuid.Uuid

    #[test]
    def test_all_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Peers are ready: {}", ready)
        assert!(ready)
    }

    #[test]
    def test_some_peers_are_ready() {
        peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.Connected,
                                    },
                                    Peer {
                                        uuid: Uuid.new_v4(),
                                        hostname: format!("host-{}", Uuid.new_v4()),
                                        status: State.PeerInCluster,
                                    }]
        ready = super.peers_are_ready(Ok(peers))
        println!("Some peers are ready: {}", ready)
        assert_eq!(ready, False)
    }

    #[test]
    def test_find_new_peers() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }

        # peer1 and peer2 are in the cluster but only peer1 is actually serving a brick.
        # find_new_peers should return peer2 as a new peer
        peers: Vec<Peer> = vec![peer1.clone(), peer2.clone()]
        existing_brick = Brick {
            peer: peer1,
            path: PathBuf.from("/mnt/brick1"),
        }

        volume_info = Volume {
            name: "Test".to_string(),
            vol_type: VolumeType.Replicate,
            id: Uuid.new_v4(),
            status: "online".to_string(),
            transport: Transport.Tcp,
            bricks: vec![existing_brick],
            options: BTreeMap.new(),
        }
        new_peers = super.find_new_peers(peers, volume_info)
        assert_eq!(new_peers, vec![peer2])
    }

    #[test]
    def test_cartesian_product() {
        peer1 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peer2 = Peer {
            uuid: Uuid.new_v4(),
            hostname: format!("host-{}", Uuid.new_v4()),
            status: State.PeerInCluster,
        }
        peers = vec![peer1.clone(), peer2.clone()]
        paths = vec!["/mnt/brick1".to_string(), "/mnt/brick2".to_string()]
        result = super.brick_and_server_cartesian_product(peers, paths)
        println!("brick_and_server_cartesian_product: {:}", result)
        assert_eq!(result,
                   vec![Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer1.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf.from("/mnt/brick2"),
                        }])
    }
}
"""

class Status(Enum):
    """
    Need more expressive return values so we can wait on peers
    """
    Created = 0,
    WaitForMorePeers = 1,
    InvalidConfig = 2,
    FailedToCreate = 3,
    FailedToStart = 4,

# Return all the virtual ip networks that will be used
def get_cluster_networks() -> Result: # -> Result<Vec<ctdb.VirtualIp>, str>:
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

def peers_are_ready(peer_list: Result) -> bool:
    if peer_list.is_ok():
        log("Got peer status: {}".format(peer_list.value))
        return all(peer.status == State.PeerInCluster for peer in peer_list.value)
    else:
        log("peers_are_ready failed to get peer status: {}".format(peer_list.value), ERROR)
        return False
    """
    match peers {
        Ok(peer_list) => {
            # Ensure all peers are in a PeerInCluster state
            log("Got peer status: {}".format(peer_list))
            return peer_list.iter().all(|peer| peer.status == State.PeerInCluster)
        }
        Err(err) => {
            log("peers_are_ready failed to get peer status: {}".format(err),
                 ERROR)
            return False
    """

# HDD's are so slow that sometimes the peers take long to join the cluster.
# This will loop and wait for them ie spinlock
def wait_for_peers() -> Result:
    log("Waiting for all peers to enter the Peer in Cluster status")
    status_set(Maintenance, "Waiting for all peers to enter the \"Peer in Cluster status\"")
    iterations = 0
    while not peers_are_ready(peer_status()):
        thread.sleep(Duration.from_secs(1))
        iterations += 1
        if iterations > 600:
            return Err("Gluster peers failed to connect after 10 minutes")
    return Ok(())

# Probe in a unit if they haven't joined yet
# This function is confusing because Gluster has weird behavior.
# 1. If you probe in units by their IP address it works.  The CLI will show you their resolved
# hostnames however
# 2. If you probe in units by their hostname instead it'll still work but gluster client mount
# commands will fail if it can not resolve the hostname.
# For example: Probing in containers by hostname will cause the glusterfs client to fail to mount
# on the container host.  :(
# 3. To get around this I'm converting hostnames to ip addresses in the gluster library to mask
# this from the callers.
#
def probe_in_units(existing_peers: List[Peer], related_units: List[juju.Relation]) -> Result:
    log("Adding in related_units: {}".format(related_units))
    for unit in related_units:
        address = match juju.relation_get_by_unit("private-address", unit)
            .map_err(|e| e.to_string()){
                Some(address) => address,
                None => {
                    log("unit {} private-address was blank, skipping.".format(unit))
                    continue
        address_trimmed = address.strip()
        already_probed = any(peer.hostname == address_trimmed for peer in existing_peers)# .iter().any(|peer| peer.hostname == address_trimmed)

        # Probe the peer in
        if not already_probed:
            log("Adding {} to cluster".format(address_trimmed))
            match peer_probe(address_trimmed) {
                Ok(_) => {
                    log("Gluster peer probe was successful")
                }
                Err(why) => {
                    log("Gluster peer probe failed: {}".format(why), ERROR)
                    return Err(why)
    return Ok(())

def find_new_peers(peers: List[Peer], volume_info: Volume) -> List[Peer]:
    new_peers = []
    for peer in peers:
        # If this peer is already in the volume, skip it
        existing_peer = any(brick.peer.uuid == peer.uuid for brick in volume_info.bricks) # volume_info.bricks.iter().any(|brick| brick.peer.uuid == peer.uuid)
        if not existing_peer:
            new_peers.append(peer)
    return new_peers

def brick_and_server_cartesian_product(peers: List[Peer], paths: List[str]) -> List[gluster.volume.Brick]:
    product = []# : Vec<gluster.volume.Brick> = []
    it = itertools.product(paths, peers)# .iter().cartesian_product(peers.iter())
    for path, host in it:
        brick = gluster.volume.Brick(
            peer=host,
            path=path,
        )
        product.append(brick)
    return product

def ephemeral_unmount() -> Result:
    match get_config_value("ephemeral_unmount") {
        Ok(mountpoint) => {
            if mountpoint.is_empty():
                return Ok(())
            # Remove the entry from the fstab if it's set
            fstab = fstab.FsTab.new(Path.new("/etc/fstab"))
            log("Removing ephemeral mount from fstab")
            fstab.remove_entry(mountpoint).map_err(|e| e.to_string())

            if filesystem_mounted(mountpoint) {
                cmd = std.process.Command.new("umount")
                cmd.arg(mountpoint)
                output = cmd.output().map_err(|e| e.to_string())
                if not output.status.success():
                    return Err(str.from_utf8_lossy(output.stderr).into_owned())
                # Unmounted Ok
                return Ok(())
            # Not mounted
            Ok(())
        _ => {
            # No-op
            Ok(())

# Given a dev device path /dev/xvdb this will check to see if the device
# has been formatted and mounted
def device_initialized(brick_path: PathBuf) -> Result:
    # Connect to the default unitdata database
    log("Connecting to unitdata storage")
    unit_storage = unitdata.Storage.new(None)
    log("Getting unit_info")
    unit_info = unit_storage.get.<bool>(brick_path.to_string_lossy())
    log("{} initialized: {}".format(brick_path,unit_info))
    # Either it's Some() and we know about the unit
    # or it's None and we don't know and therefore it's not initialized
    Ok(unit_info.unwrap_or(False))

def finish_initialization(device_path: PathBuf) -> Result:
    filesystem_config_value = config["filesystem_type"]
    defrag_interval = config["defragmentation_interval"]
    disk_elevator = config["disk_elevator"]
    scheduler =
        block.Scheduler.from_str(disk_elevator).map_err(|e| ERROR.new(ERRORKind.Other, e))
    filesystem_type = block.FilesystemType.from_str(filesystem_config_value)
    mount_path = "/mnt/{}".format(device_path.file_name().unwrap())
    unit_storage = unitdata.Storage.new(None).map_err(|e| ERROR.new(ERRORKind.Other, e))
    device_info =
        block.get_device_info(device_path).map_err(|e| ERROR.new(ERRORKind.Other, e))
    log("device_info: {}".format(device_info), INFO)

    #Zfs automatically handles mounting the device
    if filesystem_type != block.FilesystemType.Zfs {
        log("Mounting block device {} at {}".format(device_path, mount_path), INFO)
        status_set(Maintenance, "Mounting block device {} at {}".format(device_path, mount_path))

        if not os.path.exists(mount_path):
            log(format!("Creating mount directory: {}", mount_path), INFO)
            create_dir(mount_path)

        block.mount_device(device_info, mount_path)
            .map_err(|e| ERROR.new(ERRORKind.Other, e))
        fstab_entry = fstab.FsEntry {
            fs_spec: format!("UUID={}",
                             device_info.id
                                 .unwrap()
                                 .hyphenated()
                                 .to_string()),
            mountpoint: PathBuf.from(mount_path),
            vfs_type: device_info.fs_type.to_string(),
            mount_options: vec!["noatime".to_string(), "inode64".to_string()],
            dump: False,
            fsck_order: 2,
        log("Adding {} to fstab".format(fstab_entry))
        fstab = fstab.FsTab.new(Path.new("/etc/fstab"))
        fstab.add_entry(fstab_entry)
    unit_storage.set(device_path.to_string_lossy(), True)
    log("Removing mount path from updatedb {}".format(mount_path), INFO)
    updatedb.add_to_prunepath(mount_path, Path.new("/etc/updatedb.conf"))
    block.weekly_defrag(mount_path, filesystem_type, defrag_interval)
    block.set_elevator(device_path, scheduler)
    Ok(())
}

# Format and mount block devices to ready them for consumption by Gluster
# Return an Initialization struct
def initialize_storage(device: block.BrickDevice) -> Result:
    filesystem_config_value = config["filesystem_type"]
    #Custom params
    stripe_width = int(config["raid_stripe_width"])
    stripe_size = int(config["raid_stripe_size"])
    inode_size = int(config["inode_size"])

    filesystem_type = block.FilesystemType.from_str(filesystem_config_value)
    init = block.AsyncInit()

    # Format with the default XFS unless told otherwise
    if filesystem_type is block.FilesystemType.Xfs:
            log("Formatting block device with XFS: {}".format(device.dev_path), INFO)
            status_set(Maintenance, "Formatting block device with XFS: {}".format(device.dev_path))
            filesystem_type = block.Filesystem.Xfs(
                block_size=None,
                force=True,
                inode_size=inode_size,
                stripe_size=stripe_size,
                stripe_width=stripe_width,
            )
            init = block.format_block_device(device, filesystem_type)
    elif filesystem_type is block.FilesystemType.Ext4:
        log("Formatting block device with Ext4: {}".format(device.dev_path), INFO)
        status_set(Maintenance, "Formatting block device with Ext4: {}".format(device.dev_path))

        filesystem_type = block.Filesystem.Ext4(
            inode_size=inode_size,
            reserved_blocks_percentage=0,
            stride=stripe_size,
            stripe_width=stripe_width,
        )
        init = block.format_block_device(device, filesystem_type)

    elif filesystem_type is block.FilesystemType.Btrfs:
        log("Formatting block device with Btrfs: {}".format(device.dev_path),INFO)
        status_set(Maintenance, "Formatting block device with Btrfs: {}".format(device.dev_path))

        filesystem_type = block.Filesystem.Btrfs(
            leaf_size=0,
            node_size=0,
            metadata_profile=block.MetadataProfile.Single)
        init = block.format_block_device(device, filesystem_type)
    elif filesystem_type is block.FilesystemType.Zfs:
        log("Formatting block device with ZFS: {:}".format(device.dev_path), INFO)
        status_set(Maintenance, "Formatting block device with ZFS: {:}".format(device.dev_path))
        filesystem_type = block.Filesystem.Zfs(
            compression=None,
            block_size=None,
        )
        init = block.format_block_device(device, filesystem_type)
    else:
        log("Formatting block device with XFS: {}".format(device.dev_path), INFO)
        status_set(Maintenance, "Formatting block device with XFS: {}".format(device.dev_path))

        filesystem_type = block.Filesystem.Xfs(
            block_size=None,
            force=True,
            inode_size=inode_size,
            stripe_width=stripe_width,
            stripe_size=stripe_size)
        init = block.format_block_device(device, filesystem_type)
    return Ok(init)

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

def get_glusterfs_version() -> Result:
    cmd = ["dpkg", "-s", "glusterfs-server"]
    ret_code, output = run_command(cmd)
    if output > 0:
        for line in output:
            if line.startswith("Version"):
                # return the version
                parts = line.split(" ")
                if len(parts) is 2:
                    parse_version = Version.parse(parts[1]).map_err(|e| e.msg)
                    return Ok(parse_version)
                else:
                    return Err("apt-cache Verion string is invalid: {}".format(line))
    else:
        return Err(output)
    return Err("Unable to find glusterfs-server version")

# Mount the cluster at /mnt/glusterfs using fuse
def mount_cluster(volume_name: str) -> Result:
    if not os.path.exists("/mnt/glusterfs"):
        os.makedirs("/mnt/glusterfs")
    if not filesystem_mounted("/mnt/glusterfs"):
        cmd = ["mount", "-t", "glusterfs", "localhost:/{}".format(volume_name),
               "/mnt/glusterfs"]
        ret_code, output = run_command(cmd)
        if ret_code is 0:
            log("Removing /mnt/glusterfs from updatedb", INFO)
            updatedb.add_to_updatedb_prunepath("/mnt/glusterfs")
            return Ok(())
        else:
            return Err(output.stderr)
    return Ok(())

# Update the juju status information
def update_status() -> Result:
    version = get_glusterfs_version()
    juju.application_version_set("{}".format(version.upstream_version))
    volume_name = get_config_value("volume_name")

    local_bricks = gluster.get_local_bricks(volume_name)
    if local_bricks.is_ok():
        status_set(Active, "Unit is ready ({} bricks)".format(len(local_bricks.value)))
        # Ensure the cluster is mounted
        mount_cluster(volume_name)
        return Ok(())
    else:
        status_set(Blocked, "No bricks found")
        return Ok(())

def main():
    # Register our hooks with the Juju library
    hooks = Hooks()
    hooks.register("brick-storage-detaching", brick_detached)
    hooks.register("collect-metrics", collect_metrics)
    hooks.register("config-changed", config_changed)
    hooks.register("create-volume-quota", enable_volume_quota)
    hooks.register("delete-volume-quota", disable_volume_quota)
    hooks.register("disable-bitrot-scan", disable_bitrot_scan)
    hooks.register("enable-bitrot-scan", enable_bitrot_scan)
    hooks.register("fuse-relation-joined", fuse_relation_joined)
    hooks.register("list-volume-quotas", list_volume_quotas)
    hooks.register("nfs-relation-joined", nfs_relation_joined)
    hooks.register("pause-bitrot-scan", pause_bitrot_scan)
    hooks.register("resume-bitrot-scan", resume_bitrot_scan)
    hooks.register("server-relation-changed", server_changed)
    hooks.register("server-relation-departed", server_removed)
    hooks.register("set-bitrot-scan-frequency", set_bitrot_scan_frequency)
    hooks.register("set-bitrot-throttle", set_bitrot_throttle)
    hooks.register("set-volume-options", set_volume_options)
    hooks.register("update-status", update_status)

    update_status()

if __name__ == "__main__":
    main()
    # execute a hook based on the name the program is called by
    hooks.execute(sys.argv)