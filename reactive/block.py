import os
import re
import subprocess
import typing
import uuid
from enum import Enum
from typing import List, Optional, Tuple

import pyudev
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import log, storage_get, storage_list
from charmhelpers.core.unitdata import kv
from charmhelpers.fetch import apt_install
from pyudev import Context
from result import Err, Ok, Result

from lib.gluster.lib import run_command
from .shellscript import parse

config = hookenv.config


class FilesystemType(Enum):
    Btrfs = "btrfs",
    Ext2 = "ext2",
    Ext3 = "ext3",
    Ext4 = "ext4",
    Xfs = "xfs",
    Zfs = "zfs",
    Unknown = "unknown",

    @staticmethod
    def from_str(s):
        if s == "btrfs":
            return FilesystemType.Btrfs
        elif s == "ext2":
            return FilesystemType.Ext2
        elif s == "ext3":
            return FilesystemType.Ext3
        elif s == "ext4":
            return FilesystemType.Ext4
        elif s == "xfs":
            return FilesystemType.Xfs
        elif s == "zfs":
            return FilesystemType.Zfs
        else:
            return FilesystemType.Unknown

    def __str__(self):
        return "{}".format(self.value)


# Formats a block device at Path p with XFS
class MetadataProfile(Enum):
    Raid0 = "raid0",
    Raid1 = "raid1",
    Raid5 = "raid5",
    Raid6 = "raid6",
    Raid10 = "raid10",
    Single = "single",
    Dup = "dup",

    def __str__(self):
        return "{}".format(self.value)


class MediaType(Enum):
    SolidState = 0,
    Rotational = 1,
    Loopback = 2,
    Unknown = 3,


class Device:
    def __init__(self, id: Optional[uuid.UUID], name: str,
                 media_type: MediaType,
                 capacity: int, fs_type: FilesystemType):
        """
        This will be used to make intelligent decisions about setting up
        the device

        :param id:
        :param name:
        :param media_type:
        :param capacity:
        :param fs_type:
        """
        self.id = id
        self.name = name
        self.media_type = media_type
        self.capacity = capacity
        self.fs_type = fs_type


class BrickDevice:
    def __init__(self, is_block_device: bool, initialized: bool,
                 mount_path: str, dev_path: os.path):
        """

        :param is_block_device: 
        :param initialized: 
        :param mount_path: 
        :param dev_path: 
        """
        self.is_block_device = is_block_device
        self.initialized = initialized
        self.mount_path = mount_path
        self.dev_path = dev_path

    def __eq__(self, other):
        return self.is_block_device == other.is_block_device \
               and self.initialized == other.initialized \
               and self.mount_path == other.mount_path \
               and self.dev_path == other.dev_path

    def __str__(self):
        return "is block device: {} initialized: {} " \
               "mount path : {} dev path: {}".format(self.is_block_device,
                                                     self.initialized,
                                                     self.mount_path,
                                                     self.dev_path)


class AsyncInit:
    def __init__(self, format_child: subprocess.Popen,
                 post_setup_commands: List[Tuple[str, List[str]]],
                 device: BrickDevice):
        """
        The child process needed for this device initialization
        This will be an async spawned Popen handle

        :param format_child: 
        :param post_setup_commands:  After formatting is complete run these 
            commands to setup the filesystem ZFS needs this.  
            These should prob be run in sync mode
        :param device: # The device we're initializing
        """
        self.format_child = format_child
        self.post_setup_commands = post_setup_commands
        self.device = device


class Scheduler(Enum):
    # Try to balance latency and throughput
    Cfq = "cfq",
    # Latency is most important
    Deadline = "deadline",
    # Throughput is most important
    Noop = "noop",

    def __str__(self):
        return "{}".format(self.value)

    @staticmethod
    def from_str(s):
        if s == "cfq":
            return Scheduler.Cfq
        elif s == "deadline":
            return Scheduler.Deadline
        elif s == "noop":
            return Scheduler.Noop


class Filesystem:
    def __init__(self):
        pass


class Btrfs(Filesystem):
    def __init__(self, metadata_profile: MetadataProfile, leaf_size: int,
                 node_size: int):
        """

        :param metadata_profile: 
        :param leaf_size: 
        :param node_size: 
        """
        super().__init__()
        self.metadata_profile = metadata_profile
        self.leaf_size = leaf_size
        self.node_size = node_size


class Ext4(Filesystem):
    def __init__(self, inode_size: Optional[int],
                 reserved_blocks_percentage: int, stride: Optional[int],
                 stripe_width: Optional[int]):
        """

        :param inode_size: 
        :param reserved_blocks_percentage: 
        :param stride: 
        :param stripe_width: 
        """
        super().__init__()
        if inode_size is None:
            self.inode_size = 512
        else:
            self.inode_size = inode_size
        if not reserved_blocks_percentage:
            self.reserved_blocks_percentage = 0
        else:
            self.reserved_blocks_percentage = reserved_blocks_percentage
        self.stride = stride
        self.stripe_width = stripe_width


class Xfs(Filesystem):
    # This is optional.  Boost knobs are on by default:
    # http:#xfs.org/index.php/XFS_FAQ#Q:
    # _I_want_to_tune_my_XFS_filesystems_for_.3Csomething.3E
    def __init__(self, block_size: Optional[int], inode_size: Optional[int],
                 stripe_size: Optional[int], stripe_width: Optional[int],
                 force: bool):
        """

        :param block_size: 
        :param inode_size: 
        :param stripe_size: 
        :param stripe_width: 
        :param force: 
        """
        super().__init__()
        self.block_size = block_size
        if inode_size is None:
            self.inode_size = 512
        else:
            self.inode_size = inode_size
        self.stripe_size = stripe_size
        self.stripe_width = stripe_width
        self.force = force


class Zfs(Filesystem):
    # / The default blocksize for volumes is 8 Kbytes. Any
    # / power of 2 from 512 bytes to 128 Kbytes is valid.
    def __init__(self, block_size: Optional[int], compression: Optional[bool]):
        """

        :param block_size: 
        :param compression: 
        """
        super().__init__()
        self.block_size = block_size
        # / Enable compression on the volume. Default is False
        self.compression = compression


# This assumes the device is formatted at this point
def mount_device(device: Device, mount_point: str) -> Result:
    """

    :param device: 
    :param mount_point: 
    :return: 
    """
    arg_list = []
    if device.id:
        arg_list.append("-U")
        arg_list.append(device.id)
    else:
        arg_list.append("/dev/{}".format(device.name))

    arg_list.append(mount_point)
    return run_command("mount", arg_list, True, False)


def power_of_2(number: int) -> bool:
    return ((number - 1) & number == 0) and not number == 0


def next_power_of_two(x: int) -> int:
    return 2 ** (x - 1).bit_length()


def format_block_device(brick_device: BrickDevice,
                        filesystem: Filesystem) -> AsyncInit:
    """

    :param brick_device: 
    :param filesystem: 
    :return: 
    """
    device = brick_device.dev_path
    if type(filesystem) is Btrfs:
        filesystem = typing.cast(Btrfs, filesystem)
        arg_list = ["mkfs.btrfs", "-m", filesystem.metadata_profile,
                    "-l", filesystem.leaf_size, "-n", filesystem.node_size,
                    device]
        # Check if mkfs.btrfs is installed
        if not os.path.exists("/sbin/mkfs.btrfs"):
            log("Installing btrfs utils")
            apt_install(["btrfs-tools"])

        return Ok(AsyncInit(format_child=subprocess.Popen(arg_list),
                            post_setup_commands=[],
                            device=brick_device))
    elif type(filesystem) is Xfs:
        filesystem = typing.cast(Xfs, filesystem)
        arg_list = ["/sbin/mkfs.xfs"]
        if filesystem.inode_size is not None:
            arg_list.append("-i")
            arg_list.append("size{}=".format(filesystem.inode_size))

        if filesystem.force:
            arg_list.append("-f")

        if filesystem.block_size is not None:
            block_size = filesystem.block_size
            if not power_of_2(block_size):
                log("block_size {} is not a power of two. Rounding up to "
                    "nearest power of 2".format(block_size))
                block_size = next_power_of_two(block_size)

            arg_list.append("-b")
            arg_list.append("size={}".format(filesystem.block_size))

        if filesystem.stripe_size is not None and filesystem.stripe_width \
                is not None:
            arg_list.append("-d")
            arg_list.append("su={}".format(filesystem.stripe_size))
            arg_list.append("sw={}".format(filesystem.stripe_width))
        arg_list.append(device)

        # Check if mkfs.xfs is installed
        if not os.path.exists("/sbin/mkfs.xfs"):
            log("Installing xfs utils")
            apt_install(["xfsprogs"])

        format_handle = subprocess.Popen(arg_list)
        return Ok(AsyncInit(format_child=format_handle,
                            post_setup_commands=[],
                            device=brick_device))

    elif type(filesystem) is Zfs:
        filesystem = typing.cast(Zfs, filesystem)
        # Check if zfs is installed
        if not os.path.exists("/sbin/zfs"):
            log("Installing zfs utils")
            apt_install(["zfsutils-linux"])

        base_name = device.basename()
        # Mount at /mnt/dev_name
        post_setup_commands = []
        arg_list = ["/sbin/zpool", "create", "-f", "-m",
                    "/mnt/{}".format(base_name),
                    base_name, device]
        zpool_create = subprocess.Popen(arg_list)

        if filesystem.block_size is not None:
            # If zpool creation is successful then we set these
            block_size = filesystem.block_size
            log("block_size {} is not a power of two. Rounding up to nearest "
                "power of 2".format(block_size))
            block_size = next_power_of_two(block_size)
            post_setup_commands.append(("/sbin/zfs",
                                        ["set",
                                         "recordsize={}".format(block_size),
                                         base_name]))
        if filesystem.compression is not None:
            post_setup_commands.append(("/sbin/zfs", ["set", "compression=on",
                                                      base_name]))

        post_setup_commands.append(("/sbin/zfs", ["set", "acltype=posixacl",
                                                  base_name]))
        post_setup_commands.append(
            ("/sbin/zfs", ["set", "atime=off", base_name]))
        return Ok(AsyncInit(format_child=zpool_create,
                            post_setup_commands=post_setup_commands,
                            device=brick_device))

    elif type(filesystem) is Ext4:
        filesystem = typing.cast(Ext4, filesystem)
        arg_list = ["mkfs.ext4", "-m", filesystem.reserved_blocks_percentage]
        if filesystem.inode_size is not None:
            arg_list.append("-I")
            arg_list.append(filesystem.inode_size)

        if filesystem.stride is not None:
            arg_list.append("-E")
            arg_list.append("stride={}".format(filesystem.stride))

        if filesystem.stripe_width is not None:
            arg_list.append("-E")
            arg_list.append("stripe_width={}".format(filesystem.stripe_width))

        arg_list.append(device)

        return Ok(AsyncInit(format_child=subprocess.Popen(arg_list),
                            post_setup_commands=[],
                            device=brick_device))


def get_size(device: pyudev.Device) -> Optional[int]:
    """

    :param device: 
    :return: 
    """
    size = device.attributes.get('size')
    if size is not None:
        return int(size) * 512
    return None


def get_uuid(device: pyudev.Device) -> Optional[uuid.UUID]:
    """

    :param device: 
    :return: 
    """
    uuid_str = device.properties.get("ID_FS_UUID")
    if uuid_str is not None:
        return uuid.UUID(uuid_str)
    return None


def get_fs_type(device: pyudev.Device) -> Optional[FilesystemType]:
    """

    :param device: 
    :return: 
    """
    fs_type_str = device.properties.get("ID_FS_TYPE")
    if fs_type_str is not None:
        return FilesystemType.from_str(fs_type_str)
    return None


def get_media_type(device: pyudev.Device) -> MediaType:
    """

    :param device: 
    :return: 
    """
    device_sysname = device.sys_name
    loop_regex = re.compile(r"loop\d+")

    if loop_regex.match(device_sysname):
        return MediaType.Loopback

    rotation_rate = device.properties.get("ID_ATA_ROTATION_RATE_RPM")
    if rotation_rate is None:
        return MediaType.Unknown
    elif int(rotation_rate) is 0:
        return MediaType.SolidState
    else:
        return MediaType.Rotational


def is_block_device(device_path: str) -> Result:
    """

    :param device_path: 
    :return: 
    """
    context = Context()
    sysname = os.path.basename(device_path)
    for device in context.list_devices(subsystem='block'):
        if device.sys_name == sysname:
            return Ok(True)
    return Err("Unable to find device with name {}".format(device_path))


# Tries to figure out what type of device this is
def get_device_info(device_path: os.path) -> Result:  # <Device, str>
    """

    :param device_path: 
    :return: 
    """
    context = Context()
    sysname = device_path.basename()

    for device in context.list_devices(subsystem='block'):
        if sysname == device.sys_name:
            # Ok we're a block device
            device_id = get_uuid(device)
            media_type = get_media_type(device)
            capacity = get_size(device)
            if capacity is None:
                capacity = 0
            fs_type = get_fs_type(device)
            return Ok(Device(id=device_id, name=sysname,
                             media_type=media_type, capacity=capacity,
                             fs_type=fs_type))
    return Err("Unable to find device with name {}".format(device_path))


# Given a dev device path /dev/xvdb this will check to see if the device
# has been formatted and mounted
def device_initialized(brick_path: os.path) -> Result:
    """
    Connect to the default unitdata database

    :param brick_path:
    """
    log("Connecting to unitdata storage")
    unit_storage = kv()
    log("Getting unit_info")
    unit_info = unit_storage.get(brick_path)
    log("{} initialized: {}".format(brick_path, unit_info))
    # Either it's Some() and we know about the unit
    # or it's None and we don't know and therefore it's not initialized
    return Ok(unit_info.unwrap_or(False))


def scan_devices(devices: List[str]) -> Result:
    """

    :param devices: 
    :return: 
    """
    brick_devices = []
    for brick in devices:
        device_path = os.path.join(brick)
        # Translate to mount location
        brick_filename = os.path.basename(device_path)
        log("Checking if {} is a block device".format(device_path))
        block_device = is_block_device(device_path)
        if block_device.is_err():
            log("Skipping invalid block device: {}".format(device_path))
            continue
        log("Checking if {} is initialized".format(device_path))
        initialized = False
        is_initialized = device_initialized(device_path)
        if is_initialized.is_ok():
            initialized = True
        mount_path = os.path.join(os.sep, "mnt", brick_filename)
        # All devices start at initialized is False
        brick_devices.append(BrickDevice(
            is_block_device=block_device.value,
            initialized=initialized,
            dev_path=device_path,
            mount_path=mount_path))
    return Ok(brick_devices)


def set_elevator(device_path: os.path,
                 elevator: Scheduler) -> Result:
    """

    :param device_path: 
    :param elevator: 
    :return: 
    """
    log("Setting io scheduler for {} to {}".format(device_path, elevator))
    device_name = device_path.basename()
    f = open("/etc/rc.local", "r")
    elevator_cmd = "echo {scheduler} > /sys/block/{device}/queue/" \
                   "scheduler".format(scheduler=elevator, device=device_name)

    script = parse(f)
    if script.is_ok():
        for line in script.value.commands:
            if device_name in line:
                line = elevator_cmd
    f = open("/etc/rc.local", "w", encoding="utf-8")
    bytes_written = script.value.write(f)
    if bytes_written.is_ok():
        return Ok(bytes_written.value)
    else:
        return Err(bytes_written.value)


def weekly_defrag(mount: str, fs_type: FilesystemType, interval: str) -> Result:
    """

    :param mount: 
    :param fs_type: 
    :param interval: 
    :return: 
    """
    log("Scheduling weekly defrag for {}".format(mount))
    crontab = os.path.join(os.sep, "var", "spool", "cron", "crontabs", "root")
    defrag_command = ""
    if fs_type is FilesystemType.Ext4:
        defrag_command = "e4defrag"
    elif fs_type is FilesystemType.Btrfs:
        defrag_command = "btrfs filesystem defragment -r"
    elif fs_type is FilesystemType.Xfs:
        defrag_command = "xfs_fsr"

    job = "{interval} {cmd} {path}".format(
        interval=interval,
        cmd=defrag_command,
        path=mount)

    existing_crontab = []
    if os.path.exists(crontab):
        with open(crontab, 'r') as f:
            buff = f.readlines()
            filtered = filter(None, buff[0].split("\n"))
            existing_crontab = filtered

    existing_job_position = [i for i, x in enumerate(existing_crontab) if
                             mount in x]
    # If we found an existing job we remove the old and insert the new job
    if existing_job_position[0] is not None:
        existing_crontab.remove(existing_job_position[0])

    existing_crontab.append(job)

    # Write back out
    with open(crontab, 'w') as f:
        written_bytes = f.write("\n".join(existing_crontab))
        written_bytes += f.write("\n")
        return Ok(written_bytes)


def get_manual_bricks() -> Result:
    """

    :return: 
    """
    log("Gathering list of manually specified brick devices")
    brick_list = []
    manual_config_brick_devices = config["brick_devices"]
    for brick in manual_config_brick_devices:
        brick_parts = brick.split(" ")
        if brick_parts is not None:
            brick_list.append(brick_parts)
    log("List of manual storage brick devices: {}".format(
        manual_config_brick_devices))
    bricks = scan_devices(manual_config_brick_devices)
    if bricks.is_err():
        return Err(bricks.value)
    return Ok(bricks.value)


def get_juju_bricks() -> Result:
    """

    :return: 
    """
    log("Gathering list of juju storage brick devices")
    # Get juju storage devices
    brick_list = []
    juju_config_brick_devices = storage_list()
    for brick in juju_config_brick_devices:
        if brick is None:
            continue
        s = storage_get("location", brick)
        if s is not None:
            brick_list.append(s.strip())

    log("List of juju storage brick devices: {}".format(
        juju_config_brick_devices))
    bricks = scan_devices(juju_config_brick_devices)
    if bricks.is_err():
        return Err(bricks.value)
    return Ok(bricks.value)
