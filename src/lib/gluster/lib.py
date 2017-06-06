# This is a library to interface with
# [Gluster](https:#gluster.readthedocs.org/en/latest/)
#
# Most of the commands below are wrappers around the CLI XML functionality.
#
# Scale testing with this library has been done to about 60 servers
# successfully.
#
# Please file any bugs found at: [Gluster
# Repo](https:#github.com/cholcombe973/Gluster)
# Pull requests are more than welcome!

from enum import Enum
from ipaddress import ip_address
import re
from result import Err, Ok, Result
from typing import List

import subprocess


class SelfHealAlgorithm(Enum):
    Full = "full"
    Diff = "diff"
    Reset = "reset"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        if s == "full":
            return SelfHealAlgorithm.Full
        elif s == "diff":
            return SelfHealAlgorithm.Diff
        elif s == "reset":
            return SelfHealAlgorithm.Reset
        else:
            return None


class SplitBrainPolicy(Enum):
    Ctime = "ctime"
    Disable = "none"
    Majority = "majority"
    Mtime = "mtime"
    Size = "size"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        if s == "ctime":
            return SplitBrainPolicy.Ctime
        elif s == "none":
            return SplitBrainPolicy.Disable
        elif s == "majority":
            return SplitBrainPolicy.Majority
        elif s == "mtime":
            return SplitBrainPolicy.Mtime
        elif s == "size":
            return SplitBrainPolicy.Size,
        else:
            return None


class AccessMode(Enum):
    ReadOnly = "read-only"
    ReadWrite = "read-write"

    def __str__(self):
        return self.value

    @staticmethod
    def from_str(s):
        if s == "read-only":
            return AccessMode.ReadOnly
        elif s == "read-write":
            return AccessMode.ReadWrite
        else:
            return None


class Toggle(Enum):
    On = True
    Off = False

    def __str__(self):
        if self.value:
            return "On"
        else:
            return "Off"

    @staticmethod
    def from_str(s):
        s = s.lower()
        if s == "on":
            return Toggle.On
        elif s == "off":
            return Toggle.Off
        elif s == "true":
            return Toggle.On
        elif s == "false":
            return Toggle.Off
        else:
            return None


class ScrubSchedule(Enum):
    Hourly = "hourly"
    Daily = "daily"
    Weekly = "weekly"
    BiWeekly = "biweekly"
    Monthly = "monthly"

    @staticmethod
    def from_str(s):
        if s == "hourly":
            return ScrubSchedule.Hourly
        elif s == "daily":
            return ScrubSchedule.Daily
        elif s == "weekly":
            return ScrubSchedule.Weekly
        elif s == "biweekly":
            return ScrubSchedule.BiWeekly
        elif s == "monthly":
            return ScrubSchedule.Monthly
        else:
            return None

    def __str__(self):
        return "scrub-frequency"


class ScrubAggression(Enum):
    Aggressive = "aggressive"
    Lazy = "lazy"
    Normal = "normal"

    def __str__(self):
        return "scrub-throttle"

    @staticmethod
    def from_str(s):
        if s == "aggressive":
            return ScrubAggression.Aggressive
        elif s == "lazy":
            return ScrubAggression.Lazy
        elif s == "normal":
            return ScrubAggression.Normal
        else:
            return None


class ScrubControl(Enum):
    Pause = "pause"
    Resume = "resume"
    Status = "status"
    OnDemand = "ondemand"

    def __str__(self):
        return "scrub"

    @staticmethod
    def from_str(s):
        if s == "pause":
            return ScrubControl.Pause
        elif s == "resume":
            return ScrubControl.Resume
        elif s == "status":
            return ScrubControl.Status
        elif s == "ondemand":
            return ScrubControl.OnDemand
        else:
            return None


class BitrotOption(object):
    ScrubThrottle = ScrubAggression
    ScrubFrequency = ScrubSchedule
    Scrub = ScrubControl

    def __init__(self, option):
        self.option = option

    def __str__(self):
        return "{}".format(self.option)


class GlusterOption(object):
    # Valid IP address which includes wild card patterns including *,
    # such as 192.168.1.*
    AuthAllow = "auth.allow"
    # Valid IP address which includes wild card patterns including *,
    # such as 192.168.2.*
    AuthReject = "auth.reject"
    # Specifies the duration for the lock state to be maintained on the
    # client after a network disconnection in seconds
    # Range: 10-1800
    ClientGraceTimeout = "client.grace-timeout"
    # Specifies the maximum number of blocks per file on which self-heal
    # would happen simultaneously.
    # Range: 0-1025
    ClusterSelfHealWindowSize = "cluster.self-heal-window-size"
    # enable/disable client.ssl flag in the volume
    ClientSsl = "client.ssl"
    # Specifies the type of self-heal. If you set the option as "full", the
    # entire file is copied from source to destinations. If the option is set
    # to "diff" the file blocks
    # that are not in sync are copied to destinations.
    ClusterDataSelfHealAlgorithm = "cluster.data-self-heal-algorithm"
    # Percentage of required minimum free disk space
    DiagnosticsFopSampleBufSize = "diagnostics.fop-sample-buf-size"
    ClusterMinFreeDisk = "cluster.min-free-disk"
    # Specifies the size of the stripe unit that will be read from or written
    # to in bytes
    ClusterStripeBlockSize = "cluster.stripe-block-size"
    # Allows you to turn-off proactive self-heal on replicated
    ClusterSelfHealDaemon = "cluster.self-heal-daemon"
    # This option makes sure the data/metadata is durable across abrupt
    # shutdown of the brick.
    ClusterEnsureDurability = "cluster.ensure-durability"
    # The log-level of the bricks.
    DiagnosticsBrickLogLevel = "diagnostics.brick-log-level"
    # The log-level of the clients.
    DiagnosticsClientLogLevel = "diagnostics.client-log-level"
    # Interval in which we want to collect FOP latency samples.  2 means
    # collect a sample every 2nd FOP.
    DiagnosticsFopSampleInterval = "diagnostics.fop-sample-interval"
    # The maximum size of our FOP sampling ring buffer. Default: 65535
    # Enable the File Operation count translator
    DiagnosticsCountFopHits = "diagnostics.count-fop-hits"
    # Interval (in seconds) at which to auto-dump statistics. Zero disables
    # automatic dumping.
    DiagnosticsStatsDumpInterval = "diagnostics.stats-dump-interval"
    # The interval after wish a cached DNS entry will be re-validated.
    # Default: 24 hrs
    DiagnosticsStatsDnscacheTtlSec = "diagnostics.stats-dnscache-ttl-sec"
    # Statistics related to the latency of each operation would be tracked.
    DiagnosticsLatencyMeasurement = "diagnostics.latency-measurement"
    # Statistics related to file-operations would be tracked.
    DiagnosticsDumpFdStats = "diagnostics.dump-fd-stats"
    # Enables automatic resolution of split brain issues
    FavoriteChildPolicy = "cluster.favorite-child-policy"
    # Enables you to mount the entire volume as read-only for all the clients
    # (including NFS clients) accessing it.
    FeaturesReadOnly = "features.read-only"
    # Enables self-healing of locks when the network disconnects.
    FeaturesLockHeal = "features.lock-heal"
    # For performance reasons, quota caches the directory sizes on client.
    # You can set timeout indicating the maximum duration of directory sizes
    # in cache, from the time they are
    # populated, during which they are considered valid
    FeaturesQuotaTimeout = "features.quota-timeout"
    # Automatically sync the changes in the filesystem from Master to Slave.
    GeoReplicationIndexing = "geo-replication.indexing"
    # The time frame after which the operation has to be declared as dead,
    # if the server does not respond for a particular operation.
    NetworkFrameTimeout = "network.frame-timeout"
    # For 32-bit nfs clients or applications that do not support 64-bit inode
    # numbers or large files, use this option from the CLI to make Gluster NFS
    # return 32-bit inode numbers instead of 64-bit inode numbers.
    NfsEnableIno32 = "nfs.enable-ino32"
    # Set the access type for the specified sub-volume.
    NfsVolumeAccess = "nfs.volume-access"
    # If there is an UNSTABLE write from the client, STABLE flag will be
    # returned to force the client to not send a COMMIT request. In some
    # environments, combined with a replicated GlusterFS setup, this option
    # can improve write performance. This flag allows users to trust Gluster
    # replication logic to sync data to the disks and recover when required.
    # COMMIT requests if received will be handled in a default manner by
    # fsyncing. STABLE writes are still handled in a sync manner.
    NfsTrustedWrite = "nfs.trusted-write"
    # All writes and COMMIT requests are treated as async. This implies that
    # no write requests
    # are guaranteed to be on server disks when the write reply is received
    # at the NFS client.
    # Trusted sync includes trusted-write behavior.
    NfsTrustedSync = "nfs.trust-sync"
    # This option can be used to export specified comma separated
    # subdirectories in the volume.
    # The path must be an absolute path. Along with path allowed list of
    # IPs/hostname can be
    # associated with each subdirectory. If provided connection will allowed
    # only from these IPs.
    # Format: \<dir>[(hostspec[hostspec...])][,...]. Where hostspec can be an
    # IP address,
    # hostname or an IP range in CIDR notation. Note: Care must be taken
    # while configuring
    # this option as invalid entries and/or unreachable DNS servers can
    # introduce unwanted
    # delay in all the mount calls.
    NfsExportDir = "nfs.export-dir"
    # Enable/Disable exporting entire volumes, instead if used in conjunction
    # with
    # nfs3.export-dir, can allow setting up only subdirectories as exports.
    NfsExportVolumes = "nfs.export-volumes"
    # Enable/Disable the AUTH_UNIX authentication type. This option is
    # enabled by default for
    # better interoperability. However, you can disable it if required.
    NfsRpcAuthUnix = "nfs.rpc-auth-unix"
    # Enable/Disable the AUTH_NULL authentication type. It is not recommended
    # to change the default value for this option.
    NfsRpcAuthNull = "nfs.rpc-auth-null"
    # Allow client connections from unprivileged ports. By default only
    # privileged ports are
    # allowed. This is a global setting in case insecure ports are to be
    # enabled for all exports using a single option.
    NfsPortsInsecure = "nfs.ports-insecure"
    # Turn-off name lookup for incoming client connections using this option.
    # In some setups,
    # the name server can take too long to reply to DNS queries resulting in
    # timeouts of mount
    # requests. Use this option to turn off name lookups during address
    # authentication. Note,
    NfsAddrNamelookup = "nfs.addr-namelookup"
    # For systems that need to run multiple NFS servers, you need to prevent
    # more than one from
    # registering with portmap service. Use this option to turn off portmap
    # registration for Gluster NFS.
    NfsRegisterWithPortmap = "nfs.register-with-portmap"
    # Turn-off volume being exported by NFS
    NfsDisable = "nfs.disable"
    # Size of the per-file write-behind buffer.Size of the per-file
    # write-behind buffer.
    PerformanceWriteBehindWindowSize = "performance.write-behind-window-size"
    # The number of threads in IO threads translator.
    PerformanceIoThreadCount = "performance.io-thread-count"
    # If this option is set ON, instructs write-behind translator to perform
    # flush in background, by returning success (or any errors, if any
    # of previous writes were failed)
    # to application even before flush is sent to backend filesystem.
    PerformanceFlushBehind = "performance.flush-behind"
    # Sets the maximum file size cached by the io-cache translator. Can use the
    # normal size
    # descriptors of KB, MB, GB,TB or PB (for example, 6GB). Maximum size u64.
    PerformanceCacheMaxFileSize = "performance.cache-max-file-size"
    # Sets the minimum file size cached by the io-cache translator. Values same
    # as "max" above
    PerformanceCacheMinFileSize = "performance.cache-min-file-size"
    # The cached data for a file will be retained till 'cache-refresh-timeout'
    # seconds, after which data re-validation is performed.
    PerformanceCacheRefreshTimeout = "performance.cache-refresh-timeout"
    # Size of the read cache in bytes
    PerformanceCacheSize = "performance.cache-size"
    # enable/disable readdir-ahead translator in the volume
    PerformanceReadDirAhead = "performance.readdir-ahead"
    # If this option is enabled, the readdir operation is performed parallely
    # on all the bricks,
    # thus improving the performance of readdir. Note that the performance
    # improvement is higher
    # in large clusters
    PerformanceParallelReadDir = "performance.parallel-readdir"
    # maximum size of cache consumed by readdir-ahead xlator. This value is
    # global and total
    # memory consumption by readdir-ahead is capped by this value, irrespective
    # of the
    # number/size of directories cached
    PerformanceReadDirAheadCacheLimit = "performance.rda-cache-limit"
    # Allow client connections from unprivileged ports. By default only
    # privileged ports are
    # allowed. This is a global setting in case insecure ports are to be
    # enabled for all exports using a single option.
    ServerAllowInsecure = "server.allow-insecure"
    # Specifies the duration for the lock state to be maintained on the server
    # after a network disconnection.
    ServerGraceTimeout = "server.grace-timeout"
    # enable/disable server.ssl flag in the volume
    ServerSsl = "server.ssl"
    # Location of the state dump file.
    ServerStatedumpPath = "server.statedump-path"
    SslAllow = "auth.ssl-allow"
    SslCertificateDepth = "ssl.certificate-depth"
    SslCipherList = "ssl.cipher-list"
    # Number of seconds between health-checks done on the filesystem that
    # is used for the
    # brick(s). Defaults to 30 seconds, set to 0 to disable.
    StorageHealthCheckInterval = "storage.health-check-interval"

    def __init__(self, option, value):
        self.option = option
        self.value = value

    @staticmethod
    def from_str(s: str, value):
        if s == "auth-allow":
            return GlusterOption(option=GlusterOption.AuthAllow, value=value)
        elif s == "auth-reject":
            return GlusterOption(option=GlusterOption.AuthReject, value=value)
        elif s == "auth.ssl-allow":
            return GlusterOption(option=GlusterOption.SslAllow, value=value)
        elif s == "client.ssl":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClientSsl, value=t)
        elif s == "cluster.favorite-child-policy":
            policy = SplitBrainPolicy.from_str(value)
            return GlusterOption(option=GlusterOption.FavoriteChildPolicy,
                                 value=policy)
        elif s == "client-grace-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClientGraceTimeout,
                                 value=i)
        elif s == "cluster-self-heal-window-size":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClusterSelfHealWindowSize,
                                 value=i)
        elif s == "cluster-data-self-heal-algorithm":
            s = SelfHealAlgorithm.from_str(value)
            return GlusterOption(
                option=GlusterOption.ClusterDataSelfHealAlgorithm, value=s)
        elif s == "cluster-min-free-disk":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClusterMinFreeDisk,
                                 value=i)
        elif s == "cluster-stripe-block-size":
            i = int(value)
            return GlusterOption(option=GlusterOption.ClusterStripeBlockSize,
                                 value=i)
        elif s == "cluster-self-heal-daemon":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClusterSelfHealDaemon,
                                 value=t)
        elif s == "cluster-ensure-durability":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ClusterEnsureDurability,
                                 value=t)
        elif s == "diagnostics-brick-log-level":
            return GlusterOption(option=GlusterOption.DiagnosticsBrickLogLevel,
                                 value="")
        elif s == "diagnostics-client-log-level":
            return GlusterOption(option=GlusterOption.DiagnosticsClientLogLevel,
                                 value="")
        elif s == "diagnostics-latency-measurement":
            t = Toggle.from_str(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsLatencyMeasurement, value=t)
        elif s == "diagnostics.count-fop-hits":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.DiagnosticsCountFopHits,
                                 value=t)
        elif s == "diagnostics.stats-dump-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsStatsDumpInterval, value=i)
        elif s == "diagnostics.fop-sample-buf-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsFopSampleBufSize,
                value=i)
        elif s == "diagnostics.fop-sample-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsFopSampleInterval, value=i)
        elif s == "diagnostics.stats-dnscache-ttl-sec":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.DiagnosticsStatsDnscacheTtlSec, value=i)
        elif s == "diagnostics-dump-fd-stats":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.DiagnosticsDumpFdStats,
                                 value=t)
        elif s == "features-read-only":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.FeaturesReadOnly, value=t)
        elif s == "features-lock-heal":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.FeaturesLockHeal, value=t)
        elif s == "features-quota-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.FeaturesQuotaTimeout,
                                 value=i)
        elif s == "geo-replication-indexing":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.GeoReplicationIndexing,
                                 value=t)
        elif s == "network-frame-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.NetworkFrameTimeout,
                                 value=i)
        elif s == "nfs-enable-ino32":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsEnableIno32, value=t)
        elif s == "nfs-volume-access":
            s = AccessMode.from_str(value)
            return GlusterOption(option=GlusterOption.NfsVolumeAccess, value=s)
        elif s == "nfs-trusted-write":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsTrustedWrite, value=t)
        elif s == "nfs-trusted-sync":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsTrustedSync, value=t)
        elif s == "nfs-export-dir":
            return GlusterOption(option=GlusterOption.NfsExportDir, value=value)
        elif s == "nfs-export-volumes":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsExportVolumes, value=t)
        elif s == "nfs-rpc-auth-unix":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRpcAuthUnix, value=t)
        elif s == "nfs-rpc-auth-null":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRpcAuthNull, value=t)
        elif s == "nfs-ports-insecure":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsPortsInsecure, value=t)
        elif s == "nfs-addr-namelookup":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsAddrNamelookup,
                                 value=t)
        elif s == "nfs-register-with-portmap":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsRegisterWithPortmap,
                                 value=t)
        elif s == "nfs-disable":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.NfsDisable, value=t)
        elif s == "performance-write-behind-window-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceWriteBehindWindowSize, value=i)
        elif s == "performance-io-thread-count":
            i = int(value)
            return GlusterOption(option=GlusterOption.PerformanceIoThreadCount,
                                 value=i)
        elif s == "performance-flush-behind":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceFlushBehind,
                                 value=t)
        elif s == "performance-cache-max-file-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheMaxFileSize,
                value=i)
        elif s == "performance-cache-min-file-size":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheMinFileSize,
                value=i)
        elif s == "performance-cache-refresh-timeout":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceCacheRefreshTimeout, value=i)
        elif s == "performance-cache-size":
            i = int(value)
            return GlusterOption(option=GlusterOption.PerformanceCacheSize,
                                 value=i)
        elif s == "performance-readdir-ahead":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                                 value=t)
        elif s == "performance-parallel-readdir":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.PerformanceReadDirAhead,
                                 value=t)
        elif s == "performance-readdir-cache-limit":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.PerformanceReadDirAheadCacheLimit, value=i)
        elif s == "server.ssl":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ServerSsl, value=t)
        elif s == "server-allow-insecure":
            t = Toggle.from_str(value)
            return GlusterOption(option=GlusterOption.ServerAllowInsecure,
                                 value=t)
        elif s == "server-grace-timeout":
            i = int(value)
            return GlusterOption(option=GlusterOption.ServerGraceTimeout,
                                 value=i)
        elif s == "server-statedump-path":
            return GlusterOption(option=GlusterOption.ServerStatedumpPath,
                                 value=value)
        elif s == "ssl.certificate-depth":
            i = int(value)
            return GlusterOption(option=GlusterOption.SslCertificateDepth,
                                 value=i)
        elif s == "ssl.cipher-list":
            return GlusterOption(GlusterOption.SslCipherList, value=value)
        elif s == "storage-health-check-interval":
            i = int(value)
            return GlusterOption(
                option=GlusterOption.StorageHealthCheckInterval,
                value=i)
        else:
            return None


class GlusterError(Exception):
    # Custom error handling for the library
    def __init__(self, message):
        super(GlusterError, self).__init__(message)


def run_command(command: str, arg_list: List[str], as_root: bool,
                script_mode: bool) -> Result:
    """
    command: String.  The command to run
    arg_list: list. A list of arguments to add to the command
    as_root: bool.  Should the command be run as root
    script_mode: bool.  Should the command be run in script mode
    :returns: Result.  Ok(stdout) or Err(stderr)
    """
    cmd = []
    if as_root:
        cmd.append("sudo")
    else:
        cmd.append(command)
    if script_mode:
        cmd.append("--mode=script")
    for arg in arg_list:
        cmd.append(arg)
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.PIPE)
        return Ok(output)
    except subprocess.CalledProcessError as e:
        return Err(e.output)


def get_local_ip() -> Result:
    """
    Returns the local IPAddr address associated with this server
    # Failures
    Returns a GlusterError representing any failure that may have happened
    while trying to
    query this information.
    """
    output = run_command("ip", ["route", "show", "0.0.0.0/0"],
                         False, False)
    if output.is_err():
        return Err("ip route show cmd failed: {}".format(output.value))

    default_route_stdout = output.value

    # default via 192.168.1.1 dev wlan0  proto static
    addr_regex = re.compile(r"(?P<addr>via \S+)")
    default_route_parse = addr_regex.search(default_route_stdout)
    if default_route_parse is None:
        return Err("Unable to parse default route from: {}".format(
            default_route_stdout))
    addr_raw = default_route_parse.group("addr")
    addr = addr_raw.split(" ")[1]

    src_address_output = run_command("ip", ["route", "get", addr[0]],
                                     False, False)
    if src_address_output.is_err():
        return Err(
            "ip route get cmd failed: {}".format(src_address_output.value))
    # 192.168.1.1 dev wlan0  src 192.168.1.7
    local_address_stdout = src_address_output.value
    src_regex = re.compile(r"(?P<src>src \S+)")
    capture_output = src_regex.search(local_address_stdout)
    if capture_output is None:
        return Err("Unable to parse local_address from: {}".format(
            local_address_stdout))

    local_address_src = capture_output.group('src')
    local_ip = local_address_src.split(" ")[1]
    ip_addr = ip_address(local_ip.strip())

    return Ok(ip_addr)  # Resolves a str hostname into a ip address.


def resolve_to_ip(address: str) -> Result:
    """
    Resolves an dns address to an ip address.  Relies on dig
    :param address: String.  Hostname to resolve to an ip address
    :return: result
    """
    if address == "localhost":
        local_ip = get_local_ip()
        if local_ip.is_err():
            return Err(local_ip.value)
        try:
            ip_addr = ip_address(local_ip.value)
            return Ok(ip_addr)
        except ValueError:
            return Err("failed to parse ip address: {}".format(local_ip.value))

    arg_list = ["+short", address.strip()]
    output = run_command("dig", arg_list, False, False)

    if output.is_err():
        return Err("dig cmd failed with error:{}".format(output.value))
    # Remove the trailing . and newline
    trimmed = output.value.strip().rstrip(".")
    try:
        ip_addr = ip_address(trimmed)
        return Ok(ip_addr)
    except ValueError:
        return Err("failed to parse ip address: {}".format(trimmed))


def get_local_hostname() -> str:
    """
    A function to get the information from /etc/hostname
    :return: string. Hostname
    """
    hostname_path = "/etc/hostname"
    with open(hostname_path) as f:
        try:
            s = f.readlines()
            return s[0].strip()
        except IOError:
            raise IOError("Error opening {}".format(hostname_path))


def translate_to_bytes(value: str) -> float:
    """
    This is a helper function to convert values such as 1PB into a bytes.

    :param value: str. Size representation to be parsed
    :return: float. Value in bytes
    """
    k = 1024

    sizes = [
        "KB",
        "MB",
        "GB",
        "TB",
        "PB"
    ]

    if value.endswith("Bytes"):
        return float(value.rstrip("Bytes"))
    else:
        for power, size in enumerate(sizes, 1):
            if value.endswith(size):
                return float(value.rstrip(size)) * (k ** power)
        raise ValueError("Cannot translate value")
