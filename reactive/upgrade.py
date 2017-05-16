import os

extern crate chrono
extern crate gluster
extern crate init_daemon
extern crate juju
extern crate rand
extern crate rustc_serialize
extern crate uuid

use std::fs::create_dir, File, OpenOptions
use std::io::BufRead, BufReader, Read, Write
use std::path::Path
use std::thread

use self::chrono::*
use self::gluster::peer::Peer
use self::gluster::volume::volume_info
use self::rand::distributions::IndependentSample, Range
use self::rustc_serialize::json
use self::uuid::Uuid

use super::apt
use super::debian::version::Version
use super::get_glusterfs_version

def get_local_uuid() -> Uuid, String>:
    # File looks like this:
    # UUID=30602134-698f-4e53-8503-163e175aea85
    # operating-version=30800
    f = File::open("/var/lib/glusterd/glusterd.info") )
    reader = BufReader::new(f)

    line = String::new()
    reader.read_line(line) )
    if line.contains("UUID")
        parts: Vec<str> = line.split("=").collect()
        uuid = Uuid::parse_str(parts[1].trim()) )
        return uuid)

    Err("Unable to find UUID".to_string())


# Edge cases:
# 1. Previous node dies on upgrade, can we retry
def roll_cluster(new_version: Version) -> (), String>:
    # This is tricky to get right so here's what we're going to do.
    # :param new_version: str of the version to upgrade to
    # There's 2 possible cases: Either I'm first in line or not.
    # If I'm not first in line I'll wait a random time between 5-30 seconds
    # and test to see if the previous peer is upgraded yet.
    #
    log(format!("roll_cluster called with ", new_version))
    volume_name = juju::config_get("volume_name".to_string()) )
    my_uuid = get_local_uuid()

    # volume_name always has a default
    volume_bricks = volume_info(volume_name.unwrap()) ).bricks
    peer_list: Vec<Peer> = volume_bricks.iter().map(|x| x.peer.clone()).collect()
    log(format!("peer_list: :", peer_list))

    # Sort by UUID
    peer_list.sort()
    # We find our position by UUID
    position = match peer_list.iter().position(|x| x.uuid == my_uuid)
        Some(p) => p,
        None =>
            log(format!("Unable to determine upgrade position from: :", peer_list),
                 Error)
            return Err("Unable to determine upgrade position".to_string())


    log(format!("upgrade position: ", position))
    if position == 0
        # I'm first!  Roll
        # First set a key to inform others I'm about to roll
        lock_and_roll(my_uuid, new_version)
     else
        # Check if the previous node has finished
        juju::status_set(juju::Status
                             status_type: juju::StatusType::Waiting,
                             message: format!("Waiting on : to finish upgrading",
                                              peer_list[position - 1]),
                         ) )
        wait_on_previous_node(peer_list[position - 1], new_version)
        lock_and_roll(my_uuid, new_version)

    ())


def upgrade_peer(new_version: Version) -> (), String>:
    current_version = get_glusterfs_version() )
    juju::status_set(juju::Status
                         status_type: juju::StatusType::Maintenance,
                         message: "Upgrading peer".to_string(),
                     ) )
    log(format!("Current ceph version is ", current_version))
    log(format!("Upgrading to: ", new_version))

    apt::service_stop("glusterfs-server")
    apt::apt_install(vec!["glusterfs-server", "glusterfs-common", "glusterfs-client"])
    apt::service_start("glusterfs-server")
    super::update_status()
    return ())


def lock_and_roll(my_uuid: Uuid, version: Version) -> (), String>:
    start_timestamp = Local::now()

    log(format!("gluster_key_set __start ",
                 my_uuid,
                 version,
                 start_timestamp))
    gluster_key_set(format!("__start", my_uuid, version), start_timestamp)
    log("Rolling")

    # This should be quick
    upgrade_peer(version)
    log("Done")

    stop_timestamp = Local::now()
    # Set a key to inform others I am finished
    log(format!("gluster_key_set __done ",
                 my_uuid,
                 version,
                 stop_timestamp))
    gluster_key_set(format!("__done", my_uuid, version), stop_timestamp)

    return ())




def gluster_key_get(key: str) -> Option<DateTime<Local>>:
    f = match File::open(format!("/mnt/glusterfs/.upgrade/", key))
        f) => f,
        Err(_) =>
            return None


    s = f.readlines()
    log("gluster_key_get read {} bytes".format(??)
    raise
    log("gluster_key_get failed to read file \
                        /mnt/glusterfs/.upgraded/.{} Error: {}".format(key, e),


    decoded: DateTime<Local> = json::decode(s)
        d) => d,
        Err(e) =>
            log(format!("Failed to decode json file in gluster_key_get(): ", e),
                 Error)
            return None


    Some(decoded)


def gluster_key_set(key: str, timestamp: DateTime<Local>):
    if !Path::new("/mnt/glusterfs/.upgrade").exists()
        create_dir("/mnt/glusterfs/.upgrade") )

    file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .open(format!("/mnt/glusterfs/.upgrade/", key))
    encoded = json.dumps(timestamp)
    file.write(encoded)


def gluster_key_exists(key: str) -> bool:
    location = "/mnt/glusterfs/.upgrade/{}".format(key)
    return os.path.exists(location)


def wait_on_previous_node(previous_node: Peer, version: Version):
    log("Previous node is: {}".format(previous_node))

    previous_node_finished =
        gluster_key_exists(format!("__done", previous_node.uuid, version))

    while not previous_node_finished:
        log("{} is not finished. Waiting".format(previous_node.uuid))
        # Has this node been trying to upgrade for longer than
        # 10 minutes
        # If so then move on and consider that node dead.

        # NOTE: This assumes the clusters clocks are somewhat accurate
        # If the hosts clock is really far off it may cause it to skip
        # the previous node even though it shouldn't.
        current_timestamp = Local::now()

        previous_node_start_time =
            gluster_key_get(format!("__start", previous_node.uuid, version))
        match previous_node_start_time
            Some(previous_start_time) =>
                if (current_timestamp - Duration::minutes(10)) > previous_start_time:
                    # Previous node is probably dead.  Lets move on
                    if previous_node_start_time.is_some()
                        log("Waited 10 mins on node {}. current time:  > \
                                            previous node start time:  Moving on".format(previous_node.uuid),
                                     (current_timestamp - Duration::minutes(10)),
                                     previous_start_time))
                        return ()
                else:
                    # I have to wait.  Sleep a random amount of time and then
                    # check if I can lock,upgrade and roll.
                    between = Range::new(5, 30)
                    rng = rand::thread_rng()
                    wait_time = between.ind_sample(rng)
                    log(format!("waiting for  seconds", wait_time))
                    thread::sleep(::std::time::Duration::from_secs(wait_time))
                    previous_node_finished =
                        gluster_key_exists(format!("__done", previous_node.uuid, version))
            None =>
                # There is no previous start time.  What should we do
