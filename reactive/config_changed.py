from charmhelpers.core.hookenv import log
from result import Ok, Err, Result

import std::fs::File
import std::path::Path
import std::process::Command

import super::super::{create_sysctl, ephemeral_unmount, finish_initialization, get_glusterfs_version,
                   initialize_storage}
import super::super::apt
import super::super::block
import super::super::upgrade

def config_changed() -> Result:
    r = check_for_new_devices()
    if r.is_err():
        log("Checking for new devices failed with error: {}".format(r.value), Error)
    r = check_for_sysctl()
    if r.is_err():
        log("Setting sysctl's failed with error: {}".format(r.value), Error)
    # If fails we fail the hook
    check_for_upgrade()
    return Ok(())

def check_for_new_devices() -> Result:
    log("Checking for new devices", Info)
    config = juju::Config::new())
    log("Checking for ephemeral unmount")
    ephemeral_unmount()
    #if config.changed("brick_devices")) {
    brick_devices = []
    # Get user configured storage devices
    manual_brick_devices = block.get_manual_bricks()
    brick_devices.extend(manual_brick_devices)

    # Get the juju storage block devices
    juju_config_brick_devices = block.get_juju_bricks()
    brick_devices.extend(juju_config_brick_devices)

    log("storage devices: {}".format(brick_devices))

    format_handles = []
    brick_paths  = []
    # Format all drives in parallel
    for device in brick_devices:
        if not device.initialized:
            log("Calling initialize_storage for {}".format(device.dev_path))
            # Spawn all format commands in the background
            format_handles.push(initialize_storage(device))
        else:
            # The device is already initialized, lets add it to our usable paths list
            log("{} is already initialized".format(device.dev_path))
            brick_paths.push(device.mount_path)
    # Wait for all children to finish formatting their drives
    for handle in format_handles:
        output_result = handle.format_child.wait_with_output()
        if output_result.is_ok():
            process_output_result = block.process_output(output_result.value)
            if process_output_result.is_ok():
                # success
                # 1. Run any post setup commands if needed
                finish_initialization(handle.device.dev_path)
                brick_paths.append(handle.device.mount_path)
            else:
                # Failed
                log("Device {} formatting failed with error: {}. Skipping".format(handle.device.dev_path,process_output_result.value),Error)
        else:
            #Failed
            log("Device {} formatting failed with error: {}. Skipping".format(handle.device.dev_path,output_result.value), Error)
    log("Usable brick paths: {}".format(brick_paths))
    return Ok(())

def check_for_sysctl() -> Result:
    config = juju::Config::new())
    if config.changed("sysctl")) {
        config_path = Path::new("/etc/sysctl.d/50-gluster-charm.conf")
        sysctl_file = File::create(config_path))
        sysctl_dict = juju::config_get("sysctl"))
        match sysctl_dict {
            Some(sysctl) => {
                create_sysctl(sysctl, mut sysctl_file)
                # Reload sysctl's
                cmd = Command::new("sysctl")
                cmd.arg("-p")
                cmd.arg(config_path.to_string_lossy().into_owned())
                output = cmd.output())
                if !output.status.success() {
                    return Err(String::from_utf8_lossy(output.stderr).into_owned())
                }
            }
            None => {}
        }
    }
    Ok(())
}

# If the config has changed this will initiated a rolling upgrade
def check_for_upgrade() -> Result:
    config = juju.Config.new()
    if not config.changed("source"):
        # No upgrade requested
        log("No upgrade requested")
        return Ok(())

    log("Getting current_version")
    current_version = get_glusterfs_version()

    log("Adding new source line")
    source = juju.config_get("source"))
    if not source.is_some():
        # No upgrade requested
        log("Source not set.  Cannot continue with upgrade")
        return Ok(())
    add_source(source.unwrap())
    log("Calling apt update")
    apt::apt_update()

    log("Getting proposed_version")
    proposed_version = apt::get_candidate_package_version("glusterfs-server")

    # Using semantic versioning if the new version is greater than we allow the upgrade
    if proposed_version > current_version:
        log("current_version: {}".format(current_version))
        log("new_version: {}".format(proposed_version))
        log("{} to {} is a valid upgrade path.  Proceeding.".format(current_version,proposed_version))
        return upgrade.roll_cluster(proposed_version)
    else:
        # Log a helpful error message
        log("Invalid upgrade path from {} to {}. The new version needs to be \
                            greater than the old version".format(current_version, proposed_version),
             Error)
        return Ok(())
