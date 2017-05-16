#!/usr/bin/python3

from charmhelpers.core.hookenv import action_get, action_fail
from gluster import BitrotOption, ScrubAggression, ScrubSchedule, ScrubControl
from gluster.volume import quota_list, volume_add_quota, volume_disable_bitrot,

volume_enable_bitrot,

volume_enable_quotas, volume_quotas_enabled, volume_remove_quota,
volume_set_bitrot_option, volume_set_options
import juju


def enable_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    ret_code, output = volume_enable_bitrot(vol)
    if ret_code is not 0:
        action_fail("enable bitrot failed with error: {}".format(output))


def disable_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    ret_code, output = volume_disable_bitrot(vol)
    if ret_code is not 0:
        action_fail("enable disable failed with error: {}".format(output))


def pause_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Pause)
    ret_code, output = volume_set_bitrot_option(vol, option)
    if ret_code is not 0:
        action_fail("pause bitrot scan failed with error: {}".format(output))


def resume_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Resume)
    ret_code, output = volume_set_bitrot_option(vol, option)
    if ret_code is not 0:
        action_fail("resume bitrot scan failed with error: {}".format(option))


def set_bitrot_scan_frequency():
    """
    
    :return: 
    """
    vol = action_get("volume")
    frequency = action_get("frequency")
    option = ScrubSchedule.from_str(frequency)
    ret_code, output = volume_set_bitrot_option(vol,
                                                BitrotOption.ScrubFrequency(
                                                    option))
    if ret_code is not 0:
        action_fail("set bitrot scan frequency failed with error: {}".format(
            output))


def set_bitrot_throttle():
    """
    
    :return: 
    """
    vol = action_get("volume")
    throttle = action_get("throttle")
    option = ScrubAggression.from_str(throttle)
    ret_code, output = volume_set_bitrot_option(vol,
                                                BitrotOption.ScrubThrottle(
                                                    option))
    if ret_code is not 0:
        action_fail("set bitrot throttle failed with error: {}".format(output))


def enable_volume_quota():
    """
    
    :return: 
    """
    # Gather our action parameters
    volume = action_get("volume")
    usage_limit = action_get("usage-limit")
    parsed_usage_limit = int(usage_limit)
    path = action_get("path")
    # Turn quotas on if not already enabled
    quotas_enabled = volume_quotas_enabled(volume)
    if !quotas_enabled:
        ret_code, output = volume_enable_quotas(volume)

    volume_add_quota(volume, path, parsed_usage_limit)


def disable_volume_quota():
    """
    
    :return: 
    """
    volume = action_get("volume")
    path = action_get("path")
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled:
        ret_code, output = volume_remove_quota(volume, path)
        if ret_code is not 0:
            # Notify the user of the failure and then return the error
            # up the stack
            action_fail("remove quota failed with error: {}".format(output))


def list_volume_quotas():
    """
    
    :return: 
    """
    volume = action_get("volume")
    try:
        quotas_enabled = volume_quotas_enabled(volume)
        if quotas_enabled:
            quotas = quota_list(volume)
            for quota in quotas:
                quota_string = "path:{} limit:{} used:{}".format(
                    quota.path,
                    quota.limit,
                    quota.used)
            juju.action_set("quotas", "\n".join(quota_string))
    except GlusterError as e:
        action_fail("failed to get quota information: {}".format(e.message))


def set_volume_options():
    """
    
    :return: 
    """
    # volume is a required parameter so this should be safe
    volume = ""

    # Gather all of the action parameters up at once.  We don't know what
    # the user wants to change.
    options = juju.action_get()
    settings = []
    for (key, value) in options:
        if key != "volume":
            settings.append(GlusterOption.from_str(key, value))
        else:
            volume = value

    volume_set_options(volume, settings)
    return None
