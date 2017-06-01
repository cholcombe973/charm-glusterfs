#!/usr/bin/python3

from charmhelpers.core.hookenv import action_get, action_fail, action_set
from lib.gluster.lib import BitrotOption, ScrubAggression, ScrubSchedule, \
    ScrubControl, GlusterOption
from lib.gluster.volume import quota_list, volume_add_quota, \
    volume_disable_bitrot, volume_enable_bitrot, \
    volume_enable_quotas, volume_quotas_enabled, volume_remove_quota, \
    volume_set_bitrot_option, volume_set_options


def enable_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    output = volume_enable_bitrot(vol)
    if output.is_err():
        action_fail("enable bitrot failed with error: {}".format(output.value))


def disable_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    if not vol:
        action_fail("volume not specified")
    output = volume_disable_bitrot(vol)
    if output.is_err():
        action_fail("enable disable failed with error: {}".format(output.value))


def pause_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Pause)
    output = volume_set_bitrot_option(vol, option)
    if output.is_err():
        action_fail(
            "pause bitrot scan failed with error: {}".format(output.value))


def resume_bitrot_scan():
    """
    
    :return: 
    """
    vol = action_get("volume")
    option = BitrotOption.Scrub(ScrubControl.Resume)
    output = volume_set_bitrot_option(vol, option)
    if output.is_err():
        action_fail(
            "resume bitrot scan failed with error: {}".format(option.value))


def set_bitrot_scan_frequency():
    """
    
    :return: 
    """
    vol = action_get("volume")
    frequency = action_get("frequency")
    option = ScrubSchedule.from_str(frequency)
    output = volume_set_bitrot_option(vol, BitrotOption.ScrubFrequency(option))
    if output.is_err():
        action_fail("set bitrot scan frequency failed with error: {}".format(
            output.value))


def set_bitrot_throttle():
    """
    
    :return: 
    """
    vol = action_get("volume")
    throttle = action_get("throttle")
    option = ScrubAggression.from_str(throttle)
    output = volume_set_bitrot_option(vol, BitrotOption.ScrubThrottle(option))
    if output.is_err():
        action_fail(
            "set bitrot throttle failed with error: {}".format(output.value))


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
    if not quotas_enabled:
        output = volume_enable_quotas(volume)
        if output.is_err():
            action_fail("Enable quotas failed: {}".format(output.value))

    output = volume_add_quota(volume, path, parsed_usage_limit)
    if output.is_err():
        action_fail("Add quota failed: {}".format(output.value))


def disable_volume_quota():
    """
    
    :return: 
    """
    volume = action_get("volume")
    path = action_get("path")
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled:
        output = volume_remove_quota(volume, path)
        if output.is_err():
            # Notify the user of the failure and then return the error
            # up the stack
            action_fail(
                "remove quota failed with error: {}".format(output.value))


def list_volume_quotas():
    """
    
    :return: 
    """
    volume = action_get("volume")
    quotas_enabled = volume_quotas_enabled(volume)
    if quotas_enabled:
        quotas = quota_list(volume)
        if quotas.is_err():
            action_fail(
                "Failed to get volume quotas: {}".format(quotas.value))
        quota_strings = []
        for quota in quotas.value:
            quota_string = "path:{} limit:{} used:{}".format(
                quota.path,
                quota.hard_limit,
                quota.used)
            quota_strings.append(quota_string)
        action_set({"quotas": "\n".join(quota_strings)})


def set_volume_options():
    """
    
    :return: 
    """
    # volume is a required parameter so this should be safe
    volume = ""

    # Gather all of the action parameters up at once.  We don't know what
    # the user wants to change.
    options = action_get()
    settings = []
    for (key, value) in options:
        if key != "volume":
            settings.append(GlusterOption(key, value))
        else:
            volume = value

    volume_set_options(volume, settings)
