from charmhelpers.core.hookenv import ERROR, log, relation_set, unit_public_ip

from lib.gluster.volume import volume_list


def fuse_relation_joined():
    # Fuse clients only need one ip address and they can discover the rest
    """

    """
    public_addr = unit_public_ip()
    volumes = volume_list()
    if volumes.is_err():
        log("volume list is empty.  Unable to complete fuse relation", ERROR)
        return
    data = {"gluster-public-address": public_addr,
            "volumes": " ".join(volumes.value)}
    relation_set(relation_settings=data)
