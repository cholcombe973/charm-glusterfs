use gluster::volume::volume_list

def fuse_relation_joined():
    # Fuse clients only need one ip address and they can discover the rest
    public_addr = juju.unit_get_public_addr()
    volumes = volume_list()
    juju.relation_set("gluster-public-address", public_addr)
    if Some(vols) = volumes:
        juju.relation_set("volumes", " ".join(vols))
