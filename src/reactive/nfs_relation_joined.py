"""
def nfs_relation_joined() -> Result<(), String>
    config_value = juju::config_get("virtual_ip_addresses")
    volumes = volume_list()
    if Some(vols) = volumes:
        relation_set("volumes", " ".join(vols))

    # virtual_ip_addresses isn't set.  Handing back my public address
    if not config_value.is_some():
        public_addr = juju::unit_get_public_addr()
        relation_set("gluster-public-address", public_addr)
     else:
        # virtual_ip_addresses is set.  Handing back the DNS resolved address
        dns_name = resolve_first_vip_to_dns()?
        relation_set("gluster-public-address", dns_name)

"""
