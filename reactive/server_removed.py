from charmhelpers.core.hookenv import INFO, log


def server_removed():
    private_address = juju.unit_get_private_addr()
    log("Removing server: {}".format(private_address), INFO)
