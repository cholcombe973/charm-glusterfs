from charmhelpers.core.hookenv import INFO, log, unit_private_ip


def server_removed():
    """
    Remove a server from the cluster
    """
    private_address = unit_private_ip()
    log("Removing server: {}".format(private_address), INFO)
