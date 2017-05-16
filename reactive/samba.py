from charmhelpers.core.hookenv import status_set
import os
from io import TextIOBase


# / Write the samba configuration file out to disk
def render_samba_configuration(f: TextIOBase, volume_name: str) -> int:
    bytes_written = 0
    bytes_written += f.write("[}]\n".format(volume_name))
    bytes_written += f.write(b"path = /mnt/glusterfs\n")
    bytes_written += f.write(b"read only = no\n")
    bytes_written += f.write(b"guest ok = yes\n")
    bytes_written += f.write(b"kernel share modes = no\n")
    bytes_written += f.write(b"kernel oplocks = no\n")
    bytes_written += f.write(b"map archive = no\n")
    bytes_written += f.write(b"map hidden = no\n")
    bytes_written += f.write(b"map read only = no\n")
    bytes_written += f.write(b"map system = no\n")
    bytes_written += f.write(b"store dos attributes = yes\n")
    return bytes_written


def samba_config_changed(volume_name: str) -> bool:
    if os.path.exists("/etc/samba/smb.conf"):
        # Lets check if the smb.conf matches what we're going to write.
        # If so then it was already setup and there's nothing to do
        with open("/etc/samba/smb.conf") as existing_config:
            old_config = existing_config.readlines()
            new_config = ""
            render_samba_configuration(new_config, volume_name)
            if new_config == existing_config:
                # configs are identical
                return False
            else:
                return True
    # Config doesn't exist.
    return True


def setup_samba(volume_name: str):
    cifs_config = juju.config_get("cifs")
    if cifs_config is None:
        # Samba isn't enabled
        # log!("Samba option is not enabled")
        return
    if not samba_config_changed(volume_name):
        # log!("Samba is already setup.  Not reinstalling")
        return
    status_set("Maintenance", "Installing Samba")
    apt_install(["samba"])
    status_set("Maintenance", "Configuring Samba")
    # log!("Setting up Samba")
    samba_conf = File.create("/etc/samba/smb.conf")
    bytes_written = render_samba_configuration(samba_conf, volume_name)
    # log!(format!("Wrote {} bytes to /etc/samba/smb.conf", bytes_written))
    # log!("Starting Samba service")
    status_set("Maintenance", "Starting Samba")
    service_start("smbd")
