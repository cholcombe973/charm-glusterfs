from io import TextIOWrapper
import os
from result import Err, Ok, Result
from typing import List


class FsEntry:
    def __init__(self, fs_spec: str, mountpoint: os.path, vfs_type: str,
                 mount_options: List[str], dump: bool, fsck_order: int):
        """
        For help with what these fields mean consult: `man fstab` on linux.
        :param fs_spec:  The device identifer
        :param mountpoint:  the mount point
        :param vfs_type:  which filesystem type it is
        :param mount_options: mount options
        :param dump: This field is used by dump(8) to determine which 
        filesystems need to be dumped
        :param fsck_order: This field is used by fsck(8) to determine the 
        order in which filesystem checks are done at boot time.
        """
        self.fs_spec = fs_spec
        self.mountpoint = mountpoint
        self.vfs_type = vfs_type
        self.mount_options = mount_options
        self.dump = dump
        self.fsck_order = fsck_order

    def __eq__(self, item):
        if item.fs_spec == self.fs_spec and \
                        item.mountpoint == self.mountpoint and \
                        item.vfs_type == self.vfs_type and \
                        item.mount_options == self.mount_options and \
                        item.dump == self.dump and \
                        item.fsck_order == self.fsck_order:
            return True
        return False

    def __str__(self):
        return "{} {} {} {} {} {}".format(self.fs_spec,
                                          self.mountpoint,
                                          self.vfs_type,
                                          ",".join(self.mount_options),
                                          self.dump,
                                          self.fsck_order)


class FsTab:
    def __init__(self, location: os.path):
        self.location = location

    def get_entries(self) -> Result:
        """
        Takes the location to the fstab and parses it.  On linux variants
        this is usually /etc/fstab.  On SVR4 systems store block devices and
        mount point information in /etc/vfstab file. AIX stores block device
        and mount points information in /etc/filesystems file.

        :return: 
        """
        with open(self.location, "r") as file:
            entries = self.parse_entries(file)
            if entries.is_err():
                return Err(entries.value)
            return Ok(entries.value)

    def parse_entries(self, file: TextIOWrapper) -> Result:
        """

        :param file: 
        :return: 
        """
        entries = []
        contents = file.readlines()

        for line in contents:
            if line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 6:
                continue
            fsck_order = int(parts[5])
            entries.append(FsEntry(
                fs_spec=parts[0],
                mountpoint=os.path.join(parts[1]),
                vfs_type=parts[2],
                mount_options=parts[3].split(","),
                dump=False if parts[4] == "0" else True,
                fsck_order=fsck_order))
        return Ok(entries)

    def save_fstab(self, entries: List[FsEntry]) -> Result:
        """

        :param entries: 
        :return: 
        """
        try:
            with open(self.location, "w") as f:
                bytes_written = 0
                for entry in entries:
                    bytes_written += f.write(
                        "{spec} {mount} {vfs} {options} {dump} {fsck}\n".format(
                            spec=entry.fs_spec,
                            mount=entry.mountpoint.display(),
                            vfs=entry.vfs_type,
                            options=",".join(entry.mount_options),
                            dump="1" if entry.dump else "0",
                            fsck=entry.fsck_order))
                return Ok(bytes_written)
        except OSError:
            pass

    def add_entry(self, entry: FsEntry) -> Result:
        """
        Add a new entry to the fstab.  If the fstab previously did not 
        contain this entry
        then true is returned.  Otherwise it will return false indicating 
        it has been updated

        :param entry: 
        :return: 
        """
        entries = self.get_entries()
        if entries.is_err():
            return Err(entries.value)
        print("entries: {}".format(entries.value))
        position = [i for i, x in enumerate(entries.value) if
                    entry == x]
        if len(position) is not 0:
            entries.value.remove(position[0])
        entries.value.append(entry)
        save_result = self.save_fstab(entries.value)
        print("Save_result: {}".format(save_result.value))

        if len(position) is not 0:
            return Ok(False)
        else:
            return Ok(True)

    def add_entries(self, entries: List[FsEntry]) -> Result:
        """
        Bulk add a new entries to the fstab.

        :param entries: 
        :return: 
        """
        existing_entries = self.get_entries()
        if existing_entries.is_err():
            return Err(existing_entries.value)
        for new_entry in entries:
            if new_entry in existing_entries.value:
                # The old entries contain this so lets update it
                position = [i for i, x in enumerate(existing_entries.value) if
                            new_entry == x]
                del existing_entries.value[position]
                existing_entries.value.append(new_entry)
            else:
                existing_entries.value.append(new_entry),
        self.save_fstab(existing_entries.value)
        return Ok(())

    def remove_entry(self, spec: str) -> Result:
        """
        Remove the fstab entry that corresponds to the spec given.  
        IE: first fields match
        Returns true if the value was present in the fstab.

        :param spec: 
        :return: 
        """
        entries = self.get_entries()
        if entries.is_err():
            return Err(entries.value)
        position = [i for i, x in enumerate(entries.value) if
                    spec == x.fs_spec]
        if len(position) is not 0:
            del entries.value[position[0]]
            self.save_fstab(entries.value)
            return Ok(True)
        else:
            return Ok(False)
