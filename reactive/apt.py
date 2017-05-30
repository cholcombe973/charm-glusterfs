from typing import List
from charmhelpers.fetch import apt_install
from result import Result

#/ Ask apt-cache for the new candidate package that is available
def get_candidate_package_version(package_name: str) -> Result:
    """

    :param package_name: 
    :return: 
    """
    output = check_output(["apt-cache", "policy", package_name])
    if not output.status.success():
        return Err(output.stderr)

    stdout = output.stdout
    for line in stdout:
        if line.contains("Candidate"):
            parts = line.split(' ')
            match parts.last()
                Some(p) =>
                    version: Version = Version::parse(p).map_err(|e| e.msg)
                    return Ok(version)

                None =>
                    return Err("Unknown candidate line format: {}".format(parts))
    "Unable to find candidate upgrade package from stdout: {}".format(stdout)

