import apt
from result import Err, Ok, Result


def get_candidate_package_version(package_name: str) -> Result:
    """
    Ask apt-cache for the new candidate package that is available
    :param package_name: The package to check for an upgrade
    :return: Ok with the new candidate version or Err in case the candidate
        was not found
    """
    cache = apt.Cache()
    try:
        version = cache[package_name].candidate.version
        return Ok(version)
    except KeyError:
        return Err("Unable to find candidate upgrade package for: {}".format(
            package_name))
