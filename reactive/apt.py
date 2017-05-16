from typing import List
from charmhelpers.fetch import apt_install

#/ Ask apt-cache for the new candidate package that is available
def get_candidate_package_version(package_name: str) -> Result<Version, String>:
    cmd = Command::new("apt-cache")
    cmd.arg("policy")
    cmd.arg(package_name)
    output = cmd.output()
    if !output.status.success()
        return Err(String::from_utf8_lossy(output.stderr).into_owned())

    stdout = String::from_utf8_lossy(output.stdout)
    for line in stdout
        if line.contains("Candidate")
            parts: Vec<str> = line.split(' ').collect()
            match parts.last()
                Some(p) =>
                    version: Version = Version::parse(p).map_err(|e| e.msg)?
                    return Ok(version)

                None =>
                    return Err(format!("Unknown candidate line format: :?", parts))
    "Unable to find candidate upgrade package from stdout: {}".format(stdout)

