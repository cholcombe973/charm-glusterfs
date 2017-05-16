from io import TextIOBase
from result import Ok, Result
from typing import List

__author__ = 'Chris Holcombe <chris.holcombe@canonical.com>'


class ShellScript(object):
    # A very basic representation of a shell script. There is an interpreter,
    # some comments and a list of commands
    def __init__(self, interpreter: str, comments: List[str],
                 commands: List[str]):
        # the interpreter to use
        self.interpreter = interpreter
        # Any comments here will be joined with newlines when written back out
        self.comments = comments
        # Any commands here will be joined with newlines when written back out
        self.commands = commands

    def write(self, f: TextIOBase) -> Result:
        # Write the run control class back out to a file
        bytes_written = 0
        bytes_written += f.write("{}\n".format(self.interpreter))
        bytes_written += f.write("\n".join(self.comments))
        bytes_written += f.write("\n")
        bytes_written += f.write("\n".join(self.commands))
        bytes_written += f.write("\n")
        return Ok(bytes_written)


"""
def test_parse():
    shell_script = 
#!/bin/sh -e
#
# rc.local
#
# This script is executed at the end of each multiuser runlevel.
# Make sure that the script will "exit 0" on success or any other
# value on error.
#
# In order to enable or disable this script just change the execution
# bits.
#
# By default this script does nothing.
#exit 0
    c = std.io.Cursor.new(shell_script)
    result = parse(c)
    # println!("Result: :}", result)
    buff = []
    result2 = result.write(buff)
"""


def parse(f: TextIOBase) -> Result:
    comments = []
    commands = []
    interpreter = ""

    buf = f.readlines()

    for line in buf:
        trimmed = line.strip()
        if trimmed.startswith("#!"):
            interpreter = trimmed
        elif trimmed.starts_with("#"):
            comments.append(trimmed)
        else:
            if not trimmed.is_empty():
                commands.append(trimmed)
    return Ok(ShellScript(interpreter=interpreter,
                          comments=comments,
                          commands=commands))
