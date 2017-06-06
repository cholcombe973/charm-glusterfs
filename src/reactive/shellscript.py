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
        """

        :param interpreter: 
        :param comments: 
        :param commands: 
        """
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


def parse(f: TextIOBase) -> Result:
    """

    :param f: 
    :return: 
    """
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
