# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Utility
=======

Small objects and functions which facilitate operations
on the main entry point class.
"""

import os
import sys

try:
    from os import setuid, setgid, fork
except ImportError:  # pragma: no cover
    setuid = NotImplemented
    setgid = NotImplemented
    fork = NotImplemented


from pyfarm.core.enums import OS
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.cmd")


# This is a Linux specific test and will be hard to get due to the nature
# of how the tests are run, for now we're excluding it.
# TODO: figure out a reliable way to test  start_daemon_posix
def start_daemon_posix(log, chdir, uid, gid):  # pragma: no cover
    """
    Runs the agent process via a double fork.  This basically a duplicate
    of Marcechal's original code with some adjustments:

        http://www.jejik.com/articles/2007/02/
        a_simple_unix_linux_daemon_in_python/

    Source files from his post are here:
        http://www.jejik.com/files/examples/daemon.py
        http://www.jejik.com/files/examples/daemon3x.py
    """
    # first fork
    try:
        pid = fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(
            "fork 1 failed (errno: %s): %s" % (e.errno, e.strerror))
        return 1

    # decouple from the parent environment
    os.chdir(chdir or "/")
    os.setsid()
    os.umask(0)

    # second fork
    try:
        pid = fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        logger.error(
            "fork 2 failed (errno: %s): %s" % (e.errno, e.strerror))
        return 1

    # flush any pending data before we duplicate
    # the file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    # open up file descriptors for the new process
    stdin = open(os.devnull, "r")
    logout = open(log, "a+", 0)
    os.dup2(stdin.fileno(), sys.stdin.fileno())
    os.dup2(logout.fileno(), sys.stdout.fileno())
    os.dup2(logout.fileno(), sys.stderr.fileno())

    # if requested, set the user id of this process
    if uid is not None and setuid is not NotImplemented:
        setuid(uid)

    elif uid is not None:
        logger.warning(
            "--uid was requested but `setuid` is not "
            "implemented on %s" % OS.title())

    # if requested, set the group id of this process
    if gid is not None and setgid is not NotImplemented:
        setgid(gid)

    elif gid is not None:
        logger.warning(
            "--gid was requested but `setgid` is not "
            "implemented on %s" % OS.title())
