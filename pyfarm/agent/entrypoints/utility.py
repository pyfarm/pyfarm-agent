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
~~~~~~~

Small objects and functions which facilitate operations
on the main entry point class.
"""

import atexit
import os
import sys
from os.path import isfile, isdir, dirname

try:
    from os import setuid, setgid, fork
except ImportError:  # pragma: no cover
    setuid = NotImplemented
    setgid = NotImplemented
    fork = NotImplemented

import psutil
import requests
from requests import ConnectionError

from pyfarm.core.enums import OS
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert
from pyfarm.core.sysinfo import network

logger = getLogger("agent")


def get_json(url):
    """retrieve the json data from the given url or returns None"""
    try:
        page = requests.get(
            url, headers={"Content-Type": "application/json"})
    except ConnectionError:
        logger.debug("GET %s (connection error)" % url)
        return None
    else:
        logger.debug("GET %s" % url)
        logger.debug("  contents: %s" % page.text)
        if not page.ok:
            logger.warning("%s status was %s" % repr(page.reason))
            return None
        else:
            return page.json()


def get_process(pidfile):
    """
    Returns (pid, process_object) after loading the pid file.  If we
    have problems loading the pid file or the process does not exist
    this function will return (None, None)
    """
    if not isfile(pidfile):
        logger.debug("%s does not exist" % pidfile)
        return None, None

    # retrieve the pid from the file
    logger.debug("opening %s" % pidfile)
    with open(pidfile, "r") as pid_file_object:
        pid = pid_file_object.read().strip()

    # safeguard in case the file does not contain data
    if not pid:
        logger.debug("no pid in %s" % pidfile)

        try:
            os.remove(pidfile)
            logger.debug("removed empty pid file %s" % pidfile)
        except OSError:
            pass

        return None, None

    # convert the pid to a number from a string
    try:
        pid = convert.ston(pid)
    except ValueError:
        logger.error(
            "failed to convert pid in %s to a number" % pidfile)
        raise

    # this shouldn't happen....but just in case
    if pid == os.getpid():
        logger.warning("%s contains the current process pid" % pidfile)

    # try to load up the process object
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        logger.debug("no such process %s" % pid)

        try:
            os.remove(pidfile)
            logger.debug("removed stale pid file %s" % pidfile)
        except OSError:
            pass

    else:
        process_name = process.name.lower()
        logger.debug("%s is named '%s'" % (pid, process_name))


        # the vast majority of the time, the process will be
        # ours in this case
        if process_name == "pyfarm-agent" \
                or process_name.startswith("pyfarm"):
            return pid, process

        # if it's a straight Python process it might still
        # be ours depending on how it was launched but we can't
        # do much else without more information
        elif process_name.startswith("python"):
            logger.warning(
                "%s appears to be a normal Python process and may not "
                "be pyfarm-agent" % pid)
            return pid, process

        else:
            logger.warning(
                "Process name is neither python or "
                "pyfarm-agent, instead it was '%s'." % process_name)

    return None, None


def get_pids(pidfile, index_url):
    """
    Returns a named tuple of the pid from the file, pid from the http
    server, main process object
    """
    # get the pid from the file, the process object and some information
    # from the REST api
    pid_from_file, process = get_process(pidfile)
    index = get_json(index_url) or {}

    if not index:
        logger.debug("failed to retrieve agent information using the REST api")

    return pid_from_file, index.get("pid")


def start_daemon_posix(log, logerr, chroot, uid, gid):
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
            sys.exit(1)

        # decouple from the parent environment
        os.chdir(chroot or "/")
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
            sys.exit(1)

        # flush any pending data before we duplicate
        # the file descriptors
        sys.stdout.flush()
        sys.stderr.flush()

        # open up file descriptors for the new process
        stdin = open(os.devnull, "r")
        stdout = open(log, "a+")
        stderr = open(logerr, "a+", 0)
        os.dup2(stdin.fileno(), sys.stdin.fileno())
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

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


def write_pid_file(path, pid):
    # if this fails is because we've done something wrong somewhere
    # else in this code
    assert not isfile(path), "pid file should not exist now!"

    pidfile_dirname = dirname(path)
    if not isdir(pidfile_dirname):
        try:
            os.makedirs(pidfile_dirname)
        except OSError:
            logger.warning("failed to create %s" % pidfile_dirname)

    with open(path, "w") as pidfile:
        pidfile.write(str(pid))

    logger.debug("wrote %s to %s" % (pid, pidfile.name))

    def remove_pid_file(pidfile):
        try:
            os.remove(pidfile)
            logger.debug("removed %s" % pidfile)
        except OSError as e:
            logger.warning("failed to remove %s: %s" % (pidfile, e))

    atexit.register(remove_pid_file, path)


def get_default_ip():
    """returns the default ip address to use"""
    try:
        return network.ip()
    except ValueError:
        logger.error("failed to find network ip address")
        return "127.0.0.1"