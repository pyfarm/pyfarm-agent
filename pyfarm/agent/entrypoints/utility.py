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

import atexit
import os
import sys
from os.path import isfile, isdir, dirname
from random import randint

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
from pyfarm.agent.sysinfo import network

logger = getLogger("agent")

SYSTEM_IDENT_MAX = 281474976710655


def get_json(url):
    """retrieve the json data from the given url or returns None"""
    try:
        page = requests.get(
            url, headers={"content-type": "application/json"})
    except ConnectionError:
        logger.debug("GET %s (connection error)" % url)
        return None
    else:
        logger.debug("GET %s" % url)
        logger.debug("  contents: %s" % page.text)
        if not page.ok:
            logger.warning("%s's status was %s" % (url, repr(page.reason)))
            return None
        else:
            return page.json()


# TODO: improve internal coverage
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
        except OSError:  # pragma: no cover
            pass

        return None, None

    # convert the pid to a number from a string
    try:
        pid = convert.ston(pid)
    except ValueError:  # pragma: no cover
        logger.error(
            "failed to convert pid in %s to a number" % pidfile)
        raise

    # this shouldn't happen....but just in case
    if pid == os.getpid():
        logger.warning("%s contains the current process pid" % pidfile)

    # try to load up the process object
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:  # pragma: no cover
        logger.debug("no such process %s" % pid)

        try:
            os.remove(pidfile)
            logger.debug("removed stale pid file %s" % pidfile)
        except OSError:
            pass

    else:
        process_name = process.name().lower()
        logger.debug("%s is named '%s'" % (pid, process_name))

        if "PYFARM_AGENT_TEST_RUNNING" in os.environ:
            pid = int(os.environ["PYFARM_AGENT_TEST_RUNNING"])
            return pid, psutil.Process(pid)

        # Be careful, we don't want to return a pid or process object
        # for something which might not be a PyFarm process.
        if not any([
                process_name in ("python", "coverage"),
                process_name.startswith("pyfarm")]):  # pragma: no cover
            raise OSError(
                "%s contains pid %s with the name %s.  This seems to be "
                "a process this script does not know about so we're stopping "
                "here rather than continuing." % (pidfile, pid, process_name))

        return pid, process

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

    if not index:  # pragma: no cover
        logger.debug("failed to retrieve agent information using the REST api")

    return pid_from_file, index.get("pid")


# This is a Linux specific test and will be hard to get due to the nature
# of how the tests are run, for now we're excluding it.
# TODO: figure out a reliable way to test  start_daemon_posix
def start_daemon_posix(log, logerr, chroot, uid, gid):  # pragma: no cover
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
        except OSError:  # pragma: no cover
            logger.warning("failed to create %s" % pidfile_dirname)

    with open(path, "w") as pidfile:
        pidfile.write(str(pid))

    logger.debug("wrote %s to %s" % (pid, pidfile.name))

    # Not testing this because it's only run on exist
    def remove_pid_file(pidfile):  # pragma: no cover
        try:
            os.remove(pidfile)
            logger.debug("removed %s" % pidfile)
        except OSError as e:
            logger.warning("failed to remove %s: %s" % (pidfile, e))

    atexit.register(remove_pid_file, path)


def get_system_identifier(cache=None, overwrite=False):
    """
    Generate a system identifier based on the mac addresses
    of this system.  Each mac address is converted to an
    integer then XORed together into an integer value
    no greater than 281474976710655.

    This value is used to help identify the agent to the
    master more reliably than by other means alone.  In general
    we only create this value once so even if the mac addresses
    should change the return value should not.

    For reference, this function builds a system identifier in
    exactly the same way :func:`uuid.getnode` does.  The main
    differences are that we can handle multiple addresses and
    cache the value between invocations.

    :param string cache:
        If provided then the value will be retrieved from this
        location if it exists.  If the location does not exist
        however this function will generate the value and then
        store it for future use.

    :param bool overwrite:
        If ``True`` then overwrite the cache instead of reading
        from it
    """
    cached_value = None
    remove_cache = False

    # read from cache if a file was provided
    if cache is not None and (overwrite or isfile(cache)):
        with open(cache, "rb") as cache_file:
            data = cache_file.read()

        # Convert the data in the cache file back to an integer
        try:
            cached_value = int(data)

        # If we can't convert it
        except ValueError as e:
            remove_cache = True
            logger.warning(
                "System identifier in %r could not be converted to "
                "an integer: %r", cache, e)

        # Be sure that the cached value is smaller than then
        # max we expect.
        if cached_value is not None and cached_value > SYSTEM_IDENT_MAX:
            remove_cache = True
            logger.warning(
                "The system identifier found in %r cannot be "
                "larger than %s.", cache, SYSTEM_IDENT_MAX)

        # Somewhere above we determined that the cache file
        # contains invalid information and must be removed.
        if remove_cache:
            try:
                os.remove(cache)
            except (IOError, OSError):
                logger.fatal(
                    "Failed to remove invalid cache file %r", cache)
                raise
            else:
                logger.debug(
                    "Removed invalid system identifier cache %r", cache)

        if cached_value is not None and not remove_cache:
            logger.debug(
                "Read system identifier %r from %r", cached_value, cache)
            return cached_value

    result = 0
    for mac in network.mac_addresses():
        result ^= int("0x" + mac.replace(":", ""), 0)

    # Under rare conditions we could end up not generating
    # anything.  In these cases produce a warning then
    # generate something random.
    if result == 0:
        logger.warning(
            "Failed to generate a system identifier.  One will be "
            "generated randomly and then cached for future use.")

        result = randint(0, SYSTEM_IDENT_MAX)

    if cache is not None:
        try:
            with open(cache, "wb") as cache_file:
                cache_file.write(str(result))
        except (IOError, OSError) as e:
            logger.warning(
                "Failed to write system identifier to %r: %s", cache, e)
        else:
            logger.debug(
                "Cached system identifier %r to %r", result, cache_file.name)

    return result
