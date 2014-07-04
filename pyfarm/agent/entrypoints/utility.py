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
from argparse import Action
from os.path import isfile
from random import randint

try:
    from os import setuid, setgid, fork
except ImportError:  # pragma: no cover
    setuid = NotImplemented
    setgid = NotImplemented
    fork = NotImplemented


from pyfarm.core.enums import OS, STRING_TYPES, INTEGER_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import system
from pyfarm.agent.utility import rmpath

logger = getLogger("agent.cmd.util")

SYSTEMID_MAX = 281474976710655


class SetConfig(Action):
    """
    An action which can be used by an argument parser to update
    the configuration object when a flag on the command line is
    set

    >>> from functools import partial
    >>> from argparse import ArgumentParser
    >>> from pyfarm.agent.config import config
    >>> parser = ArgumentParser()
    >>> parser.add_argument("--foo", action=partial(SetConfig, key="foobar"))
    >>> parser.parse_args(["--foo", "bar"])
    >>> assert "foobar" in config and config["foobar"] == "bar"
    """
    def __init__(self, *args, **kwargs):
        self.key = kwargs.pop("key")
        super(SetConfig, self).__init__(*args, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        assert isinstance(values, STRING_TYPES)
        config[self.key] = values


# This is a Linux specific test and will be hard to get due to the nature
# of how the tests are run, for now we're excluding it.
# TODO: figure out a reliable way to test  start_daemon_posix
def start_daemon_posix(log, chroot, uid, gid):  # pragma: no cover
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


def get_system_identifier(systemid=None, cache_path=None, overwrite=False):
    """
    Generate a system identifier based on the mac addresses
    of this system.  Each mac address is converted to an
    integer then XORed together into an integer value
    no greater than 0xffffffffffff.  This maximum value is
    derived from a mac address which has been maxed out
    ``ff:ff:ff:ff:ff:ff``.

    This value is used to help identify the agent to the
    master more reliably than by other means alone.  In general
    we only create this value once so even if the mac addresses
    should change the return value should not.

    For reference, this function builds a system identifier in
    exactly the same way :func:`uuid.getnode` does.  The main
    differences are that we can handle multiple addresses and
    cache the value between invocations.

    :param int systemid:
        If provided then use this value directly instead of trying
        to generate one.  This is useful if you want to use a
        specific value and provide caching at the same time.

    :param string cache_path:
        If provided then the value will be retrieved from this
        location if it exists.  If the location does not exist
        however this function will generate the value and then
        store it for future use.

    :param bool overwrite:
        If ``True`` then overwrite the cache instead of reading
        from it

    :raises ValueError:
        Raised if ``systemid`` is provided and it's outside the value
        range of input (0 to :const:`SYSTEMID_MAX`)

    :raises TypeError:
        Raised if we receive an unexpected type for one of the inputs
    """
    remove_cache = False

    if isinstance(systemid, INTEGER_TYPES):
        if not 0 < systemid <= SYSTEMID_MAX:
            raise ValueError("systemid's range is 0 to %s" % SYSTEMID_MAX)

        # We don't want to cache custom values because the default behavior
        # is to read the correct system id from disk
        logger.warning(  # pragma: no cover
            "Specific system identifier has been provided, this value will "
            "not be cached.")
        return systemid

    if systemid is not None:
        raise TypeError("Expected ``systemid`` to be an integer")

    if cache_path is not None and not isinstance(cache_path, STRING_TYPES):
        raise TypeError("Expected a string for ``cache_path``")

    # read from cache if a file was provided
    if cache_path is not None and (overwrite or isfile(cache_path)):
        with open(cache_path, "rb") as cache_file:
            cache_data = cache_file.read()

        # Convert the data in the cache file back to an integer
        try:
            systemid = int(cache_data)

        except ValueError as e:
            remove_cache = True
            logger.warning(
                "System identifier in %r could not be converted to "
                "an integer: %r", cache_path, e)

        # Be sure that the cached value is smaller than then
        # max we expect.
        if systemid is not None and systemid > SYSTEMID_MAX:
            systemid = None
            remove_cache = True
            logger.warning(
                "The system identifier found in %r cannot be "
                "larger than %s.", cache_path, SYSTEMID_MAX)

        # Somewhere above we determined that the cache file
        # contains invalid information and must be removed.
        if remove_cache:
            try:
                os.remove(cache_path)
            except (IOError, OSError):  # pragma: no cover
                logger.warning(
                    "Failed to remove invalid cache file %r, system id will "
                    "be generated live.", cache_path)
                systemid = None  # reset the systemid
                rmpath(cache_path, exit_retry=True)

            else:
                logger.debug(
                    "Removed invalid system identifier cache %r", cache_path)

        # We have a systemid value already and the cache file itself
        # is not invalid.
        if systemid is not None and not remove_cache:
            logger.debug(
                "Read system identifier %r from %r", systemid, cache_path)
            return systemid

    # If the system id has not been set, or we invalidated it
    # above, generate it.
    if systemid is None:
        systemid = system.system_identifier()

        # Under rare conditions we could end up not generating
        # anything.  In these cases produce a warning then
        # generate something random.
        if systemid == 0:  # pragma: no cover
            logger.warning(
                "Failed to generate a system identifier.  One will be "
                "generated randomly and then cached for future use.")

            systemid = randint(0, SYSTEMID_MAX)

    # Try to cache the value
    if cache_path is not None:
        try:
            with open(cache_path, "wb") as cache_file:
                cache_file.write(str(systemid))
        except (IOError, OSError) as e:  # pragma: no cover
            logger.warning(
                "Failed to write system identifier to %r: %s", cache_path, e)
        else:
            logger.debug(
                "Cached system identifier %r to %r", systemid, cache_file.name)

    return systemid
