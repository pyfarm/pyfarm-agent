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
from argparse import _StoreAction, _StoreTrueAction
from errno import EEXIST, ENOENT
from os.path import dirname, abspath

try:
    from os import setuid, setgid, fork
except ImportError:  # pragma: no cover
    setuid = NotImplemented
    setgid = NotImplemented
    fork = NotImplemented


from pyfarm.core.enums import OS, INTEGER_TYPES
from pyfarm.core.utility import convert
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import system
from pyfarm.agent.utility import rmpath

logger = getLogger("agent.cmd")

SYSTEMID_MAX = 281474976710655


class SetConfig(_StoreAction):
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
        self.isfile = kwargs.pop("isfile", False)
        super(SetConfig, self).__init__(*args, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if self.isfile:
            values = abspath(values)

        config[self.key] = values
        super(SetConfig, self).__call__(
            parser, namespace, values, option_string=option_string)


class SetConfigConst(_StoreTrueAction):
    """
    Performs the same actions as :class:`SetConfig` except
    it meant to always set a constant value (much like 'store_true' would)
    """
    def __init__(self, *args, **kwargs):
        value = kwargs.pop("value")
        self.key = kwargs.pop("key")
        super(SetConfigConst, self).__init__(*args, **kwargs)
        self.const = value

    def __call__(self, parser, namespace, values, option_string=None):
        config[self.key] = self.const
        super(SetConfigConst, self).__call__(
            parser, namespace, values, option_string=option_string)


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


def get_system_identifier(systemid, cache_path=None, write_always=False):
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

    :raises ValueError:
        Raised if ``systemid`` is provided and it's outside the value
        range of input (0 to :const:`SYSTEMID_MAX`)

    :raises TypeError:
        Raised if we receive an unexpected type for one of the inputs
    """
    if not any([systemid is None,
                systemid == "auto",
                isinstance(systemid, INTEGER_TYPES)]):
        raise TypeError("Expected None, 'auto' or an integer for "
                        "`input_systemid`")

    if isinstance(systemid, INTEGER_TYPES) \
            and not 0 < systemid <= SYSTEMID_MAX:
        raise ValueError("input_systemid's range is 0 to %s" % SYSTEMID_MAX)

    if cache_path is None:
        cache_path = config["agent_systemid_cache"]

    write_failed = False
    if systemid == "auto":
        try:
            # Try to read the systemid from the cached file
            with open(cache_path, "r") as cache_file:
                try:
                    systemid = convert.ston(cache_file.read())

                # There's something wrong with the data in the cache
                # file
                except ValueError:
                    logger.warning(
                        "Failed to read cached system identifier from %s, "
                        "this file will be removed.", cache_file.name)
                    rmpath(cache_path)

                    # overwrite because there's a problem with the
                    # stored value
                    write_always = True
                else:
                    logger.info("Loaded system identifier %s from %s",
                                systemid, cache_file.name)

        except (OSError, IOError) as e:
            # If the file exists there may be something wrong with it,
            # try to remove it.
            if e.errno != ENOENT:
                logger.error(
                    "System identifier cache file %s exists but it could not "
                    "be read: %s.  The file will be removed.", cache_path, e)
            write_failed = True

    if isinstance(systemid, INTEGER_TYPES) \
            and not 0 < systemid <= SYSTEMID_MAX:
        logger.warning(
            "System identifier from cache is not in range is "
            "0 to 281474976710655.  Cache file will be deleted.")
        rmpath(cache_path)
        write_always = True

    if write_failed or write_always:
        # Create the parent directory if it does not exist
        parent_dir = dirname(cache_path)
        try:
            os.makedirs(parent_dir)
        except (OSError, IOError) as e:
            if e.errno != EEXIST:
                logger.error("Failed to create %r: %s", parent_dir, e)
                raise
        else:
            logger.debug("Created %r", parent_dir)

        # System identifier is either not cache, invalid or
        # none was given.  Generate systemid and cache it.
        systemid = system.system_identifier()
        try:
            with open(cache_path, "w") as cache_file:
                cache_file.write(str(systemid))
        except (OSError, IOError) as e:
            logger.warning(
                "Failed to cache system identifier to %s: %s", systemid, e)
        else:
            logger.info(
                "Cached system identifier %s to %s",
                systemid, cache_file.name)

        if isinstance(systemid, INTEGER_TYPES) \
                and not 0 < systemid <= SYSTEMID_MAX:
            raise ValueError("systemid's range is 0 to %s" % SYSTEMID_MAX)

        return systemid

    logger.debug("Custom system identifier will not be cached.")
    return systemid
