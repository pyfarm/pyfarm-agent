# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
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
System
------

Information about the operating system including type, filesystem information,
and other relevant information.  This module may also contain os specific
information such as the Linux distribution, Windows version, bitness, etc.
"""

import atexit
import os
import platform
import sys
import time
import tempfile
import uuid
from errno import ENOENT
from os.path import isfile

import psutil

try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo.network import mac_addresses

logger = getLogger("agent.sysinfo")

try:
    _filesystem_is_case_sensitive
except NameError:  # pragma: no cover
    _filesystem_is_case_sensitive = None

try:
    _environment_is_case_sensitive
except NameError:  # pragma: no cover
    _environment_is_case_sensitive = None


def filesystem_is_case_sensitive():  # pragma: no cover
    """returns True if the file system is case sensitive"""
    global _filesystem_is_case_sensitive
    if _filesystem_is_case_sensitive is not None:
        return _filesystem_is_case_sensitive

    fd, path = tempfile.mkstemp()
    case_sensitive = not all(map(isfile, [path, path.lower(), path.upper()]))

    try:
        os.remove(path)
    except (WindowsError, OSError, NotImplementedError) as e:
        if getattr(e, "errno", None) != ENOENT:
            logger.warning("Could not remove temp file %s: %s: %s",
                           path, type(e).__name__, e)

            @atexit.register
            def remove():
                try:
                    os.remove(path)
                except (WindowsError, OSError, NotImplementedError) as e:
                    if getattr(e, "errno", None) != ENOENT:
                        logger.error("Failed to remove %s: %s", path, e)

    os.close(fd)

    return case_sensitive


def environment_is_case_sensitive():
    """returns True if the environment is case sensitive"""
    global _environment_is_case_sensitive
    if _environment_is_case_sensitive is not None:
        return _environment_is_case_sensitive

    envvar_lower = "PYFARM_CHECK_ENV_CASE_" + uuid.uuid4().hex
    envvar_upper = envvar_lower.upper()

    # populate environment then compare the difference
    os.environ.update({envvar_lower: "0", envvar_upper: "1"})
    _environment_is_case_sensitive = \
        os.environ[envvar_lower] != os.environ[envvar_upper]

    # remove the envvars we just made
    for envvar in (envvar_lower, envvar_upper):
        os.environ.pop(envvar, None)

    return _environment_is_case_sensitive


def machine_architecture(arch=platform.machine().lower()):
    """returns the architecture of the host itself"""
    if arch in ("amd64", "x86_64", "ia64") or "wow64" in arch:
        return 64

    elif arch in ("i386", "i686", "x86"):
        return 32

    elif not arch:
        raise NotImplementedError(
            "Cannot handle `arch` being unpopulated.")

    else:
        raise NotImplementedError(
            "Don't know how to handle a machine architecture %s" % repr(arch))


# Don't collect coverage because it's using the internal Python
# implementation which varies between platforms.
def interpreter_architecture():  # pragma: no cover
    """returns the architecture of the interpreter itself (32 or 64)"""
    if hasattr(sys, "maxsize"):
        if sys.maxsize > 2**32:
            return 64
        else:
            return 32
    else:
        # Python < 2.6, not as accurate as the above
        if platform.architecture()[0] == "64bits":
            return 64
        else:
            return 32


def uptime():
    """
    Returns the amount of time the system has been running in
    seconds.
    """
    return time.time() - psutil.boot_time()


def operating_system(plat=sys.platform):
    """
    Returns the operating system for the given platform.  Please
    note that while you can call this function directly you're more
    likely better off using values in :mod:`pyfarm.core.enums` instead.
    """
    if plat.startswith("linux"):
        return "linux"
    elif plat.startswith("win"):
        return "windows"
    elif plat.startswith("darwin"):
        return "mac"
    else:
        logger.warning("unknown operating system: %r", plat)
        return "other"


def system_identifier():
    """Generates a system identifier"""
    result = 0
    for address in mac_addresses(long_addresses=False, as_integers=True):
        result ^= address
    return result
