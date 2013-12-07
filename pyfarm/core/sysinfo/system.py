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

:const OS:
    Integer containing the os type, this is a mapping to a value
    on :class:`OperatingSystem`

:const IS_LINUX:
    set to True if running linux

:const IS_WINDOWS:
    set to True if running windows

:const IS_MAC:
    set to True if running mac os

:const IS_OTHER:
    set to True if running something we could not determine a mapping for

:const IS_POSIX:
    set to True if running a posix platform (such as linux or mac)

:const CASE_SENSITIVE_FILESYSTEM:
    set to True if the filesystem is case sensitive

:const CASE_SENSITIVE_ENVIRONMENT:
    set to True if the environment is case sensitive

:const ARCHITECTURE:
    the architecture if the machine itself as an integer

:const ARCHITECTURE32:
    set to ``True`` when the system architecture is 32 bits

:const ARCHITECTURE64:
    set to ``True`` when the system architecture is 64 bits

:const INTERPRETER_ARCHITECTURE:
    the architecture of the Python interpreter as an integer

:const INTERPRETER_ARCHITECTURE32:
    set to ``True`` when the Python interpreter is 32 bits

:const INTERPRETER_ARCHITECTURE64:
    set to ``True`` when the Python interpreter is 64 bits
"""

import os
import platform
import sys
import time
import tempfile

import psutil

from pyfarm.core.enums import OS, OperatingSystem


def _case_sensitive_filesystem():  # pragma: no cover
    """returns True if the file system is case sensitive"""
    fd, path = tempfile.mkstemp()
    if path.islower() or not path.isupper():
        return os.path.isfile(path) and not os.path.isfile(path.upper())
    else:
        return os.path.isfile(path) and not os.path.isfile(path.lower())


def _case_sensitive_environment():  # pragma: no cover
    """returns True if the environment is case sensitive"""
    envvar_lower = os.urandom(4).encode("hex")
    envvar_upper = envvar_lower.upper()

    # populate environment then compare the difference
    os.environ.update({envvar_lower: "0", envvar_upper: "1"})
    result = os.environ[envvar_lower] != os.environ[envvar_upper]

    # cleanup
    os.environ.pop(envvar_lower)
    os.environ.pop(envvar_upper)

    return result


def _system_architecture():
    bitness, linkage = platform.architecture()

    if bitness == "64bit":
        return 64
    elif bitness == "32bit":
        return 32
    else:
        raise ValueError("unknown bitness %s" % bitness)


def _interpreter_architecture():
    """returns the architecture of the interpreter itself (32 or 64)"""
    if hasattr(sys, "maxsize"):
        if sys.maxsize > 2**32:
            return 64
        else:
            return 32
    else:  # Python < 2.6, not as accurate as the above
        if platform.architecture()[0] == "64bits":
            return 64
        else:
            return 32


def uptime():
    """
    Returns the amount of time the system has been running in
    seconds
    """
    return time.time() - psutil.BOOT_TIME


IS_LINUX = OS == OperatingSystem.LINUX
IS_WINDOWS = OS == OperatingSystem.WINDOWS
IS_MAC = OS == OperatingSystem.MAC
IS_OTHER = OS == OperatingSystem.OTHER
IS_POSIX = OS in (OperatingSystem.LINUX, OperatingSystem.MAC)
CASE_SENSITIVE_FILESYSTEM = _case_sensitive_filesystem()
CASE_SENSITIVE_ENVIRONMENT = _case_sensitive_environment()
ARCHITECTURE = _system_architecture()
ARCHITECTURE64 = ARCHITECTURE == 64
ARCHITECTURE32 = ARCHITECTURE == 32
INTERPRETER_ARCHITECTURE = _interpreter_architecture()
INTERPRETER_ARCHITECTURE32 = INTERPRETER_ARCHITECTURE == 32
INTERPRETER_ARCHITECTURE64 = INTERPRETER_ARCHITECTURE == 64