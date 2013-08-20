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
Information about the operating system including type, filesystem information,
and other relevant information.  This module may also contain os specific
information such as the Linux distribution, Windows version, bitness, etc.
"""

import os
import time
import tempfile

import psutil
from pyfarm.core.enums import OS, OperatingSystem


class OperatingSystemInfo(object):
    """
    .. note::
        This class has already been instanced onto
        `pyfarm.system.operating_system`

    Namespace class which returns information about
    the current operating system such as case-sensitivity, type, etc.

    :attr OS:
        Integer containing the os type, this is a mapping to a value
        on :class:`OperatingSystem`

    :attr IS_LINUX:
        set to True if running linux

    :attr IS_WINDOWS:
        set to True if running windows

    :attr IS_MAC:
        set to True if running mac os

    :attr IS_OTHER:
        set to True if running something we could not determine a mapping for

    :attr IS_POSIX:
        set to True if running a posix platform (such as linux or mac)

    :attr CASE_SENSITIVE:
        set to True if the filesystem is case sensitive
    """
    OS = OS
    IS_LINUX = OS == OperatingSystem.LINUX
    IS_WINDOWS = OS == OperatingSystem.WINDOWS
    IS_MAC = OS == OperatingSystem.MAC
    IS_OTHER = OS == OperatingSystem.OTHER
    IS_POSIX = OS in (OperatingSystem.LINUX, OperatingSystem.MAC)
    CASE_SENSITIVE = None

    def __init__(self):
        if self.__class__.CASE_SENSITIVE is None:
            fid, path = tempfile.mkstemp()
            exists = map(os.path.isfile, [path, path.lower(), path.upper()])
            if not any(exists):
                raise ValueError(
                    "failed to determine if path was case sensitive")
            elif all(exists):
                self.__class__.CASE_SENSITIVE = False

            elif exists.count(True) == 1:
                self.__class__.CASE_SENSITIVE = True

            try:
                os.remove(path)
            except:
                pass

    def uptime(self):
        """
        Returns the amount of time the system has been running in
        seconds
        """
        return time.time() - psutil.BOOT_TIME