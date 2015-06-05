# No shebang line, this module is meant to be imported
#
# Copyright 2015 Ambient Entertainment GmbH & Co. KG
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
Disks
-----

Contains information about the local disks.
"""

from os import statvfs

try:
    from wmi import WMI
except ImportError:  # pragma: no cover
    WMI = NotImplemented

from pyfarm.core.enums import WINDOWS, LINUX, BSD
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.disks")


class DiskLookupError(Exception):
    pass


class DiskInfo:
    def __init__(self, mountpoint, free, size):
        self.mountpoint = mountpoint
        self.free = free
        self.size = size


def disks():
    """
    Returns a list of disks in the system, in the for of DiskInfo objects
    """
    if WINDOWS:
        wmi = WMI()
        disks = wmi.Win32_LogicalDisk(DriveType=3)
        disk_info_list = [DiskInfo(x.Caption, x.FreeSpace, x.Size)
                          for x in disks]
        return disk_info_list

    elif LINUX or BSD:
        disk_info_list = []
        with open("/etc/mtab", "r") as mtab:
            for line in mtab:
                # Simple heuristic to filter out pseudo filesystems
                # (proc, sys and such) and net mounts
                if line.startswith("/") and not line.startswith("//"):
                    cols = line.split()
                    mountpoint = cols[1]
                    stat_struct = statvfs(mountpoint)
                    disk_info_list.append(DiskInfo(
                        mountpoint,
                        stat_struct.f_bavail * stat_struct.f_bsize,
                        stat_struct.f_blocks * stat_struct.f_frsize))
        return disk_info_list

    raise DiskLookupError("Don't know how to read disks on this OS")
