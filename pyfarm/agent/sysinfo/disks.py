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

import psutil

from pyfarm.agent.logger import getLogger

from collections import namedtuple
DiskInfo = namedtuple("DiskInfo", ("mountpoint", "free", "size"))

logger = getLogger("agent.disks")


def disks():
    """
    Returns a list of disks in the system, in the form of DiskInfo objects
    """
    out = []
    for partition in psutil.disk_partitions():
        usage = psutil.disk_usage(partition.mountpoint)

        info = DiskInfo(
            mountpoint=partition.mountpoint,
            free=usage.free,
            size=usage.total)
        out.append(info)

    return out
