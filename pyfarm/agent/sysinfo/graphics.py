# No shebang line, this module is meant to be imported
#
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
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
graphics
--------

Contains information about the installed graphics cards
"""

from exceptions import Exception
from os import path
from re import compile
from subprocess import Popen, PIPE

try:
    from wmi import WMI
except ImportError:  # pragma: no cover
    WMI = NotImplemented

from pyfarm.core.enums import WINDOWS, LINUX
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.sysinfo.gpu")


class GPULookupError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def graphics_cards():
    """
    Returns a list of the full names of GPUs installed in this system
    """
    if WINDOWS:
        wmi = WMI()
        gpus = wmi.Win32_VideoController.query()[0].Name
        gpu_names = [x.name for x in gpus]
        return gpu_names

    elif LINUX:
        lspci = None
        if path.isfile("/sbin/lspci"):
            lspci = "/sbin/lspci"
        elif path.isfile("/usr//sbin/lspci"):
            lspci = "/usr/sbin/lspci"
        else:
            logger.warning("Could not look up graphics cards, consider "
                           "installing pci-utils")
            raise GPULookupError("lspci not found")

        lspci_pipe = Popen(lspci, stdout=PIPE)
        gpu_names = []
        for line in lspci_pipe.stdout:
            if "VGA compatible controller:" in line:
                gpu_names.append(line.split(":", 2)[2].strip())
        return gpu_names

    else:
        raise GPULookupError("Don't know how to look up gpus on this platform.")
