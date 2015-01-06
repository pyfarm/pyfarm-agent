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
Graphics
--------

Contains information about the installed graphics cards
"""

from itertools import izip
from subprocess import Popen, PIPE

try:
    from xml.etree.cElementTree import ElementTree
except ImportError:  # pragma: no cover
    from xml.etree.ElementTree import ElementTree

try:
    from wmi import WMI
except ImportError:  # pragma: no cover
    WMI = NotImplemented

from pyfarm.core.enums import WINDOWS, LINUX, MAC
from pyfarm.agent.logger import getLogger
from pyfarm.agent.config import config

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
        gpus = wmi.Win32_VideoController.query()
        gpu_names = [x.name for x in gpus]
        return gpu_names

    elif LINUX:
        gpu_names = []
        for lspci_command in config["sysinfo_command_lspci"]:
            try:
                lspci_pipe = Popen(lspci_command.split(" "), stdout=PIPE)

                for line in lspci_pipe.stdout:
                    if "VGA compatible controller:" in line:
                        gpu_names.append(line.split(":", 2)[2].strip())
                break

            except (ValueError, OSError) as e:
                logger.debug("Failed to call %r", lspci_command)
                continue

        else:
            logger.warning("Could not run lspci to find graphics card data. "
                           "Consider installing pci-utils.")
            raise GPULookupError("Failed to locate the lspci command")

        return gpu_names

    elif MAC:
        try:
            profiler_pipe = Popen(
                ["system_profiler", "-xml", "SPDisplaysDataType"], stdout=PIPE)
        except (ValueError, OSError) as e:
            logger.warning("Could not run system_profiler to find graphics "
                           "card data. Error was %r.", e)
            raise GPULookupError("Failed to execute `system_profiler`")

        gpu_names = []
        tree = ElementTree()
        root = tree.parse(profiler_pipe.stdout)

        for element in root.findall("array/dict/array/dict"):
            iter_element = iter(element)
            for key, string in izip(iter_element, iter_element):
                if key.text == "sppci_model":
                    gpu_names.append(string.text)

        return gpu_names
    else:
        raise GPULookupError("Don't know how to look up gpus on this platform.")
