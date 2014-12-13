# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
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
CPU
---

Contains information about the cpu and its relation to the operating
system such as load, processing times, etc.
"""

from __future__ import division

import platform
from ctypes import cdll, create_string_buffer, byref, c_uint
from ctypes.util import find_library

import psutil

from pyfarm.core.enums import WINDOWS, LINUX, MAC
from pyfarm.agent.logger import getLogger

try:
    from wmi import WMI
except ImportError:  # pragma: no cover
    WMI = NotImplemented

try:
    LIBC = cdll.LoadLibrary(find_library("c"))
except OSError:  # pragma: no cover
    LIBC = NotImplemented

logger = getLogger("agent.cpu")


def cpu_name():
    """
    Returns the full name of the CPU installed in the system.
    """
    if WINDOWS:
        wmi = WMI()
        processor = wmi.Win32_Processor()[0]
        return processor.name

    elif LINUX:
        with open("/proc/cpuinfo", "r") as cpuinfo:
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()

    elif MAC:
        # Try a couple of different sysctl names.  On platforms
        # such as OS X the first entry will work.  On all platforms
        # the second name should work in every other case.  For some
        # systems the second entry will in fact contain information about the
        # processor but in others it could just be something generic like
        # "MacBookPro".
        for ctlname in ("machdep.cpu.brand_string", "hw.model"):
            uint = c_uint(0)
            result = LIBC.sysctlbyname(ctlname, None, byref(uint), None, 0)

            if result != 0:
                logger.warning(
                    "sysctlbyname(%s) failed, result was %s", ctlname, result)
                continue

            string_buffer = create_string_buffer(uint.value)
            result = LIBC.sysctlbyname(
                ctlname, string_buffer, byref(uint), None, 0)

            if result != 0:
                logger.warning(
                    "sysctlbyname(%s) failed, result was %s", ctlname, result)
                continue

            return string_buffer.value

        else:
            logger.error(
                "sysctlbyname() failed to return a non-zero result.  Falling "
                "back on platform.processor()")

    return platform.processor()


def total_cpus(logical=True):
    """
    Returns the total number of cpus installed on the system.

    :param bool logical:
        If True the return the number of cores the system has.  Setting
        this value to False will instead return the number of physical
        cpus present on the system.
    """
    return psutil.cpu_count(logical=logical)


def load(interval=1):
    """
    Returns the load across all cpus value from zero to one.  A value
    of 1.0 means the average load across all cpus is 100%.
    """
    return psutil.cpu_percent(interval) / total_cpus()


def user_time():
    """
    Returns the amount of time spent by the cpu in user
    space
    """
    return psutil.cpu_times().user


def system_time():
    """
    Returns the amount of time spent by the cpu in system
    space
    """
    return psutil.cpu_times().system


def idle_time():
    """
    Returns the amount of time spent by the cpu in idle
    space
    """
    return psutil.cpu_times().idle


def iowait():
    """
    Returns the amount of time spent by the cpu waiting
    on io

    .. note::
        on platforms other than linux this will return None
    """
    try:
        cpu_times = psutil.cpu_times()
        if hasattr(cpu_times, "iowait"):
            return psutil.cpu_times().iowait
    except AttributeError:  # pragma: no cover
        return None