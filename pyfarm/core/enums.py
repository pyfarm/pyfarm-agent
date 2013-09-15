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
Enums
=====

Provides enum values for certain aspect of PyFarm.  See below for more
detailed information.

.. csv-table:: **OperatingSystem**
    :header: Attribute, Description
    :widths: 10, 50

    LINUX, operating system on agent is a Linux variant
    WINDOWS, operating system on agent is a Windows variant
    MAC, operating system on agent is an Apple OS variant


.. csv-table:: **JobTypeLoadMode**
    :header: Attribute, Description
    :widths: 10, 50

    DOWNLOAD, download the jobtype file from a url
    OPEN, open the jobtype file from a url
    IMPORT, import the jobtype from the given string (ex. `foo.bar.ClassName`)


.. csv-table:: **AgentState**
    :header: Attribute, Description
    :widths: 10, 50

    OFFLINE, agent cannot be reached
    ONLINE, agent is waiting for work
    DISABLED, agent is online but cannot accept work
    RUNNING, agent is currently processing work
    ALLOC, special internal state used when the agent entry is being built


.. csv-table:: **WorkState**
    :header: Attribute, Description
    :widths: 10, 50

    PAUSED, this task cannot be assigned right now but can be once unpaused
    BLOCKED, this task cannot be assigned to an agent at this point in time
    QUEUED, waiting on queue to assign this work
    ASSIGN, work has been assigned to an agent but is waiting to start
    RUNNING, work is currently being processed
    DONE, work is finished (previous failures may be present)
    FAILED, work as failed and cannot be continued
    ALLOC, special internal state for a job or task entry is being built
"""

import sys
from warnings import warn

try:
    from collections import namedtuple
except ImportError:  # pragma: no cover
    from pyfarm.core.backports import namedtuple

from pyfarm.core.warning import NotImplementedWarning


class _OperatingSystem(namedtuple(
    "OperatingSystem",
    ["LINUX", "WINDOWS", "MAC", "OTHER"])):
    """base class for OperatingSystem"""

class _JobTypeLoadMode(namedtuple(
    "JobTypeLoadMode",
    ["DOWNLOAD", "OPEN", "IMPORT"])):
        """base class for JobTypeLoadMode"""

class _AgentState(namedtuple(
    "AgentState",
    ["OFFLINE", "ONLINE", "DISABLED", "RUNNING", "ALLOC"])):
        """base class for AgentState"""

class _WorkState(namedtuple(
    "WorkState",
    ["PAUSED", "BLOCKED", "QUEUED", "ASSIGN", "RUNNING", "DONE",
     "FAILED", "ALLOC"])):
    """base class for WorkState"""


OperatingSystem = _OperatingSystem(
    LINUX=0, WINDOWS=1, MAC=2, OTHER=3)

JobTypeLoadMode = _JobTypeLoadMode(
    DOWNLOAD=4, OPEN=5, IMPORT=6)

AgentState = _AgentState(
    OFFLINE=7, ONLINE=8, DISABLED=9, RUNNING=10, ALLOC=11)

WorkState = _WorkState(
    PAUSED=12, BLOCKED=13, QUEUED=14, ASSIGN=15, RUNNING=16,
    DONE=17, FAILED=18, ALLOC=19)


def _getOS(platform=sys.platform):
    """returns the operating system for the given platform"""
    if platform.startswith("linux"):
        return OperatingSystem.LINUX
    elif platform.startswith("win"):
        return OperatingSystem.WINDOWS
    elif platform.startswith("darwin"):
        return OperatingSystem.MAC
    else:
        warn("unknown operating system: %s" % platform, NotImplementedWarning)
        return OperatingSystem.OTHER


OS = _getOS()
