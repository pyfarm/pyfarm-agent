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

try:
    from collections import namedtuple as _namedtuple
except ImportError:
    from pyfarm.core.backports import namedtuple as _namedtuple

# base class setup
_OperatingSystem = _namedtuple(
    "OperatingSystem",
    ["LINUX", "WINDOWS", "MAC", "OTHER"])
_WorkState = _namedtuple(
    "WorkState",
    ["PAUSED", "BLOCKED", "QUEUED", "ASSIGN", "RUNNING", "DONE", "FAILED"])
_AgentState = _namedtuple(
    "AgentState",
    ["OFFLINE", "ONLINE", "DISABLED", "RUNNING"])

# instance and apply values
OperatingSystem = _OperatingSystem(
    LINUX=0, WINDOWS=1, MAC=2, OTHER=3)
WorkState = _WorkState(
    PAUSED=4, BLOCKED=5, QUEUED=6, ASSIGN=7, RUNNING=8, DONE=9, FAILED=10)
AgentState = _AgentState(
    OFFLINE=11, ONLINE=12, DISABLED=13, RUNNING=14)


def checkForDuplicates():
    values = []
    for name, value in globals().iteritems():
        if hasattr(value, "_asdict") and not name.startswith("_"):
            values.extend(value._asdict().values())

    assert len(values) == len(set(values)), "repeated numbers found"

# check for programmer error
checkForDuplicates()
del checkForDuplicates