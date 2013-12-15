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

import sys
import inspect
import warnings

if sys.version_info[0:2] < (2, 7):
    from unittest2 import TestCase
else:
    from unittest import TestCase

from pyfarm.core import enums
from pyfarm.core.enums import (
    OS, WorkState, AgentState, OperatingSystem, UseAgentAddress,
    JobTypeLoadMode, get_operating_system)
from pyfarm.core.warning import NotImplementedWarning


class TestEnums(TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", NotImplementedWarning)

    def tearDown(self):
        warnings.simplefilter("always", NotImplementedWarning)

    def test_duplicate_enum_values(self):
        values = []
        for name, value in vars(enums).iteritems():
            if name.startswith("_") or name == "APIErrorValue":
                continue

            if hasattr(value, "_asdict") and not name.startswith("_"):
                values.extend(value._asdict().values())

        self.assertEqual(
            len(set(values)), len(values), "duplicate enum number(s) found")

    def test_direct_work_values(self):
        self.assertEqual(WorkState.PAUSED, 100)
        self.assertEqual(WorkState.QUEUED, 101)
        self.assertEqual(WorkState.BLOCKED, 102)
        self.assertEqual(WorkState.ALLOC, 103)
        self.assertEqual(WorkState.ASSIGN, 104)
        self.assertEqual(WorkState.RUNNING, 105)
        self.assertEqual(WorkState.DONE, 106)
        self.assertEqual(WorkState.FAILED, 107)
        self.assertEqual(WorkState.JOBTYPE_FAILED_IMPORT, 108)
        self.assertEqual(WorkState.JOBTYPE_INVALID_CLASS, 109)
        self.assertEqual(WorkState.NO_SUCH_COMMAND, 110)

        # make sure there are not any new or missing values
        self.assertEqual(sorted(WorkState), sorted(range(100, 110+1)))

    def test_direct_agent_values(self):
        self.assertEqual(AgentState.DISABLED, 200)
        self.assertEqual(AgentState.OFFLINE, 201)
        self.assertEqual(AgentState.ONLINE, 202)
        self.assertEqual(AgentState.RUNNING, 203)

        # make sure there are not any new or missing values
        self.assertEqual(sorted(AgentState), sorted(range(200, 203+1)))

    def test_direct_os_values(self):
        self.assertEqual(OperatingSystem.LINUX, 300)
        self.assertEqual(OperatingSystem.WINDOWS, 301)
        self.assertEqual(OperatingSystem.MAC, 302)
        self.assertEqual(OperatingSystem.OTHER, 303)

        # make sure there are not any new or missing values
        self.assertEqual(sorted(OperatingSystem), sorted(range(300, 303+1)))

    def test_direct_agent_addr(self):
        self.assertEqual(UseAgentAddress.LOCAL, 310)
        self.assertEqual(UseAgentAddress.REMOTE, 311)
        self.assertEqual(UseAgentAddress.HOSTNAME, 312)
        self.assertEqual(UseAgentAddress.PASSIVE, 313)

        # make sure there are not any new or missing values
        self.assertEqual(sorted(UseAgentAddress), sorted(range(310, 313+1)))

    def test_direct_jobtype_load_values(self):
        self.assertEqual(JobTypeLoadMode.DOWNLOAD, 320)
        self.assertEqual(JobTypeLoadMode.OPEN, 321)
        self.assertEqual(JobTypeLoadMode.IMPORT, 322)

        # make sure there are not any new or missing values
        self.assertEqual(sorted(JobTypeLoadMode), sorted(range(320, 322+1)))

    def test_os_value(self):
        self.assertEqual(get_operating_system(), OS)

    def test_getOs(self):
        self.assertEqual(
            enums.get_operating_system("linux"), enums.OperatingSystem.LINUX)
        self.assertEqual(
            enums.get_operating_system("win"), enums.OperatingSystem.WINDOWS)
        self.assertEqual(
            enums.get_operating_system("darwin"), enums.OperatingSystem.MAC)
        self.assertEqual(
            enums.get_operating_system("FOO"), enums.OperatingSystem.OTHER)
