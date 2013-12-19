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
import warnings

if sys.version_info[0:2] < (2, 7):
    from unittest2 import TestCase
else:
    from unittest import TestCase

from pyfarm.core import enums
from pyfarm.core.enums import (
    OS, WorkState, AgentState, OperatingSystem, UseAgentAddress,
    JobTypeLoadMode, get_operating_system, _WorkState, _AgentState,
    _OperatingSystem, _UseAgentAddress, _JobTypeLoadMode, DBUseAgentAddress,
    DBAgentState, DBOperatingSystem, DBWorkState, DBJobTypeLoadMode, Enum,
    EnumValue, cast_enum)
from pyfarm.core.warning import NotImplementedWarning


class TestEnums(TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", NotImplementedWarning)

    def tearDown(self):
        warnings.simplefilter("always", NotImplementedWarning)

    def test_values(self):
        # NOTE: The test below is auto generated with the below code.
        #       Generally speaking the current test should be manually
        #       updated in the future.
        # from pyfarm.core import enums
        #
        # for name, value in vars(enums).iteritems():
        #     if name.startswith("__"):
        #         continue
        #
        #     if name.startswith("_"):
        #         for k, v in value._asdict().iteritems():
        #             print "self.assertEqual(%s.%s.int, %s)" % (
        #                 name, k, v.int)
        #             print "self.assertEqual(%s.%s.str, \"%s\")" % (
        #                 name, k, v.str)
        #             print "self.assertEqual(%s.%s, \"%s\")" % (
        #                 name[1:], k, v.str)
        #             print "self.assertEqual(%s.%s, %s)" % (
        #                 "DB"+name[1:], k, v.int)
        self.assertEqual(_UseAgentAddress.PASSIVE.int, 313)
        self.assertEqual(_UseAgentAddress.PASSIVE.str, "passive")
        self.assertEqual(UseAgentAddress.PASSIVE, "passive")
        self.assertEqual(DBUseAgentAddress.PASSIVE, 313)
        self.assertEqual(_UseAgentAddress.REMOTE.int, 311)
        self.assertEqual(_UseAgentAddress.REMOTE.str, "remote")
        self.assertEqual(UseAgentAddress.REMOTE, "remote")
        self.assertEqual(DBUseAgentAddress.REMOTE, 311)
        self.assertEqual(_UseAgentAddress.HOSTNAME.int, 312)
        self.assertEqual(_UseAgentAddress.HOSTNAME.str, "hostname")
        self.assertEqual(UseAgentAddress.HOSTNAME, "hostname")
        self.assertEqual(DBUseAgentAddress.HOSTNAME, 312)
        self.assertEqual(_UseAgentAddress.LOCAL.int, 310)
        self.assertEqual(_UseAgentAddress.LOCAL.str, "local")
        self.assertEqual(UseAgentAddress.LOCAL, "local")
        self.assertEqual(DBUseAgentAddress.LOCAL, 310)
        self.assertEqual(_AgentState.DISABLED.int, 200)
        self.assertEqual(_AgentState.DISABLED.str, "disabled")
        self.assertEqual(AgentState.DISABLED, "disabled")
        self.assertEqual(DBAgentState.DISABLED, 200)
        self.assertEqual(_AgentState.OFFLINE.int, 201)
        self.assertEqual(_AgentState.OFFLINE.str, "offline")
        self.assertEqual(AgentState.OFFLINE, "offline")
        self.assertEqual(DBAgentState.OFFLINE, 201)
        self.assertEqual(_AgentState.RUNNING.int, 203)
        self.assertEqual(_AgentState.RUNNING.str, "running")
        self.assertEqual(AgentState.RUNNING, "running")
        self.assertEqual(DBAgentState.RUNNING, 203)
        self.assertEqual(_AgentState.ONLINE.int, 202)
        self.assertEqual(_AgentState.ONLINE.str, "online")
        self.assertEqual(AgentState.ONLINE, "online")
        self.assertEqual(DBAgentState.ONLINE, 202)
        self.assertEqual(_OperatingSystem.WINDOWS.int, 301)
        self.assertEqual(_OperatingSystem.WINDOWS.str, "windows")
        self.assertEqual(OperatingSystem.WINDOWS, "windows")
        self.assertEqual(DBOperatingSystem.WINDOWS, 301)
        self.assertEqual(_OperatingSystem.MAC.int, 302)
        self.assertEqual(_OperatingSystem.MAC.str, "mac")
        self.assertEqual(OperatingSystem.MAC, "mac")
        self.assertEqual(DBOperatingSystem.MAC, 302)
        self.assertEqual(_OperatingSystem.OTHER.int, 303)
        self.assertEqual(_OperatingSystem.OTHER.str, "other")
        self.assertEqual(OperatingSystem.OTHER, "other")
        self.assertEqual(DBOperatingSystem.OTHER, 303)
        self.assertEqual(_OperatingSystem.LINUX.int, 300)
        self.assertEqual(_OperatingSystem.LINUX.str, "linux")
        self.assertEqual(OperatingSystem.LINUX, "linux")
        self.assertEqual(DBOperatingSystem.LINUX, 300)
        self.assertEqual(_WorkState.ALLOC.int, 103)
        self.assertEqual(_WorkState.ALLOC.str, "alloc")
        self.assertEqual(WorkState.ALLOC, "alloc")
        self.assertEqual(DBWorkState.ALLOC, 103)
        self.assertEqual(_WorkState.JOBTYPE_INVALID_CLASS.int, 109)
        self.assertEqual(_WorkState.JOBTYPE_INVALID_CLASS.str, "jobtype_invalid_class")
        self.assertEqual(WorkState.JOBTYPE_INVALID_CLASS, "jobtype_invalid_class")
        self.assertEqual(DBWorkState.JOBTYPE_INVALID_CLASS, 109)
        self.assertEqual(_WorkState.QUEUED.int, 101)
        self.assertEqual(_WorkState.QUEUED.str, "queued")
        self.assertEqual(WorkState.QUEUED, "queued")
        self.assertEqual(DBWorkState.QUEUED, 101)
        self.assertEqual(_WorkState.PAUSED.int, 100)
        self.assertEqual(_WorkState.PAUSED.str, "paused")
        self.assertEqual(WorkState.PAUSED, "paused")
        self.assertEqual(DBWorkState.PAUSED, 100)
        self.assertEqual(_WorkState.FAILED.int, 107)
        self.assertEqual(_WorkState.FAILED.str, "failed")
        self.assertEqual(WorkState.FAILED, "failed")
        self.assertEqual(DBWorkState.FAILED, 107)
        self.assertEqual(_WorkState.RUNNING.int, 105)
        self.assertEqual(_WorkState.RUNNING.str, "running")
        self.assertEqual(WorkState.RUNNING, "running")
        self.assertEqual(DBWorkState.RUNNING, 105)
        self.assertEqual(_WorkState.DONE.int, 106)
        self.assertEqual(_WorkState.DONE.str, "done")
        self.assertEqual(WorkState.DONE, "done")
        self.assertEqual(DBWorkState.DONE, 106)
        self.assertEqual(_WorkState.JOBTYPE_FAILED_IMPORT.int, 108)
        self.assertEqual(_WorkState.JOBTYPE_FAILED_IMPORT.str, "jobtype_failed_import")
        self.assertEqual(WorkState.JOBTYPE_FAILED_IMPORT, "jobtype_failed_import")
        self.assertEqual(DBWorkState.JOBTYPE_FAILED_IMPORT, 108)
        self.assertEqual(_WorkState.NO_SUCH_COMMAND.int, 110)
        self.assertEqual(_WorkState.NO_SUCH_COMMAND.str, "no_such_command")
        self.assertEqual(WorkState.NO_SUCH_COMMAND, "no_such_command")
        self.assertEqual(DBWorkState.NO_SUCH_COMMAND, 110)
        self.assertEqual(_WorkState.ASSIGN.int, 104)
        self.assertEqual(_WorkState.ASSIGN.str, "assign")
        self.assertEqual(WorkState.ASSIGN, "assign")
        self.assertEqual(DBWorkState.ASSIGN, 104)
        self.assertEqual(_WorkState.BLOCKED.int, 102)
        self.assertEqual(_WorkState.BLOCKED.str, "blocked")
        self.assertEqual(WorkState.BLOCKED, "blocked")
        self.assertEqual(DBWorkState.BLOCKED, 102)
        self.assertEqual(_JobTypeLoadMode.DOWNLOAD.int, 320)
        self.assertEqual(_JobTypeLoadMode.DOWNLOAD.str, "download")
        self.assertEqual(JobTypeLoadMode.DOWNLOAD, "download")
        self.assertEqual(DBJobTypeLoadMode.DOWNLOAD, 320)
        self.assertEqual(_JobTypeLoadMode.IMPORT.int, 322)
        self.assertEqual(_JobTypeLoadMode.IMPORT.str, "import")
        self.assertEqual(JobTypeLoadMode.IMPORT, "import")
        self.assertEqual(DBJobTypeLoadMode.IMPORT, 322)
        self.assertEqual(_JobTypeLoadMode.OPEN.int, 321)
        self.assertEqual(_JobTypeLoadMode.OPEN.str, "open")
        self.assertEqual(JobTypeLoadMode.OPEN, "open")
        self.assertEqual(DBJobTypeLoadMode.OPEN, 321)

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

    def test_cast_enum(self):
        e = Enum("e", A=EnumValue(-sys.maxint, "A"))
        self.assertEqual(e.A.int, -sys.maxint)
        self.assertEqual(e.A.str, "A")
        s = cast_enum(e, str)
        self.assertEqual(s.A, "A")
        i = cast_enum(e, int)
        self.assertEqual(i.A, -sys.maxint)
        self.assertEqual(i._map, {"A": -sys.maxint, -sys.maxint: "A"})
