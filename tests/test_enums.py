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

from pyfarm.core.enums import PY26

if PY26:
    from unittest2 import TestCase, skipUnless
else:
    from unittest import TestCase, skipUnless

from pyfarm.core.sysinfo.system import operating_system
from pyfarm.core.enums import (
    OS, WorkState, AgentState, OperatingSystem, UseAgentAddress,
    _WorkState, _AgentState, STRING_TYPES, NUMERIC_TYPES,
    PY2, PY3, PY27, PY_MAJOR, PY_MINOR, PY_VERSION,
    _OperatingSystem, _UseAgentAddress, DBUseAgentAddress,
    DBAgentState, DBOperatingSystem, DBWorkState, Enum,
    Values, cast_enum, LINUX, MAC, WINDOWS, POSIX, BOOLEAN_TRUE, BOOLEAN_FALSE)


class TestEnums(TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", UserWarning)
        Values.check_uniqueness = True

    def tearDown(self):
        warnings.simplefilter("always", UserWarning)

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
        #             print "        self.assertEqual(\n      " \
        #                   "      %s.%s.int, %s)" % (
        #                 name, k, v.int)
        #             print "        self.assertEqual(\n      " \
        #                   "      %s.%s.str, \"%s\")" % (
        #                 name, k, v.str)
        #             print "        self.assertEqual(\n      " \
        #                   "      %s.%s, \"%s\")" % (
        #                 name[1:], k, v.str)
        #             print "        self.assertEqual(\n      " \
        #                   "      %s.%s, %s)" % (
        #                 "DB"+name[1:], k, v.int)

        self.assertEqual(
            _UseAgentAddress.PASSIVE.int, 313)
        self.assertEqual(
            _UseAgentAddress.PASSIVE.str, "passive")
        self.assertEqual(
            UseAgentAddress.PASSIVE, "passive")
        self.assertEqual(
            DBUseAgentAddress.PASSIVE, 313)
        self.assertEqual(
            _UseAgentAddress.REMOTE.int, 311)
        self.assertEqual(
            _UseAgentAddress.REMOTE.str, "remote")
        self.assertEqual(
            UseAgentAddress.REMOTE, "remote")
        self.assertEqual(
            DBUseAgentAddress.REMOTE, 311)
        self.assertEqual(
            _UseAgentAddress.HOSTNAME.int, 312)
        self.assertEqual(
            _UseAgentAddress.HOSTNAME.str, "hostname")
        self.assertEqual(
            UseAgentAddress.HOSTNAME, "hostname")
        self.assertEqual(
            DBUseAgentAddress.HOSTNAME, 312)
        self.assertEqual(
            _UseAgentAddress.LOCAL.int, 310)
        self.assertEqual(
            _UseAgentAddress.LOCAL.str, "local")
        self.assertEqual(
            UseAgentAddress.LOCAL, "local")
        self.assertEqual(
            DBUseAgentAddress.LOCAL, 310)
        self.assertEqual(
            _AgentState.DISABLED.int, 200)
        self.assertEqual(
            _AgentState.DISABLED.str, "disabled")
        self.assertEqual(
            AgentState.DISABLED, "disabled")
        self.assertEqual(
            DBAgentState.DISABLED, 200)
        self.assertEqual(
            _AgentState.OFFLINE.int, 201)
        self.assertEqual(
            _AgentState.OFFLINE.str, "offline")
        self.assertEqual(
            AgentState.OFFLINE, "offline")
        self.assertEqual(
            DBAgentState.OFFLINE, 201)
        self.assertEqual(
            _AgentState.RUNNING.int, 203)
        self.assertEqual(
            _AgentState.RUNNING.str, "running")
        self.assertEqual(
            AgentState.RUNNING, "running")
        self.assertEqual(
            DBAgentState.RUNNING, 203)
        self.assertEqual(
            _AgentState.ONLINE.int, 202)
        self.assertEqual(
            _AgentState.ONLINE.str, "online")
        self.assertEqual(
            AgentState.ONLINE, "online")
        self.assertEqual(
            DBAgentState.ONLINE, 202)
        self.assertEqual(
            _OperatingSystem.WINDOWS.int, 301)
        self.assertEqual(
            _OperatingSystem.WINDOWS.str, "windows")
        self.assertEqual(
            OperatingSystem.WINDOWS, "windows")
        self.assertEqual(
            DBOperatingSystem.WINDOWS, 301)
        self.assertEqual(
            _OperatingSystem.MAC.int, 302)
        self.assertEqual(
            _OperatingSystem.MAC.str, "mac")
        self.assertEqual(
            OperatingSystem.MAC, "mac")
        self.assertEqual(
            DBOperatingSystem.MAC, 302)
        self.assertEqual(
            _OperatingSystem.OTHER.int, 303)
        self.assertEqual(
            _OperatingSystem.OTHER.str, "other")
        self.assertEqual(
            OperatingSystem.OTHER, "other")
        self.assertEqual(
            DBOperatingSystem.OTHER, 303)
        self.assertEqual(
            _OperatingSystem.LINUX.int, 300)
        self.assertEqual(
            _OperatingSystem.LINUX.str, "linux")
        self.assertEqual(
            OperatingSystem.LINUX, "linux")
        self.assertEqual(
            DBOperatingSystem.LINUX, 300)
        self.assertEqual(
            _WorkState.FAILED.int, 107)
        self.assertEqual(
            _WorkState.FAILED.str, "failed")
        self.assertEqual(
            WorkState.FAILED, "failed")
        self.assertEqual(
            DBWorkState.FAILED, 107)
        self.assertEqual(
            _WorkState.RUNNING.int, 105)
        self.assertEqual(
            _WorkState.RUNNING.str, "running")
        self.assertEqual(
            WorkState.RUNNING, "running")
        self.assertEqual(
            DBWorkState.RUNNING, 105)
        self.assertEqual(
            _WorkState.DONE.int, 106)
        self.assertEqual(
            _WorkState.DONE.str, "done")
        self.assertEqual(
            WorkState.DONE, "done")
        self.assertEqual(
            DBWorkState.DONE, 106)


    @skipUnless(sys.platform.startswith("win"), "Not windows")
    def test_windows(self):
        self.assertTrue(WINDOWS)

    @skipUnless(sys.platform.startswith("darwin"), "Not Mac")
    def test_mac(self):
        self.assertTrue(MAC)

    @skipUnless(sys.platform.startswith("linux"), "Not Linux")
    def test_linux(self):
        self.assertTrue(LINUX)

    def test_os(self):
        self.assertEqual(operating_system("linux"), OperatingSystem.LINUX)
        self.assertEqual(operating_system("win"), OperatingSystem.WINDOWS)
        self.assertEqual(operating_system("darwin"), OperatingSystem.MAC)
        self.assertEqual(operating_system("FOO"), OperatingSystem.OTHER)
        self.assertEqual(OS, operating_system())
        self.assertEqual(LINUX, OS == OperatingSystem.LINUX)
        self.assertEqual(MAC, OS == OperatingSystem.MAC)
        self.assertEqual(WINDOWS, OS == OperatingSystem.WINDOWS)
        self.assertEqual(POSIX,
                         OS in (OperatingSystem.LINUX, OperatingSystem.MAC))

    def test_cast_enum(self):
        Values.check_uniqueness = False
        e = Enum("e", A=Values(-4242, "A"))
        self.assertEqual(e.A.int, -4242)
        self.assertEqual(e.A.str, "A")
        s = cast_enum(e, str)
        self.assertEqual(s.A, "A")
        i = cast_enum(e, int)
        self.assertEqual(i.A, -4242)
        self.assertEqual(i._map, {"A": -4242, -4242: "A"})

        with self.assertRaises(TypeError):
            cast_enum(e, None)


class TestPythonVersion(TestCase):
    @skipUnless(sys.version_info[0:2] == (2, 6), "Not Python 2.6")
    def test_py26(self):
        self.assertTrue(PY26)

    @skipUnless(sys.version_info[0:2] == (2, 7), "Not Python 2.7")
    def test_py27(self):
        self.assertTrue(PY27)

    @skipUnless(sys.version_info[0] == 2, "Not Python 2")
    def test_py2(self):
        self.assertTrue(PY2)

    @skipUnless(sys.version_info[0] == 3, "Not Python 3")
    def test_py3(self):
        self.assertTrue(PY3)

    def test_py_version(self):
        self.assertEqual(sys.version_info[0:2], PY_VERSION)

    def test_py_major(self):
        self.assertEqual(sys.version_info[0], PY_MAJOR)

    def test_py_minor(self):
        self.assertEqual(sys.version_info[1], PY_MINOR)


class TestPythonTypes(TestCase):
    @skipUnless(sys.version_info[0] == 2, "Not Python 2")
    def test_string_types_py2(self):
        self.assertEqual(STRING_TYPES, (str, unicode))

    @skipUnless(sys.version_info[0] == 2, "Not Python 2")
    def test_numeric_types_py2(self):
        self.assertEqual(NUMERIC_TYPES, (int, long, float, complex))

    @skipUnless(sys.version_info[0] == 3, "Not Python 3")
    def test_string_types_py3(self):
        self.assertEqual(STRING_TYPES, (str, ))

    @skipUnless(sys.version_info[0] == 3, "Not Python 3")
    def test_numeric_types_py3(self):
        self.assertEqual(NUMERIC_TYPES, (int, float, complex))


class TestBooleanTypes(TestCase):
    def test_true(self):
        self.assertEqual(
            BOOLEAN_TRUE, set(["1", "t", "y", "true", "yes", True, 1]))

    def test_false(self):
        self.assertEqual(
            BOOLEAN_FALSE, set(["0", "f", "n", "false", "no", False, 0]))


class TestEnumValueClass(TestCase):
    def setUp(self):
        Values.check_uniqueness = False

    def test_to_dict(self):
        foobar = Enum("foobar", A=1, to_dict=lambda: None, instance=False)
        self.assertTrue(hasattr(foobar, "to_dict"))

    def test_hash(self):
        v = Values(int=1, str="foo")
        self.assertEqual(hash(v), hash(v.str))

    def test_input_types(self):
        with self.assertRaises(TypeError):
            Values(int="foo", str="foo")

        with self.assertRaises(TypeError):
            Values(int=1, str=1)

    def test_unique_values(self):
        Values(1, "A")
        Values.check_uniqueness = True
        with self.assertRaises(ValueError):
            Values(1, "A")

    def test_equal(self):
        self.assertEqual(Values(1, "A"), Values(1, "A"))
        self.assertEqual(Values(1, "A"), "A")
        self.assertEqual(Values(1, "A"), 1)

    def test_not_equal(self):
        self.assertNotEqual(Values(1, "A"), Values(2, "B"))
        self.assertNotEqual(Values(1, "A"), "B")
        self.assertNotEqual(Values(1, "A"), 2)

    def test_greater(self):
        self.assertGreater(2, Values(1, "A"))
        self.assertGreater(Values(2, "A"), Values(1, "A"))

        with self.assertRaises(NotImplementedError):
            self.assertGreater("", Values(1, "A"))

    def test_greater_equal(self):
        self.assertGreaterEqual(1, Values(1, "1"))
        self.assertGreaterEqual(2, Values(1, "A"))
        self.assertGreaterEqual(Values(2, "A"), Values(1, "A"))

        with self.assertRaises(NotImplementedError):
            self.assertGreaterEqual("", Values(1, "1"))

    def test_less(self):
        self.assertLess(0, Values(1, "A"))
        self.assertLess(Values(0, "A"), Values(1, "A"))

        with self.assertRaises(NotImplementedError):
            self.assertLess("", Values(1, "A"))

    def test_less_equal(self):
        self.assertLessEqual(0, Values(1, "A"))
        self.assertLessEqual(1, Values(1, "A"))
        self.assertLessEqual(Values(1, "A"), Values(1, "A"))

        with self.assertRaises(NotImplementedError):
            self.assertLessEqual("", Values(1, "A"))

    def test_contains(self):
        self.assertIn(1, Values(1, "A"))
        self.assertIn("A", Values(1, "A"))
        self.assertIn(Values(1, "A"), Values(1, "A"))

    def test_convert_int(self):
        self.assertEqual(int(Values(1, "A")), 1)

    def test_convert_str(self):
        self.assertEqual(str(Values(1, "A")), "A")
