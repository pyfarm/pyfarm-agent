# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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


from collections import namedtuple
from functools import partial

from pyfarm.agent.entrypoints.utility import SYSTEMID_MAX
from pyfarm.agent.entrypoints.argtypes import (
    assert_instance, ip, port, integer, direxists, number, enum,
    system_identifier)
from pyfarm.agent.testutil import TestCase, ErrorCapturingParser

DummyArgs = namedtuple("DummyArgs", ["uid"])


class BaseTestArgTypes(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.args = None
        self.parser = ErrorCapturingParser()


class TestAssertInstance(TestCase):
    def test_instance_set(self):
        @assert_instance
        def function(instance=None):
            pass

        with self.assertRaises(AssertionError):
            function(instance=None)

    def test_has_args_and_parser(self):
        @assert_instance
        def function(instance=None):
            return True

        self.parser = ErrorCapturingParser()
        self.args = self.parser.parse_args()
        args = self.args
        parser = self.parser
        del self.args
        del self.parser

        with self.assertRaises(AssertionError):
            function(instance=self)

        self.parser = parser
        with self.assertRaises(AssertionError):
            function(instance=self)

        self.args = args
        self.assertTrue(function(instance=self))


class TestIp(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)
        self.parser.add_argument("--ip", type=partial(ip, instance=self))

    def test_valid(self):
        self.args = self.parser.parse_args(["--ip", "127.0.0.1"])
        self.assertEqual(self.args.ip, "127.0.0.1")

    def test_invalid(self):
        self.args = self.parser.parse_args(["--ip", "!"])
        self.assertEqual(self.parser.errors, ["! is not a valid ip address"])


class TestPort(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)

    def add_arguments(self, add_uid=True, add_port=True):
        if add_port:
            self.parser.add_argument(
                "--port", type=partial(port, instance=self))

        if add_uid:
            self.parser.add_argument(
                "--uid", type=partial(integer, instance=self, min_=0))

    def test_uid_not_provided(self):
        self.add_arguments(add_uid=False)
        self.args = self.parser.parse_args(["--port", "49152"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.port, 49152)
        self.args = self.parser.parse_args(["--port", "0"])
        self.assertEqual(
            self.parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_non_root_min(self):
        self.add_arguments()
        self.args = DummyArgs(uid=1000)
        self.args = self.parser.parse_args(["--uid", "1000", "--port", "49152"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.port, 49152)
        self.args = self.parser.parse_args(["--uid", "1000", "--port", "49151"])
        self.assertEqual(
            self.parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_non_root_max(self):
        self.add_arguments()
        self.args = DummyArgs(uid=1000)
        self.args = self.parser.parse_args(["--uid", "1000", "--port", "65535"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.port, 65535)
        self.args = self.parser.parse_args(["--uid", "1000", "--port", "65536"])
        self.assertEqual(
            self.parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_root_min(self):
        self.add_arguments()
        self.args = DummyArgs(uid=0)
        self.args = self.parser.parse_args(["--uid", "0", "--port", "1"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.port, 1)
        self.args = self.parser.parse_args(["--uid", "0", "--port", "0"])
        self.assertEqual(
            self.parser.errors, ["valid port range is 1 to 65535"])

    def test_valid_root_max(self):
        self.add_arguments()
        self.args = DummyArgs(uid=0)
        self.args = self.parser.parse_args(["--uid", "0", "--port", "65535"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.port, 65535)
        self.args = self.parser.parse_args(["--uid", "0", "--port", "65536"])
        self.assertEqual(
            self.parser.errors, ["valid port range is 1 to 65535"])

    def test_port_not_a_number(self):
        self.add_arguments()
        self.args = DummyArgs(uid=0)
        self.args = self.parser.parse_args(["--uid", "0", "--port", "!"])
        self.assertEqual(
            self.parser.errors, ["failed to convert --port to a number"])


class TestDirectory(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)
        self.parser.add_argument(
            "--dir", type=partial(direxists, instance=self, flag="dir"))

    def test_is_file_not_directory(self):
        path = self.create_test_file()
        self.args = self.parser.parse_args(["--dir", path])
        self.assertEqual(
            self.parser.errors,
            ["--dir, path does not exist or is not a directory: %s" % path])

    def test_directory_exists(self):
        directory, files = self.create_test_directory()
        self.args = self.parser.parse_args(["--dir", directory])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.dir, directory)


class TestNumber(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)
        self.parser.add_argument(
            "--num",
            type=partial(
                number, instance=self, types=int, flag="num"))
        self.parser.add_argument(
            "--inf",
            type=partial(
                number, instance=self, types=int, allow_inf=True, flag="inf"))

    def test_auto(self):
        self.args = self.parser.parse_args(["--num", "auto"])
        self.assertEqual(self.args.num, "auto")

    def test_infinite(self):
        self.args = self.parser.parse_args(["--inf", "infinite"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.inf, float("inf"))

    def test_infinite_not_allowed(self):
        self.args = self.parser.parse_args(["--num", "infinite"])
        self.assertIn(
            "--num does not allow an infinite value", self.parser.errors)

    def test_unable_to_parse(self):
        self.args = self.parser.parse_args(["--num", "!"])
        self.assertEqual(
            self.parser.errors, ["--num failed to convert '!' to a number"])

    def test_not_a_number(self):
        self.args = self.parser.parse_args(["--num", "a"])
        self.assertEqual(
            self.parser.errors,
            ["--num, 'a' is not an instance of <type 'int'>"])

    def test_less_than_minimum(self):
        self.args = self.parser.parse_args(["--num", "0"])
        self.assertEqual(
            self.parser.errors,
            ["--num's value must be greater than 1"])


class TestEnum(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)
        _enum = namedtuple("Enum", ["a", "b", "c"])
        self.enum = _enum(a="one", b="two", c="three")
        self.parser.add_argument(
            "--enum",
            type=partial(
                enum, instance=self, enum=self.enum, flag="enum"))

    def test_invalid(self):
        self.args = self.parser.parse_args(["--enum", "foo"])
        self.assertEqual(
            self.parser.errors,
            ["invalid enum value foo for --enum, valid values "
             "are ['one', 'two', 'three']"])

    def test_valid(self):
        self.args = self.parser.parse_args(["--enum", "one"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.enum, "one")


class TestSystemIdentifier(BaseTestArgTypes):
    def setUp(self):
        BaseTestArgTypes.setUp(self)
        self.parser.add_argument(
            "--systemid",
            type=partial(system_identifier, instance=self))

    def test_auto(self):
        self.args = self.parser.parse_args(["--systemid", "auto"])
        self.assertEqual(self.parser.errors, [])
        self.assertEqual(self.args.systemid, "auto")

    def test_unable_to_parse(self):
        self.args = self.parser.parse_args(["--systemid", "!"])
        self.assertEqual(
            self.parser.errors,
            ["failed to convert value provided to --systemid to an integer"])

    def test_less_than_zero(self):
        self.args = self.parser.parse_args(["--systemid", "-1"])
        self.assertEqual(
            self.parser.errors,
            ["valid range for --systemid is 0 to 281474976710655"])

    def test_greater_than_max(self):
        systemid = SYSTEMID_MAX + 1
        self.args = self.parser.parse_args(["--systemid", str(systemid)])
        self.assertEqual(
            self.parser.errors,
            ["valid range for --systemid is 0 to 281474976710655"])
