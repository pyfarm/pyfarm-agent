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
from os import urandom

from pyfarm.agent.entrypoints.parser import (
    assert_parser, ip, port, direxists, number, enum)
from pyfarm.agent.testutil import TestCase, ErrorCapturingParser


class ArgTestCase(TestCase):
    def addarg(self, parser, *args, **kwargs):
        kwargs.setdefault("help", "<no help>")
        kwargs.setdefault("config", False)
        return parser.add_argument(*args, **kwargs)


class TestAssertInstance(TestCase):
    def test_instance_set(self):
        @assert_parser
        def function(instance=None):
            pass

        with self.assertRaises(AssertionError):
            function(instance=None)


class TestIp(ArgTestCase):
    def test_valid(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--ip", type=ip)
        args = parser.parse_args(["--ip", "127.0.0.1"])
        self.assertEqual(args.ip, "127.0.0.1")

    def test_invalid(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--ip", type=ip)
        parser.parse_args(["--ip", "!"])
        self.assertEqual(
            parser.errors, ["! is not a valid ip address for --ip"])


class TestPort(ArgTestCase):
    def add_arguments(self, parser, add_uid=True, add_port=True, uid=0):
        if add_port:
            self.addarg(parser, "--port",
                        type=port, type_kwargs=dict(get_uid=lambda: uid))

        if add_uid:
            self.addarg(parser, "--uid",
                        type=int, type_kwargs=dict(min_=0))

    def test_uid_not_provided(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, add_uid=False, uid=1000)
        args = parser.parse_args(["--port", "49152"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.port, 49152)
        parser.parse_args(["--port", "0"])
        self.assertEqual(
            parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_non_root_min(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, uid=1000)
        args = parser.parse_args(["--uid", "1000", "--port", "49152"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.port, 49152)
        parser.parse_args(["--uid", "1000", "--port", "49151"])
        self.assertEqual(
            parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_non_root_max(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, uid=1000)
        args = parser.parse_args(["--uid", "1000", "--port", "65535"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.port, 65535)
        parser.parse_args(["--uid", "1000", "--port", "65536"])
        self.assertEqual(
            parser.errors, ["valid port range is 49152 to 65535"])

    def test_valid_root_min(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, uid=0)
        args = parser.parse_args(["--uid", "0", "--port", "1"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.port, 1)
        parser.parse_args(["--uid", "0", "--port", "0"])
        self.assertEqual(
            parser.errors, ["valid port range is 1 to 65535"])

    def test_valid_root_max(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, uid=0)
        args = parser.parse_args(["--uid", "0", "--port", "65535"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.port, 65535)
        parser.parse_args(["--uid", "0", "--port", "65536"])
        self.assertEqual(
            parser.errors, ["valid port range is 1 to 65535"])

    def test_port_not_a_number(self):
        parser = ErrorCapturingParser()
        self.add_arguments(parser, uid=0)
        parser.parse_args(["--uid", "0", "--port", "!"])
        self.assertEqual(
            parser.errors, ["--port requires a number"])


class TestDirectory(ArgTestCase):
    def test_directory_exists(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--dir", type=direxists)
        directory, _ = self.create_test_directory(count=1)
        args = parser.parse_args(["--dir", directory])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.dir, directory)

    def test_directory_missing(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--dir", type=direxists)
        directory = urandom(16).encode("hex")
        parser.parse_args(["--dir", directory])
        self.assertEqual(
            parser.errors,
            ["--dir, path does not exist or is "
             "not a directory: %s" % directory])


class TestNumber(ArgTestCase):
    def test_auto(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--num", type=number)
        args = parser.parse_args(["--num", "auto"])
        self.assertEqual(args.num, "auto")

    def test_infinite(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--num",
                    type=number, type_kwargs=dict(allow_inf=True))
        args = parser.parse_args(["--num", "infinite"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.num, float("inf"))

    def test_infinite_not_allowed(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--num",
                    type=number, type_kwargs=dict(allow_inf=False))
        parser.parse_args(["--num", "infinite"])
        self.assertIn(
            "--num does not allow an infinite value", parser.errors)

    def test_unable_to_parse(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--num", type=number)
        parser.parse_args(["--num", "!"])
        self.assertEqual(
            parser.errors, ["--num failed to convert '!' to a number"])

    def test_less_than_minimum(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--num", type=number, type_kwargs=dict(min_=1))
        parser.parse_args(["--num", "0"])
        self.assertEqual(
            parser.errors,
            ["--num's value must be greater than 1"])


class TestInteger(ArgTestCase):
    def test_disallow_float(self):
        parser = ErrorCapturingParser()
        self.addarg(parser, "--int", type=int)
        parser.parse_args(["--int", "3.14159"])
        self.assertEqual(
            parser.errors,
            ["--int, '3.14159' is not an instance of <type 'int'>"])


class TestEnum(ArgTestCase):
    def setUp(self):
        super(TestEnum, self).setUp()
        self._enum = namedtuple("Enum", ["a", "b", "c"])

    def test_invalid(self):
        parser = ErrorCapturingParser()
        self.addarg(
            parser,
            "--enum", type=enum,
            type_kwargs=dict(enum=self._enum(a="one", b="two", c="three")))
        parser.parse_args(["--enum", "foo"])
        self.assertEqual(
            parser.errors,
            ["invalid enum value foo for --enum, valid values "
             "are ['one', 'two', 'three']"])

    def test_valid(self):
        parser = ErrorCapturingParser()
        self.addarg(
            parser,
            "--enum", type=enum,
            type_kwargs=dict(enum=self._enum(a="one", b="two", c="three")))
        args = parser.parse_args(["--enum", "one"])
        self.assertEqual(parser.errors, [])
        self.assertEqual(args.enum, "one")
