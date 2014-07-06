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

from functools import partial
from os import urandom, environ
from os.path import abspath

from pyfarm.core.utility import convert
from pyfarm.agent.config import config
from pyfarm.agent.entrypoints.utility import (
    SYSTEMID_MAX, get_system_identifier, SetConfig, SetConfigConst)
from pyfarm.agent.sysinfo import network, system
from pyfarm.agent.testutil import TestCase, ErrorCapturingParser, skipIf


class TestSystemIdentifier(TestCase):
    def setUp(self):
        super(TestSystemIdentifier, self).setUp()
        self.sysident = 0
        for mac in network.mac_addresses():
            self.sysident ^= int("0x" + mac.replace(":", ""), 0)

    @skipIf("TRAVIS" in environ, "Fails on Travis")
    def test_generation(self):
        self.assertEqual(
            self.sysident,
            get_system_identifier(self.sysident, self.create_test_file()))

    @skipIf("TRAVIS" in environ, "Fails on Travis")
    def test_stores_cache(self):
        path = self.create_test_file()
        value = get_system_identifier(self.sysident, path, write_always=True)

        with open(path, "rb") as cache_file:
            cached_value = cache_file.read()

        self.assertEqual(str(value), cached_value)

    def test_oversize_value_fail(self):
        path = self.create_test_file()
        systemid = SYSTEMID_MAX + 10
        with self.assertRaises(ValueError):
            get_system_identifier(systemid, path)

    @skipIf("TRAVIS" in environ, "Fails on Travis")
    def test_oversize_value_ignored_cache(self):
        systemid = SYSTEMID_MAX + 10
        path = self.create_test_file(str(systemid))

        self.assertEqual(
            system.system_identifier(), get_system_identifier("auto", path))

    def test_retrieves_stored_value(self):
        path = self.create_test_file(str(42))
        self.assertEqual(42, get_system_identifier("auto", path))

    def test_invalid_systemid_range(self):
        with self.assertRaises(ValueError):
            get_system_identifier(SYSTEMID_MAX + 1, self.create_test_file())

    def test_invalid_systemid_type(self):
        with self.assertRaises(TypeError):
            get_system_identifier("", self.create_test_file())

    @skipIf("TRAVIS" in environ, "Fails on Travis")
    def test_cache_path_is_none(self):
        result = get_system_identifier("auto", cache_path=None)
        self.assertEqual(result, system.system_identifier())

        with open(config["agent_systemid_cache"], "r") as stream:
            cache_data = stream.read().strip()

        self.assertEqual(convert.ston(cache_data), result)

    @skipIf("TRAVIS" in environ, "Fails on Travis")
    def test_invalid_cache_data(self):
        with open(config["agent_systemid_cache"], "w") as stream:
            stream.write("foobar")

        result = get_system_identifier("auto", cache_path=None)
        self.assertEqual(result, system.system_identifier())


class TestSetConfig(TestCase):
    def test_requires_key(self):
        with self.assertRaises(KeyError):
            SetConfig()

    def test_set_config(self):
        key = urandom(16).encode("hex")
        value = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument(
            "--foo", action=partial(SetConfig, key=key))
        self.assertNotIn(key, config)
        args = parser.parse_args(["--foo", value])
        self.assertEqual(args.foo, value)
        self.assertIn(key, config)
        self.assertEqual(config[key], value)

    def test_set_config_abspath(self):
        key = urandom(16).encode("hex")
        path = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument(
            "--foo", action=partial(SetConfig, key=key, isfile=True))
        self.assertNotIn(key, config)
        args = parser.parse_args(["--foo", path])
        self.assertEqual(args.foo, abspath(path))
        self.assertIn(key, config)
        self.assertEqual(config[key], abspath(path))


class TestSetConfigConst(TestCase):
    def test_requires_key(self):
        with self.assertRaises(KeyError):
            SetConfigConst()

    def test_requires_value(self):
        with self.assertRaises(KeyError):
            SetConfigConst(key="foo")

    def test_set_true(self):
        key = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument(
            "--foo", action=partial(SetConfigConst, key=key, value=True))
        self.assertNotIn(key, config)
        args = parser.parse_args(["--foo"])
        self.assertEqual(args.foo, True)
        self.assertIn(key, config)
        self.assertEqual(config[key], True)

    def test_set_false(self):
        key = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument(
            "--foo", action=partial(SetConfigConst, key=key, value=False))
        self.assertNotIn(key, config)
        args = parser.parse_args(["--foo"])
        self.assertEqual(args.foo, False)
        self.assertIn(key, config)
        self.assertEqual(config[key], False)
