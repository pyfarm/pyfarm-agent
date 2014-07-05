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

import tempfile

from pyfarm.agent.entrypoints.utility import (
    get_system_identifier, SYSTEMID_MAX)
from pyfarm.agent.sysinfo import network, system
from pyfarm.agent.testutil import TestCase


class TestSystemIdentifier(TestCase):
    def setUp(self):
        super(TestSystemIdentifier, self).setUp()
        self.sysident = 0
        for mac in network.mac_addresses():
            self.sysident ^= int("0x" + mac.replace(":", ""), 0)

        if self.sysident == 0:
            self.skipTest(
                "System identifier could not be generated in a non-random "
                "fashion.")

    def test_generation(self):
        self.assertEqual(
            self.sysident,
            get_system_identifier(self.sysident, self.create_test_file()))

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

    def test_oversize_value_ignored_cache(self):
        systemid = SYSTEMID_MAX + 100000000000000000000
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

