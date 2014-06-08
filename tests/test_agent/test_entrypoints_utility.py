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
from pyfarm.agent.sysinfo import network
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
        self.assertEqual(self.sysident, get_system_identifier())

    def test_stores_cache(self):
        _, path = tempfile.mkstemp()
        self.add_cleanup_path(path)

        value = get_system_identifier(cache_path=path, overwrite=True)
        with open(path, "rb") as cache_file:
            cached_value = cache_file.read()

        self.assertEqual(str(value), cached_value)

    def test_cache_oversized_value(self):
        _, path = tempfile.mkstemp()
        self.add_cleanup_path(path)

        with open(path, "wb") as cache_file:
            cache_file.write(str(SYSTEMID_MAX + 10))

        self.assertEqual(self.sysident, get_system_identifier(cache_path=path))

    def test_retrieves_stored_value(self):
        _, path = tempfile.mkstemp()
        self.add_cleanup_path(path)

        with open(path, "wb") as cache_file:
            cache_file.write(str(42))

        self.assertEqual(42, get_system_identifier(cache_path=path))

    def test_invalid_systemid_range(self):
        self.assertRaises(
            ValueError,
            lambda: get_system_identifier(systemid=SYSTEMID_MAX + 1))

    def test_invalid_systemid_type(self):
        self.assertRaises(
            TypeError,
            lambda: get_system_identifier(systemid=""))

    def test_invalid_cache_path_type(self):
        self.assertRaises(
            TypeError,
            lambda: get_system_identifier(cache_path=1))
