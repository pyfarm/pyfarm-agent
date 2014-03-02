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

from __future__ import with_statement

from json import loads

from pyfarm.core.testutil import TestCase
from pyfarm.core.enums import Values
from pyfarm.core.utility import convert, dumps


class Convert(TestCase):
    def test_convert_bytetomb(self):
        self.assertEqual(convert.bytetomb(10485760), 10.0)
        self.assertEqual(convert.bytetomb(11010048), 10.5)

    def test_convert_mbtogb(self):
        self.assertEqual(convert.mbtogb(2048), 2.0)
        self.assertEqual(convert.mbtogb(4608), 4.5)

    def test_convert_ston(self):
        self.assertEqual(convert.ston(42), 42)
        self.assertEqual(convert.ston("42"), 42)

        with self.assertRaises(TypeError):
            convert.ston(None)

        with self.assertRaises(ValueError):
            convert.ston("foo")


class JSONDumper(TestCase):
    def setUp(self):
        Values.check_uniqueness = False

    def test_dump_enum_value(self):
        self.assertEqual(
            loads(dumps({"data": Values(1, "A")})),
            loads(dumps({"data": "A"})))
