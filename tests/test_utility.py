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

import math
from json import loads

try:
    _range = xrange
except NameError:
    _range = range

from pyfarm.core.testutil import TestCase
from pyfarm.core.enums import Values
from pyfarm.core.utility import float_range, convert, rounded, dumps


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


class Rounding(TestCase):
    def test_rounded(self):
        self.assertEqual(rounded(math.pi), 3.1416)
        self.assertEqual(rounded(math.pi, 2), 3.14)
        self.assertEqual(rounded(math.pi, 6), 3.141593)

    def test_rounded_places_type_error(self):
        with self.assertRaises(TypeError):
            rounded(1.5, None)

    def test_rounded_places_count(self):
        with self.assertRaises(ValueError):
            rounded(1.5, 0)


class Range(TestCase):
    def test_range_end_error(self):
        with self.assertRaises(ValueError):
            float_range(2, 1)

    def test_range_by_error(self):
        with self.assertRaises(ValueError):
            float_range(5, by=0)

    def test_intrange_start(self):
        self.assertEqual(list(float_range(5)), list(_range(5)))

    def test_intrangestartby(self):
        self.assertEqual(list(float_range(5, by=1)), [0, 1, 2, 3, 4])
        self.assertEqual(list(float_range(5, by=2)), [0, 2, 4])

    def test_intrange_startendby(self):
        self.assertEqual(list(float_range(1, 1, 1)), list(_range(1, 1, 1)))
        self.assertEqual(list(float_range(1, 10, 1)), list(_range(1, 10, 1)))
        self.assertEqual(list(float_range(1, 10, 2)), list(_range(1, 10, 2)))

    def test_floatrange_startby(self):
        self.assertEqual(
            list(float_range(2.25, by=.25)),
            [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25])
        self.assertEqual(list(float_range(2, by=.25)),
            [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0])
        self.assertEqual(list(float_range(2, by=.75)), [0, 0.75, 1.5])
        self.assertEqual(list(float_range(2, by=.75, add_endpoint=True)),
            [0, 0.75, 1.5, 2.0])
        self.assertEqual(list(float_range(2.5, by=.15)),
            [0, 0.15, 0.3, 0.45, 0.6, 0.75, 0.9, 1.05, 1.2, 1.35, 1.5,
             1.65, 1.8, 1.95, 2.1, 2.25, 2.4])

    def test_floatrange_start(self):
        self.assertEqual(list(list(float_range(2.25))), [0, 1, 2])
        self.assertEqual(list(list(float_range(2.25, add_endpoint=True))),
                         [0, 1, 2, 2.25])

    def test_floatrange_startend(self):
        self.assertEqual(list(float_range(2.25, 5)), [2.25, 3.25, 4.25])
        self.assertEqual(list(float_range(2.25, 5, add_endpoint=True)),
                         [2.25, 3.25, 4.25, 5])

    def test_floatrange_startendby(self):
        self.assertEqual(list(float_range(1.5, 2.5, .15)),
            [1.5, 1.65, 1.8, 1.95, 2.1, 2.25, 2.4])
        self.assertEqual(list(float_range(1.5, 2.5, .15, add_endpoint=True)),
            [1.5, 1.65, 1.8, 1.95, 2.1, 2.25, 2.4, 2.5])


class JSONDumper(TestCase):
    def setUp(self):
        Values.check_uniqueness = False

    def test_dump_enum_value(self):
        self.assertEqual(
            loads(dumps({"data": Values(1, "A")})),
            loads(dumps({"data": "A"})))
