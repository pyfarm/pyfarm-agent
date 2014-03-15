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
from pyfarm.core.enums import Values, BOOLEAN_TRUE, BOOLEAN_FALSE, NONE
from pyfarm.core.utility import convert, dumps, ImmutableDict


class ConvertSize(TestCase):
    def test_convert_bytetomb(self):
        self.assertEqual(convert.bytetomb(10485760), 10.0)
        self.assertEqual(convert.bytetomb(11010048), 10.5)

    def test_convert_mbtogb(self):
        self.assertEqual(convert.mbtogb(2048), 2.0)
        self.assertEqual(convert.mbtogb(4608), 4.5)


class ConvertString(TestCase):
    def test_convert_ston(self):
        self.assertEqual(convert.ston(42), 42)
        self.assertEqual(convert.ston("42"), 42)

    def test_convert_ston_error(self):
        with self.assertRaises(TypeError):
            convert.ston(None)

        with self.assertRaises(ValueError):
            convert.ston("foo")

        with self.assertRaises(ValueError):
            convert.ston("[]")


class ConvertBool(TestCase):
    def test_convert_true(self):
        for true_value in BOOLEAN_TRUE:
            self.assertTrue(convert.bool(true_value))

    def test_convert_false(self):
        for false_value in BOOLEAN_FALSE:
            self.assertFalse(convert.bool(false_value))

    def test_convert_bool_error(self):
        with self.assertRaises(ValueError):
            convert.bool("")


class ConvertNone(TestCase):
    def test_convert_none(self):
        for none_value in NONE:
            self.assertIsNone(convert.none(none_value))

    def test_convert_none_error(self):
        with self.assertRaises(ValueError):
            convert.none("foo")


class ConvertList(TestCase):
    def test_convert_list_bad_input_types(self):
        with self.assertRaises(TypeError):
            convert.list(None)

        with self.assertRaises(TypeError):
            convert.list("", sep=None)

    def test_convert_list(self):
        self.assertEqual(convert.list("  a,b , c"), ["a", "b", "c"])

    def test_convert_list_no_strip(self):
        self.assertEqual(
            convert.list("  a,b , c", strip=False),
            ["  a", "b ", " c"])

    def test_convert_list_filter_empty(self):
        self.assertEqual(
            convert.list("  a,b , c,,", strip=False),
            ["  a", "b ", " c"])

    def test_convert_alt_sep(self):
        self.assertEqual(convert.list("a:b", sep=":"), ["a", "b"])


class JSONDumper(TestCase):
    def setUp(self):
        Values.check_uniqueness = False

    def test_dump_enum_value(self):
        self.assertEqual(
            loads(dumps({"data": Values(1, "A")})),
            loads(dumps({"data": "A"})))


class TestImmutableDict(TestCase):
    def test_no_decorator(self):
        self.assertFalse(hasattr(ImmutableDict, "write_required"))

    def test_iterators(self):
        i = ImmutableDict()
        self.assertNotIsInstance(i.items(), (list, tuple))
        self.assertNotIsInstance(i.keys(), (list, tuple))
        self.assertNotIsInstance(i.values(), (list, tuple))

    def test_parent_class(self):
        i = ImmutableDict()
        self.assertIsInstance(i, dict)

    def test_cant_call_init_again(self):
        i = ImmutableDict({"foo": True})
        with self.assertRaises(RuntimeError):
            i.__init__()

    def test_immutable(self):
        i = ImmutableDict({"true": True})
        with self.assertRaises(RuntimeError):
            i.clear()

        with self.assertRaises(RuntimeError):
            i.pop("true")

        with self.assertRaises(RuntimeError):
            i.popitem()

        with self.assertRaises(RuntimeError):
            i.setdefault("false", False)

        with self.assertRaises(RuntimeError):
            i.update(one=1)

        self.assertEqual(i, {"true": True})
