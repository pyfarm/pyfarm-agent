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

import os
import sys
import uuid

if sys.version_info[0:2] < (2, 7):
    from unittest2 import TestCase
else:
    from unittest import TestCase

from pyfarm.core.config import (
    read_env, read_env_number, read_env_bool, read_env_strict_number,
    BOOLEAN_FALSE, BOOLEAN_TRUE)


class TestConfig(TestCase):
    def test_readenv_missing(self):
        key = uuid.uuid4().hex
        with self.assertRaises(EnvironmentError):
            read_env(key)
        self.assertEqual(read_env(key, 42), 42)

    def test_readenv_exists(self):
        key = uuid.uuid4().hex
        value = uuid.uuid4().hex
        os.environ[key] = value
        self.assertEqual(read_env(key), value)
        del os.environ[key]

    def test_readenv_eval(self):
        key = uuid.uuid4().hex

        for value in (True, False, 42, 3.141, None, [1, 2, 3]):
            os.environ[key] = str(value)
            self.assertEqual(read_env(key, eval_literal=True), value)

        os.environ[key] = "f"
        with self.assertRaises(ValueError):
            read_env(key, eval_literal=True)

        self.assertEqual(
            read_env(key, 42, eval_literal=True, raise_eval_exception=False),
            42)

        del os.environ[key]

    def test_read_env_bool(self):
        for true in BOOLEAN_TRUE:
            key = uuid.uuid4().hex
            os.environ[key] = true
            self.assertTrue(read_env_bool(key, False))

        for false in BOOLEAN_FALSE:
            key = uuid.uuid4().hex
            os.environ[key] = false
            self.assertFalse(read_env_bool(key, True))

        with self.assertRaises(AssertionError):
            read_env_bool("")

        with self.assertRaises(TypeError):
            key = uuid.uuid4().hex
            os.environ[key] = "42"
            read_env_bool(key, 1)

        with self.assertRaises(TypeError):
            key = uuid.uuid4().hex
            self.assertTrue(read_env_bool(key, 1))

        key = uuid.uuid4().hex
        self.assertTrue(read_env_bool(key, True))

    def test_read_env_number(self):
        key = uuid.uuid4().hex
        os.environ[key] = "42"
        self.assertEqual(read_env_number(key), 42)
        key = uuid.uuid4().hex
        os.environ[key] = "3.14159"
        self.assertEqual(read_env_number(key), 3.14159)

        key = uuid.uuid4().hex
        os.environ[key] = "foo"
        with self.assertRaises(ValueError):
            self.assertEqual(read_env_number(key))

        key = uuid.uuid4().hex
        os.environ[key] = "None"
        with self.assertRaises(TypeError):
            self.assertEqual(read_env_number(key))

    def test_read_env_strict_number(self):
        with self.assertRaises(AssertionError):
            read_env_strict_number("")

        key = uuid.uuid4().hex
        os.environ[key] = "3.14159"
        self.assertEqual(read_env_strict_number(key, number_type=float),
                         3.14159)

        key = uuid.uuid4().hex
        os.environ[key] = "42"
        with self.assertRaises(TypeError):
            read_env_strict_number(key, number_type=float)