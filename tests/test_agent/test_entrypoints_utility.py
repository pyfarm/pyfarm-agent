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

from os import urandom
import re

from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, ErrorCapturingParser


class TestConfigWithParser(TestCase):
    def test_set(self):
        key = urandom(16).encode("hex")
        value = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument("--foo", config=key, help=key, default=False)
        self.assertIn(key, config)
        args = parser.parse_args(["--foo", value])
        self.assertEqual(args.foo, value)
        self.assertIn(key, config)
        self.assertEqual(config[key], value)

    def test_uses_default(self):
        key = urandom(16).encode("hex")
        parser = ErrorCapturingParser()
        parser.add_argument("--foo", config=key, help=key, default=False)
        args = parser.parse_args()
        self.assertEqual(args.foo, False)
        self.assertEqual(config[key], False)

    def test_requires_default(self):
        parser = ErrorCapturingParser()
        with self.assertRaisesRegexp(
                AssertionError, re.compile(".*no default was provided.*")):
            parser.add_argument("--foo", config="foo", help="foo")
