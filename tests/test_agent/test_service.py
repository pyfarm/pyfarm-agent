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

from functools import partial

from twisted.python.usage import UsageError
from pyfarm.core.enums import UseAgentAddress, AgentState

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.service import (
    check_address, convert_option_ston, convert_option_projects,
    convert_option_contact_addr, convert_option_enum)


class TestConversionFunctions(TestCase):
    def test_address(self):
        self.assertRaises(ValueError, lambda: check_address("."))
        self.assertEqual(check_address("0.0.0.0"), "0.0.0.0")

    def test_ston(self):
        for v in ("inf", "infinite", "unlimited"):
            self.assertEqual(
                convert_option_ston("http-max-retries", v), float("inf"))

        self.assertEqual(
            convert_option_ston("ram", "42"), 42)

        self.assertRaises(UsageError, lambda: convert_option_ston("", ""))

    def test_projects(self):
        self.assertEqual(
            convert_option_projects("", "a,b,c"), ["a", "b", "c"])
        self.assertEqual(
            convert_option_projects("", "a   ,b ,c  "), ["a", "b", "c"])
        self.assertEqual(
            convert_option_projects("", "a,b,"), ["a", "b"])

    def test_contact_addr(self):
        for i in UseAgentAddress:
            self.assertIsInstance(i, basestring)
            self.assertEqual(convert_option_contact_addr("", i), i)
            upper_i = i.upper()
            self.assertEqual(convert_option_contact_addr("", upper_i), i)
        self.assertRaises(
            UsageError, lambda: convert_option_contact_addr("", "foobar"))

    def test_enum_options(self):
        for enum in (UseAgentAddress, AgentState):
            converter = partial(convert_option_enum, enum=enum)

            for value in enum:
                self.assertEqual(converter("", value), value)
                self.assertEqual(converter("", value.upper()), value)

            self.assertRaises(UsageError, lambda: converter("", "foobar"))
