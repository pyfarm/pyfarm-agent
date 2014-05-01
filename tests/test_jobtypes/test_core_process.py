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

import os
from collections import namedtuple

from pyfarm.agent.testutil import TestCase
from pyfarm.jobtypes.core.process import (
    ProcessInputs, ReplaceEnvironment, ProcessProtocol)

DummyInputs = namedtuple("DummyInputs", ("task", ))


class TestProcessInputs(TestCase):
    def test_task_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs(None, []))

    def test_command_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, None,))

    def test_env_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, [], env=-1))

    def test_path_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, [], path=-1))
        
    def test_user_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, [], user=-1))
        
    def test_group_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, [], group=-1))

    def convert_command_type(self):
        self.assertRaises(TypeError,
            lambda: ProcessInputs({}, [None]))

    def test_convert_numeric_command_values(self):
        self.assertEqual(ProcessInputs({}, [1]).command, ("1", ))


class TestReplaceEnvironment(TestCase):
    original_environment = os.environ.copy()

    def tearDown(self):
        super(TestReplaceEnvironment, self).tearDown()
        os.environ.clear()
        os.environ.update(self.original_environment)

    def test_uses_os_environ(self):
        os.environ.clear()
        os.environ.update(
            {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")})
        env = ReplaceEnvironment(None)
        self.assertEqual(env.environment, os.environ)
        self.assertIs(env.environment, os.environ)

    def test_enter(self):
        original = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        frozen = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        os.environ.clear()
        os.environ.update(original)

        with ReplaceEnvironment(frozen, os.environ) as env:
            self.assertEqual(env.original_environment, original)
            self.assertEqual(os.environ, frozen)

    def test_exit(self):
        original = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        original_copy = original.copy()
        frozen = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        os.environ.clear()
        os.environ.update(original)

        with ReplaceEnvironment(frozen, os.environ) as env:
            pass

        self.assertEqual(os.environ, original_copy)


class TestProcessProtocol(TestCase):
    def test_process_id_from_task(self):
        pass
        # protocol = ProcessProtocol(
        #     None, None, DummyInputs(task=42), None, None, None, None, None,
        #     None)