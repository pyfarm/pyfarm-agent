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

from jinja2 import Environment as _Environment, BytecodeCache
from twisted.internet.defer import Deferred

from pyfarm.core.enums import AgentState
from pyfarm.agent.config import config
from pyfarm.agent.http.core.template import (
    InMemoryCache, Environment, EncodedStringTemplate, load)
from pyfarm.agent.testutil import TestCase


class TestMemoryCache(TestCase):
    def test_parent_class(self):
        self.assertIsInstance(InMemoryCache(), BytecodeCache)

    def test_clear_cache(self):
        cache = InMemoryCache()
        cache.cache.update(foo=True)
        cache.clear()
        self.assertEqual(InMemoryCache.cache, {})


class TestEnvironment(TestCase):
    def setUp(self):
        super(TestEnvironment, self).setUp()
        self._created_agent_id = False
        self._created_state = False

    def prepare_config(self):
        super(TestEnvironment, self).prepare_config()
        config.update({
            "state": AgentState.ONLINE,
            "agent_id": 0
        })

    def test_parent_class(self):
        self.assertIsInstance(Environment(), _Environment)

    def test_template_class(self):
        self.assertIs(Environment.template_class, EncodedStringTemplate)

    def test_global_functions(self):
        env = Environment()
        self.assertTrue(env.globals["is_int"](1))
        self.assertTrue(env.globals["is_str"](""))
        self.assertEqual(
            env.globals["typename"](InMemoryCache()), "InMemoryCache")
        self.assertEqual(
            env.globals["agent_hostname"](), config["agent_hostname"])
        self.assertEqual(env.globals["agent_id"](), config["agent_id"])
        self.assertEqual(env.globals["state"](), config["state"])
        self.assertEqual(env.globals["repr"]("foo"), "'foo'")


class TestLoad(TestCase):
    def test_loads_deferred(self):
        self.assertIsInstance(load("index.html"), EncodedStringTemplate)

    def test_render(self):
        template = load("index.html")
        data = template.render()
        self.assertIn("DOCTYPE html", data)
        self.assertIn("<title>PyFarm:Agent - Information</title>", data)
