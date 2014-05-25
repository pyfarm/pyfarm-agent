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
import json

try:
    from httplib import OK, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from pyfarm.core.enums import AgentState
from pyfarm.agent.sysinfo.system import system_identifier
from pyfarm.agent.testutil import TestCase, PYFARM_AGENT_MASTER
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.agent.service import Agent


# TODO: need better tests, these are a little rudimentary at the moment
class TestAgentBasicMethods(TestCase):
    def test_agent_api_url(self):
        config["agent-id"] = 1
        agent = Agent()
        self.assertEqual(
            agent.agent_api(),
            "http://%s/api/v1/agents/1" % PYFARM_AGENT_MASTER)

    def test_agent_api_url_keyerror(self):
        agent = Agent()
        self.assertIsNone(agent.agent_api())

    def test_system_data(self):
        expected = {
            "systemid": system_identifier(),
            "hostname": config["hostname"],
            "ram": config["ram"],
            "cpus": config["cpus"],
            "port": config["port"],
            "free_ram": config["free-ram"],
            "time_offset": config["time-offset"],
            "state": config["state"]}

        agent = Agent()
        self.assertEqual(agent.system_data(), expected)
        config["remote-ip"] = expected["remote_ip"] = \
            os.urandom(16).encode("hex")
        self.assertEqual(agent.system_data(), expected)


class TestRunAgent(TestCase):
    def test_created(self):
        self.skipTest("NOT IMPLEMENTED")

    def test_updated(self):
        self.skipTest("NOT IMPLEMENTED")
