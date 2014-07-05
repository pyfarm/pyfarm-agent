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

try:
    from httplib import OK, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED

from pyfarm.agent.sysinfo.system import system_identifier
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.config import config
from pyfarm.agent.service import Agent


# TODO: need better tests, these are a little rudimentary at the moment
class TestAgentBasicMethods(TestCase):
    def test_agent_api_url(self):
        config["agent-id"] = 1
        agent = Agent()
        self.assertEqual(
            agent.agent_api(),
            "%s/agents/1" % config["master_api"])

    def test_agent_api_url_keyerror(self):
        agent = Agent()
        config.pop("agent-id")
        self.assertIsNone(agent.agent_api())

    def test_system_data(self):
        config["remote_ip"] = os.urandom(16).encode("hex")
        expected = {
            "current_assignments": {},
            "systemid": system_identifier(),
            "hostname": config["agent_hostname"],
            "version": config.version,
            "ram": config["agent_ram"],
            "cpus": config["agent_cpus"],
            "remote_ip": config["remote_ip"],
            "port": config["agent_api_port"],
            "free_ram": config["free-ram"],
            "time_offset": config["agent_time_offset"],
            "state": config["state"]}

        agent = Agent()
        self.assertEqual(agent.system_data(), expected)


class TestRunAgent(TestCase):
    def test_created(self):
        self.skipTest("NOT IMPLEMENTED")

    def test_updated(self):
        self.skipTest("NOT IMPLEMENTED")
