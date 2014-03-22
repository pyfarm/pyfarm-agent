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
from random import randint, choice
from httplib import OK

from twisted.internet.defer import DeferredList, Deferred

from pyfarm.core.config import read_env_bool
from pyfarm.core.enums import AgentState, UseAgentAddress
from pyfarm.core.sysinfo import memory, cpu
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.config import config
from pyfarm.agent.http.client import get
from pyfarm.agent.service import Agent

ORIGINAL_CONFIGURATION = config.copy()
TIME_OFFSET = randint(-50, 50)

config.update({
    "time-offset": TIME_OFFSET
})


class AgentTestBase(TestCase):
    RAND_LENGTH = 8

    def setUp(self):
        super(AgentTestBase, self).setUp()
        self.config = {
            "http-retry-delay": 1,
            "persistent-http-connections": False,
            "master-api": "http://127.0.0.1:80/api/v1",
            "master": "127.0.0.1",
            "hostname": os.urandom(self.RAND_LENGTH).encode("hex"),
            "ip": "10.%s.%s.%s" % (
                randint(1, 255), randint(1, 255), randint(1, 255)),
            "use-address": choice(UseAgentAddress),
            "ram": int(memory.total_ram()),
            "cpus": cpu.total_cpus(),
            "port": randint(10000, 50000),
            "free-ram": int(memory.ram_free()),
            "time-offset": randint(-50, 50),
            "state": choice(AgentState)}
        config.update(self.config)


# TODO: need better tests, these are a little rudimentary at the moment
class TestAgentBasicMethods(AgentTestBase):
    def test_agent_api_url(self):
        config["agent-id"] = 1
        agent = Agent()
        self.assertEqual(
            agent.agent_api(), "http://127.0.0.1:80/api/v1/agents/1")

    def test_agent_api_url_keyerror(self):
        agent = Agent()
        self.assertIsNone(agent.agent_api())

    def test_http_retry_delay(self):
        config["http-retry-delay"] = 1
        agent = Agent()
        self.assertEqual(agent.http_retry_delay(uniform=True), 1)

    def test_http_retry_delay_custom_delay(self):
        config["http-retry-delay"] = 1
        agent = Agent()
        self.assertEqual(
            agent.http_retry_delay(uniform=False, get_delay=lambda: 1), 2)

    def test_system_data(self):
        expected = {
            "hostname": config["hostname"],
            "ip": config["ip"],
            "use_address": config["use-address"],
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


class TestRunAgent(AgentTestBase):
    def setUp(self):
        if not read_env_bool("PYFARM_AGENT_TEST_STARTUP", True):
            self.skipTest("startup tests disabled")

        super(TestRunAgent, self).setUp()
        self.agent_id = None

    def test_agent_created(self):
        agent = Agent()
        finised = Deferred()

        # test to make sure that the data in the database
        # matches that our agent says
        def test_resulting_agent_data(result):
            self.assertEqual(result.code, OK)
            db_data = result.json()
            agent_data = json.loads(json.dumps(agent.system_data()))
            shared_keys = db_data.viewkeys() & agent_data.viewkeys()
            filtered_db_data = dict(
                (k, v) for k, v in db_data.items() if k in shared_keys)
            self.assertEqual(filtered_db_data, agent_data)
            finised.callback(None)

        def start_search_for_agent_finished(result):
            self.assertIn((True, "created"), result)
            self.assertIn("agent-id", config)
            self.assertEqual(
                agent.agent_api(),
                "%(master-api)s/agents/%(agent-id)s" % config)
            return get(
                agent.agent_api(),
                callback=test_resulting_agent_data,
                errback=lambda failure: self.fail(str(failure)))

        deferred = agent.start_search_for_agent()
        deferred.addCallback(start_search_for_agent_finished)
        return DeferredList([deferred, finised])

    def test_agent_updated(self):
        agent = Agent()
        results_tested = Deferred()
        agent_created_two = Deferred()
        self.agent_id = None

        # test to make sure that the data in the database
        # matches that our agent says
        def test_resulting_agent_data(result):
            self.assertEqual(result.code, OK)
            db_data = result.json()
            agent_data = json.loads(json.dumps(agent.system_data()))
            shared_keys = db_data.viewkeys() & agent_data.viewkeys()
            filtered_db_data = dict(
                (k, v) for k, v in db_data.items() if k in shared_keys)
            self.assertEqual(filtered_db_data, agent_data)
            results_tested.callback(None)

        # The method used to start the agent has started a second
        # time.  The results should be nearly identical minus
        # what the result contains
        def second_start_search_for_agent_finished(result):
            self.assertTrue((True, "updated"), result)
            self.assertEqual(config["agent-id"], self.agent_id)
            self.assertEqual(
                agent.agent_api(),
                "%s/agents/%s" % (config["master-api"], self.agent_id))
            return get(
                agent.agent_api(),
                callback=test_resulting_agent_data,
                errback=lambda failure: self.fail(str(failure)))

        # the agent has been started
        def start_search_for_agent_finished(result):
            self.assertIn((True, "created"), result)
            self.assertIn("agent-id", config)
            self.agent_id = config["agent-id"]
            self.assertEqual(
                agent.agent_api(),
                "%s/agents/%s" % (config["master-api"], self.agent_id))

            # callbacks can't be called twice so we reset the callback
            agent.agent_created = agent_created_two

            deferred = agent.start_search_for_agent()
            deferred.addCallback(second_start_search_for_agent_finished)
            return deferred

        deferred = agent.start_search_for_agent()
        deferred.addCallback(start_search_for_agent_finished)
        return DeferredList([
            deferred, agent_created_two, results_tested])