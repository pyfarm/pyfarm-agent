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
from httplib import OK, CREATED

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from pyfarm.core.enums import AgentState
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.config import config
from pyfarm.agent.http.client import get
from pyfarm.agent.service import Agent


# TODO: need better tests, these are a little rudimentary at the moment
class TestAgentBasicMethods(TestCase):
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


class TestRunAgent(TestCase):
    def test_agent_created(self):
        self.agent = Agent()
        finished = Deferred()

        def get_resulting_agent_data(run=True):
            def get_data():
                return get(
                    self.agent.agent_api(),
                    callback=test_resulting_agent_data,
                    errback=finished.errback)
            return get_data() if run else get_data

        # test to make sure that the data in the database
        # matches that our agent says
        def test_resulting_agent_data(result):
            if result.code != OK:
                return reactor.callLater(
                    .1, get_resulting_agent_data(run=False))
            else:
                db_data = result.json()
                agent_data = json.loads(json.dumps(self.agent.system_data()))
                self.assertEqual(db_data["id"], config["agent-id"])
                self.assertEqual(db_data["hostname"], agent_data["hostname"])
                self.assertEqual(db_data["cpus"], agent_data["cpus"])
                self.assertEqual(db_data["time_offset"], config["time-offset"])

                # we've run all the tests
                finished.callback(None)

        def start_search_for_agent_finished(result):
            # Have to use a try/except here because we're not working
            # directly with a deferred.  If we don't do this any tests
            # that fail would end up blocking.
            try:
                self.assertIn((True, CREATED), result)
                self.assertIn("agent-id", config)
                self.assertEqual(
                    self.agent.agent_api(),
                    "%(master-api)s/agents/%(agent-id)s" % config)
            except Exception as e:
                finished.errback(e)
            else:
                reactor.callLater(.1, get_resulting_agent_data(run=False))

        deferred = self.agent.run(shutdown_events=False, http_server=False)
        deferred.addCallbacks(
            start_search_for_agent_finished, finished.errback)
        return finished

    def test_agent_updated(self):
        self.agent = Agent()
        finished = Deferred()
        agent_created_two = Deferred()
        self.agent_id = None

        # retrieve the data we just updated
        def get_resulting_agent_data(run=True):
            def get_data():
                get(
                    self.agent.agent_api(),
                    callback=test_resulting_agent_data,
                    errback=finished.errback)
            return get_data() if run else get_data

        # We got a response back from our GET request to the newly
        # updated agent.  We only care about OK responses, all others
        # should be retried.
        def test_resulting_agent_data(result):
            if result.code != OK:
                reactor.callLater(.1, get_resulting_agent_data(run=False))
            else:
                db_data = result.json()
                agent_data = json.loads(json.dumps(self.agent.system_data()))
                self.assertEqual(db_data["id"], config["agent-id"])
                self.assertEqual(db_data["hostname"], agent_data["hostname"])
                self.assertEqual(db_data["cpus"], agent_data["cpus"])
                self.assertEqual(db_data["time_offset"], config["time-offset"])
                finished.callback(None)

        # The method used to start the agent has started a second
        # time.  The results should be nearly identical minus
        # what the result contains
        def second_start_search_for_agent_finished(result):
            try:
                self.assertTrue((True, OK), result)
                self.assertEqual(config["agent-id"], self.agent_id)
                self.assertEqual(
                    self.agent.agent_api(),
                    "%s/agents/%s" % (config["master-api"], self.agent_id))
            except Exception as e:
                finished.errback(e)

            return reactor.callLater(.1, get_resulting_agent_data(run=False))

        # Callback run when the agent has been created in the database
        # and we've got a response back from the REST api.
        def start_search_for_agent_finished(result):
            try:
                self.assertIn((True, CREATED), result)
                self.assertIn("agent-id", config)
                self.agent_id = config["agent-id"]
                self.assertEqual(
                    self.agent.agent_api(),
                    "%s/agents/%s" % (config["master-api"], self.agent_id))
            except Exception as e:
                finished.errback(e)

            # callbacks can't be called twice so we reset the callback
            self.agent.agent_created = agent_created_two

            deferred = self.agent.start_search_for_agent()
            deferred.addCallbacks(
                second_start_search_for_agent_finished, finished.errback)
            return deferred

        deferred = self.agent.run(shutdown_events=False, http_server=False)
        deferred.addCallbacks(
            start_search_for_agent_finished, finished.errback)

        return finished

    def test_shutdown(self):
        finished = Deferred()

        def get_resulting_agent_data(run=True):
            def get_data():
                get(
                    self.agent.agent_api(),
                    callback=test_resulting_agent_data,
                    errback=finished.errback)
            return get_data() if run else get_data

        def test_resulting_agent_data(result):
            if result.code != OK:
                reactor.callLater(.1, get_resulting_agent_data(run=False))
            else:
                db_data = result.json()
                self.assertEqual(db_data["state"], AgentState.OFFLINE)
                agent_data = json.loads(json.dumps(self.agent.system_data()))
                self.assertEqual(db_data["id"], config["agent-id"])
                self.assertEqual(db_data["hostname"], agent_data["hostname"])
                self.assertEqual(db_data["cpus"], agent_data["cpus"])
                self.assertEqual(db_data["time_offset"], config["time-offset"])
                finished.callback(None)

        def start_test(_):
            try:
                # in the tests above we turn off shutdown event
                # registration by default
                self.assertFalse(self.agent.shutdown_registered)
                deferred = self.agent.shutdown_post_update()
                deferred.addCallback(get_resulting_agent_data)

            except Exception as e:
                finished.errback(e)

        deferred = self.test_agent_created()
        deferred.addCallbacks(start_test, finished.errback)
        return finished
