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
import uuid
from datetime import datetime, timedelta
from functools import partial
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from platform import platform

try:
    from httplib import OK, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED

from mock import patch
from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.defer import Deferred, inlineCallbacks

from pyfarm.core.enums import AgentState
from pyfarm.agent.sysinfo.system import operating_system
from pyfarm.agent.sysinfo import cpu
from pyfarm.agent.testutil import TestCase, random_port
from pyfarm.agent.config import config
from pyfarm.agent.service import Agent, svclog
from pyfarm.agent.sysinfo import network, graphics


class HTTPReceiver(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)
        self.requests = []
        self.post_data = []
        self.headers = []
        self.data = []
        self.code = None
        self.content = None

    def render_POST(self, request):
        assert self.code is not None
        self.requests.append(request)
        self.headers.append(request.getAllHeaders())
        self.content = request.content.read()
        self.data.append(json.loads(self.content))


class FakeAgentsAPI(HTTPReceiver):
    def render_POST(self, request):
        HTTPReceiver.render_POST(self, request)
        request.setResponseCode(self.code)
        request.write(self.content)
        request.finish()
        return NOT_DONE_YET


# TODO: need better tests, these are a little rudimentary at the moment
class TestAgentBasicMethods(TestCase):
    def test_agent_api_url(self):
        config["agent_id"] = uuid.uuid4()
        agent = Agent()
        self.assertEqual(
            agent.agent_api(),
            "{master_api}/agents/{agent_id}".format(
                master_api=config["master_api"],
                agent_id=config["agent_id"]))

    def test_agent_api_url_keyerror(self):
        agent = Agent()
        config.pop("agent_id")
        self.assertIsNone(agent.agent_api())

    def test_system_data(self):
        config["remote_ip"] = os.urandom(16).encode("hex")
        expected = {
            "id": config["agent_id"],
            "current_assignments": {},
            "hostname": config["agent_hostname"],
            "version": config.version,
            "ram": config["agent_ram"],
            "cpus": config["agent_cpus"],
            "os_class": operating_system(),
            "os_fullname": platform(),
            "cpu_name": cpu.cpu_name(),
            "remote_ip": config["remote_ip"],
            "port": config["agent_api_port"],
            "time_offset": config["agent_time_offset"],
            "state": config["state"],
            "mac_addresses": list(network.mac_addresses())}

        try:
            gpu_names = graphics.graphics_cards()
            expected["gpus"] = gpu_names
        except graphics.GPULookupError:
            pass

        agent = Agent()
        system_data = agent.system_data()
        self.assertApproximates(
            system_data.pop("free_ram"), config["free_ram"], 64)
        self.assertEqual(system_data, expected)


class TestAgentPostToMaster(TestCase):
    def setUp(self):
        super(TestAgentPostToMaster, self).setUp()
        self.fake_api = FakeAgentsAPI()
        self.resource = Resource()
        self.resource.putChild("agents", self.fake_api)
        self.site = Site(self.resource)
        self.server = reactor.listenTCP(random_port(), self.site)
        config["master_api"] = "http://127.0.0.1:%s" % self.server.port

        # These usually come from the master.  We're setting them here
        # so we can operate the apis without actually talking to the
        # master.
        config["state"] = AgentState.ONLINE

    @inlineCallbacks
    def tearDown(self):
        yield self.server.loseConnection()

    @inlineCallbacks
    def test_post_created(self):
        self.fake_api.code = CREATED
        messages = []

        def capture_messages(message, *args, **kwargs):
            messages.append((message, args, kwargs))

        agent = Agent()
        with patch.object(svclog, "info", capture_messages):
            result = yield agent.post_agent_to_master()

        self.assertEqual(result["id"], config["agent_id"])
        self.assertLessEqual(
            datetime.utcnow() - config["last_master_contact"],
            timedelta(seconds=5)
        )

        for message, args, kwargs in messages:
            if "POST to %s was successful" in message:
                self.assertIn("was created", message)
                self.assertEqual(args[1], result["id"])
                break
        else:
            self.fail("Never found log message.")


    @inlineCallbacks
    def test_post_ok(self):
        self.fake_api.code = OK
        messages = []

        def capture_messages(message, *args, **kwargs):
            messages.append((message, args, kwargs))

        agent = Agent()
        with patch.object(svclog, "info", capture_messages):
            result = yield agent.post_agent_to_master()

        self.assertEqual(result["id"], config["agent_id"])
        self.assertLessEqual(
            datetime.utcnow() - config["last_master_contact"],
            timedelta(seconds=5)
        )

        for message, args, kwargs in messages:
            if "POST to %s was successful" in message:
                self.assertIn("was updated", message)
                self.assertEqual(args[1], result["id"])
                break
        else:
            self.fail("Never found log message.")
