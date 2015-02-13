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
from platform import platform

try:
    from httplib import OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR

from mock import patch
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList

from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import deferLater

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
class TestAgentServiceAttributes(TestCase):
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

    def test_callback_agent_id_set(self):
        schedule_call_args = []

        def fake_schedule_call(*args, **kwargs):
            schedule_call_args.append((args, kwargs))

        agent = Agent()
        with patch.object(agent, "repeating_call", fake_schedule_call):
            config.pop("free_ram", None)
            agent.callback_agent_id_set(
                config.CREATED, "agent_id", None, None, shutdown_events=True
            )
            config["agent_id"] = uuid.uuid4()

        self.assertTrue(agent.register_shutdown_events)
        self.assertIsNotNone(config["free_ram"])
        self.assertEqual(
            schedule_call_args[0][0][0], config["agent_master_reannounce"])
        self.assertEqual(
            schedule_call_args[0][0][1], agent.reannounce)

    def test_shutting_down_getter(self):
        config["shutting_down"] = 42
        agent = Agent()
        self.assertEqual(agent.shutting_down, 42)

    def test_shutting_down_settr(self):
        agent = Agent()
        agent.shutting_down = True
        self.assertEqual(config["shutting_down"], True)
        agent.shutting_down = False
        self.assertEqual(config["shutting_down"], False)

    def test_shutting_down_settr_invalid_type(self):
        agent = Agent()
        with self.assertRaises(AssertionError):
            agent.shutting_down = None


class TestScheduledCall(TestCase):
    @inlineCallbacks
    def test_shutting_down(self):
        agent = Agent()
        agent.shutting_down = True
        result = yield agent.repeating_call(0, lambda: None)
        self.assertIsNone(result)

    @inlineCallbacks
    def test_repeat_until_shutdown(self):
        wait_for_shutdown = Deferred()
        agent = Agent()
        self.counter = 0
        self.not_shutdown_counter = 0

        def callback():
            if not agent.shutting_down:
                self.not_shutdown_counter += 1
            self.counter += 1

        def shutdown():
            agent.shutting_down = True
            wait_for_shutdown.callback(None)

        agent.repeating_call(0, callback)
        reactor.callLater(.01, shutdown)
        yield wait_for_shutdown
        self.assertEqual(self.counter, self.not_shutdown_counter)

    @inlineCallbacks
    def test_now(self):
        agent = Agent()
        deferred = Deferred()
        agent.repeating_call(
            0, lambda: deferred.callback(None), now=True, repeat_max=1)

        yield deferred

    @inlineCallbacks
    def test_repeat(self):
        agent = Agent()
        deferred1 = Deferred()
        deferred2 = Deferred()
        deferred3 = Deferred()

        def callback():
            if not deferred1.called:
                deferred1.callback(None)

            elif not deferred2.called:
                deferred2.callback(None)

            elif not deferred3.called:
                deferred3.callback(None)

        agent.repeating_call(0, callback, repeat_max=2)
        yield DeferredList([deferred1, deferred2])
        self.assertFalse(deferred3.called)
        agent.repeating_call(0, callback, repeat_max=1)
        yield deferred3


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
        super(TestAgentPostToMaster, self).tearDown()
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

    @inlineCallbacks
    def test_bad_request(self):
        self.fake_api.code = BAD_REQUEST
        messages = []

        def capture_messages(message, *args, **kwargs):
            messages.append((message, args, kwargs))

        agent = Agent()
        with patch.object(svclog, "error", capture_messages):
            result = yield agent.post_agent_to_master()

        self.assertIsNone(result)

        for message, args, kwargs in messages:
            if "accepted our POST request but responded with code" in message:
                self.assertIn("we cannot retry this request", message)
                break
        else:
            self.fail("Never found log message.")

    @inlineCallbacks
    def test_internal_server_error_retry(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR
        warning_messages = []
        info_messages = []
        agent = Agent()

        def capture_warning_messages(message, *args, **kwargs):
            warning_messages.append((message, args, kwargs))

        def capture_info_messages(message, *args, **kwargs):
            info_messages.append((message, args, kwargs))

        def change_response_code():
            self.fake_api.code = CREATED

        # TODO: lower this value once the retry delay min. is configurable
        deferLater(reactor, 2.5, change_response_code)

        with patch.object(svclog, "warning", capture_warning_messages):
            with patch.object(svclog, "info", capture_info_messages):
                yield agent.post_agent_to_master()

        warning_count = 0
        for message, args, kwargs in warning_messages:
            if "Failed to post to master due to a server side" in message:
                warning_count += 1

        self.assertGreaterEqual(warning_count, 2)

        for message, args, kwargs in info_messages:
            if "A new agent with an id of %s was created." in message:
                break
        else:
            self.fail("Never found log message.")

    @inlineCallbacks
    def test_internal_server_error_retry_stop_on_shutdown(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR
        warning_messages = []

        def capture_warning_messages(message, *args, **kwargs):
            warning_messages.append((message, args, kwargs))

        agent = Agent()

        def shutdown():
            agent.shutting_down = True

        # TODO: lower this value once the retry delay min. is configurable
        deferLater(reactor, 1.5, shutdown)

        with patch.object(svclog, "warning", capture_warning_messages):
            yield agent.post_agent_to_master()

        for message, args, kwargs in warning_messages:
            if ("Failed to post to master" in message
                    and "shutting down" in message):
                break
        else:
            self.fail("Never found log message.")

    @inlineCallbacks
    def test_exception_raised(self):
        # Shutdown the server so we don't have anything to
        # reply on.
        yield self.server.loseConnection()

        warning_messages = []
        error_messages = []
        agent = Agent()

        def capture_warning_messages(message, *args, **kwargs):
            warning_messages.append((message, args, kwargs))

        def capture_error_messages(message, *args, **kwargs):
            error_messages.append((message, args, kwargs))

        def shutdown():
            agent.shutting_down = True

        # TODO: lower this value once the retry delay min. is configurable
        # NOTE: In other tests we specifically test shutdown behavior.  Here
        # we're not doing that because it can be done in one test and because
        # the http server is not online.
        deferLater(reactor, 2.5, shutdown)

        with patch.object(svclog, "warning", capture_warning_messages):
            with patch.object(svclog, "error", capture_error_messages):
                yield agent.post_agent_to_master()

        error_count = 0
        for message, args, kwargs in error_messages:
            if "the connection was refused" in message:
                error_count += 1

        self.assertGreaterEqual(error_count, 2)

        for message, args, kwargs in warning_messages:
            if "Not retrying POST to master, shutting down." in message:
                break
        else:
            self.fail("Never found log message.")
