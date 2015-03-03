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
from contextlib import nested
from datetime import datetime, timedelta
from platform import platform

try:
    from httplib import (
        OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND)
except ImportError:  # pragma: no cover
    from http.client import (
        OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND)

from mock import patch
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.task import deferLater
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.internet.defer import inlineCallbacks

from pyfarm.core.enums import AgentState
from pyfarm.agent.sysinfo.system import operating_system
from pyfarm.agent.sysinfo import cpu
from pyfarm.agent.testutil import TestCase, random_port
from pyfarm.agent.config import config
from pyfarm.agent.service import Agent, svclog
from pyfarm.agent.sysinfo import network, graphics, memory


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

    def test_callback_shutting_down_true(self):
        config["agent_shutdown_timeout"] = 4242
        agent = Agent()
        self.assertIsNone(agent.shutdown_timeout)
        agent.shutting_down = True
        self.assertIsInstance(agent.shutdown_timeout, datetime)
        expected = timedelta(
            seconds=config["agent_shutdown_timeout"]) + datetime.utcnow()
        self.assertDateAlmostEqual(agent.shutdown_timeout, expected)

    def test_callback_shutting_down_false(self):
        config["agent_shutdown_timeout"] = 4242
        agent = Agent()
        self.assertIsNone(agent.shutdown_timeout)
        agent.shutting_down = True
        agent.shutting_down = False
        self.assertIsNone(agent.shutdown_timeout)

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

        agent = Agent()
        with patch.object(svclog, "info") as log_info:
            result = yield agent.post_agent_to_master()

        self.assertEqual(result["id"], config["agent_id"])
        self.assertLessEqual(
            datetime.utcnow() - config["last_master_contact"],
            timedelta(seconds=5)
        )
        log_info.assert_called_with(
            "POST to %s was successful.  A new agent with an id of %s was "
            "created.", agent.agents_endpoint(), config["agent_id"])

    @inlineCallbacks
    def test_post_ok(self):
        self.fake_api.code = OK

        agent = Agent()
        with patch.object(svclog, "info") as log_info:
            result = yield agent.post_agent_to_master()

        self.assertEqual(result["id"], config["agent_id"])
        self.assertLessEqual(
            datetime.utcnow() - config["last_master_contact"],
            timedelta(seconds=5)
        )
        log_info.assert_called_with(
            "POST to %s was successful. Agent %s was updated.",
            agent.agents_endpoint(), config["agent_id"]
        )

    @inlineCallbacks
    def test_bad_request(self):
        self.fake_api.code = BAD_REQUEST

        agent = Agent()
        with patch.object(svclog, "error") as error_log:
            result = yield agent.post_agent_to_master()

        self.assertIsNone(result)
        error_log.assert_called_with(
            "%s accepted our POST request but responded with code %s "
            "which is a client side error.  The message the server "
            "responded with was %r.  Sorry, but we cannot retry this "
            "request as it's an issue with the agent's request.",
            agent.agents_endpoint(), BAD_REQUEST, self.fake_api.content
        )

    @inlineCallbacks
    def test_internal_server_error_retry(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR

        agent = Agent()

        def change_response_code():
            self.fake_api.code = CREATED

        # TODO: lower this value once the retry delay min. is configurable
        deferLater(reactor, 2.5, change_response_code)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(svclog, "info")
        ) as (warning_log, info_log):
            yield agent.post_agent_to_master()

        warning_log.assert_called_with(
            "Failed to post to master due to a server side error error %s, "
            "retrying in %s seconds", INTERNAL_SERVER_ERROR, 1.0
        )
        info_log.assert_called_with(
            "POST to %s was successful.  A new agent with an id of %s "
            "was created.", agent.agents_endpoint(), config["agent_id"]
        )

    @inlineCallbacks
    def test_internal_server_error_retry_stop_on_shutdown(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR
        agent = Agent()

        def shutdown():
            agent.shutting_down = True

        # TODO: lower this value once the retry delay min. is configurable
        deferLater(reactor, 1.5, shutdown)

        with patch.object(svclog, "warning") as warning_log:
            yield agent.post_agent_to_master()

        warning_log.assert_called_with(
            "Failed to post to master due to a server side error error %s. "
            "Not retrying, because the agent is shutting down",
            INTERNAL_SERVER_ERROR
        )

    @inlineCallbacks
    def test_exception_raised(self):
        # Shutdown the server so we don't have anything to
        # reply on.
        yield self.server.loseConnection()

        agent = Agent()

        def shutdown():
            agent.shutting_down = True

        # TODO: lower this value once the retry delay min. is configurable
        # NOTE: In other tests we specifically test shutdown behavior.  Here
        # we're not doing that because it can be done in one test and because
        # the http server is not online.
        deferLater(reactor, 2.5, shutdown)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(svclog, "error")
        ) as (warning_log, error_log):
            yield agent.post_agent_to_master()

        warning_log.assert_called_with(
            "Not retrying POST to master, shutting down.")
        error_log.assert_called_with(
            "Failed to POST agent to master, the connection was refused. "
            "Retrying in %s seconds", 1.0)


class TestPostShutdownToMaster(TestCase):
    def setUp(self):
        super(TestPostShutdownToMaster, self).setUp()
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

        # Mock out memory.free_ram
        self.free_ram_mock = patch.object(
            memory, "free_ram", return_value=424242)
        self.free_ram_mock.start()

        self.normal_result = {
            "state": AgentState.OFFLINE,
            "current_assignments": {},
            "free_ram": 424242
        }

    @inlineCallbacks
    def tearDown(self):
        super(TestPostShutdownToMaster, self).tearDown()
        self.free_ram_mock.stop()
        yield self.server.loseConnection()

    @inlineCallbacks
    def test_assert_shutting_down(self):
        agent = Agent()
        agent.shutting_down = False

        with nested(
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release"),
            self.assertRaises(AssertionError)
        ) as (acquire, release, _):
            yield agent.post_shutdown_to_master()

        self.assertFalse(acquire.called)
        self.assertFalse(release.called)

    @inlineCallbacks
    def test_post_not_found(self):
        self.fake_api.code = NOT_FOUND

        agent = Agent()
        agent.shutting_down = True
        with nested(
            patch.object(svclog, "warning"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (warning_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertEqual(self.normal_result, result)
        warning_log.assert_called_with(
            "Agent %r no longer exists, cannot update state.",
            config["agent_id"]
        )
        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)

    @inlineCallbacks
    def test_post_ok(self):
        self.fake_api.code = OK

        agent = Agent()
        agent.shutting_down = True
        with nested(
            patch.object(svclog, "info"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (info_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertEqual(self.normal_result, result)
        info_log.assert_called_with(
            "Agent %r has POSTed shutdown state change successfully.",
            config["agent_id"]
        )
        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)

    @inlineCallbacks
    def test_post_internal_server_error_timeout_expired(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR

        agent = Agent()
        agent.shutting_down = True
        agent.shutdown_timeout = datetime.utcnow() - timedelta(hours=1)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (warning_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertEqual(self.normal_result, result)
        warning_log.assert_called_with(
            "State update failed due to server error: %s.  Shutdown timeout "
            "reached, not retrying.", self.fake_api.data[0]
        )
        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)

    @inlineCallbacks
    def test_post_internal_server_error_retries(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR

        agent = Agent()
        agent.shutting_down = True
        agent.shutdown_timeout = datetime.utcnow() + timedelta(seconds=3)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (warning_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertEqual(self.normal_result, result)
        warning_log.assert_called_with(
            "State update failed due to server error: %s.  Shutdown timeout "
            "reached, not retrying.", self.fake_api.data[0]
        )
        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)

    @inlineCallbacks
    def test_post_exception_timeout_expired(self):
        # Shutdown the server so we don't have anything to
        # reply on.
        yield self.server.loseConnection()

        agent = Agent()
        agent.shutting_down = True
        agent.shutdown_timeout = datetime.utcnow() - timedelta(hours=1)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (warning_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertIsNone(result)
        self.assertEqual(
            warning_log.call_args[0][0],
            "State update failed due to unhandled error: %s.  "
            "Shutdown timeout reached, not retrying."
        )
        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)

    @inlineCallbacks
    def test_post_exception_retry(self):
        # Shutdown the server so we don't have anything to
        # reply on.
        yield self.server.loseConnection()

        agent = Agent()
        agent.shutting_down = True
        agent.shutdown_timeout = datetime.utcnow() + timedelta(seconds=3)

        with nested(
            patch.object(svclog, "warning"),
            patch.object(agent.post_shutdown_lock, "acquire"),
            patch.object(agent.post_shutdown_lock, "release")
        ) as (warning_log, acquire, release):
            result = yield agent.post_shutdown_to_master()

        self.assertIsNone(result)
        self.assertGreaterEqual(warning_log.call_count, 3)

        for call in warning_log.mock_calls:
            if (call[1][0] == "State update failed due to unhandled error: "
                              "%s.  Retrying in %s seconds"):
                break
        else:
            self.fail("State update never failed")

        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(release.call_count, 1)
