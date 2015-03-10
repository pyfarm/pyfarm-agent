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
import ntplib
import sys
import time
import json
import uuid
from contextlib import nested
from datetime import datetime, timedelta
from platform import platform
from os.path import join

try:
    from httplib import (
        OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND)
except ImportError:  # pragma: no cover
    from http.client import (
        OK, CREATED, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND)

from mock import patch, Mock
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks
from twisted.internet.task import deferLater
from twisted.python.failure import Failure
from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET

from pyfarm.core.enums import AgentState
from pyfarm.agent.http.api.assign import Assign
from pyfarm.agent.http.api.base import APIRoot, Versions
from pyfarm.agent.http.api.config import Config
from pyfarm.agent.http.api.tasks import Tasks
from pyfarm.agent.http.api.tasklogs import TaskLogs
from pyfarm.agent.http.api.state import Status, Stop, Restart
from pyfarm.agent.http.api.update import Update
from pyfarm.agent.http.core.server import StaticPath
from pyfarm.agent.http.system import Index, Configuration
from pyfarm.agent.sysinfo.system import operating_system
from pyfarm.agent.sysinfo import cpu
from pyfarm.agent.testutil import TestCase, random_port
from pyfarm.agent.config import config
from pyfarm.agent.service import Agent, svclog, ntplog
from pyfarm.agent.sysinfo import network, graphics, memory
from pyfarm.agent import utility


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


class TestSystemData(TestCase):
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
            "mac_addresses": list(network.mac_addresses()),
            "gpus": [1, 3, 5]
        }

        agent = Agent()
        with patch.object(graphics, "graphics_cards", return_value=[1, 3, 5]):
            system_data = agent.system_data()

        self.assertApproximates(
            system_data.pop("free_ram"), config["free_ram"], 64)
        self.assertEqual(system_data, expected)

    def test_agent_time_offset(self):
        config["agent_time_offset"] = "auto"
        agent = Agent()
        agent.system_data()
        self.assertNotEqual(config["agent_time_offset"], "auto")

    def test_get_offset(self):
        agent = Agent()

        response = Mock()
        response.tx_time = 10

        with nested(
            patch.object(ntplib.NTPClient, "request", return_value=response),
            patch.object(time, "time", return_value=5),
        ):
            agent.system_data(requery_timeoffset=True)

        self.assertEqual(config["agent_time_offset"], 5)

    def test_get_offset_error(self):
        offset = config["agent_time_offset"]
        agent = Agent()

        error = Exception()

        def raise_(*args, **kwargs):
            raise error

        with nested(
            patch.object(ntplib.NTPClient, "request", raise_),
            patch.object(time, "time", return_value=5),
            patch.object(ntplog, "warning"),
        ) as (_, _, warning):
            agent.system_data(requery_timeoffset=True)

        self.assertEqual(config["agent_time_offset"], offset)
        warning.assert_called_with(
            "Failed to determine network time: %s", error)


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
    POP_CONFIG_KEYS = ["master_api"]

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


class TestSigintHandler(TestCase):
    POP_CONFIG_KEYS = ["run_control_file"]

    def setUp(self):
        super(TestSigintHandler, self).setUp()
        self.fake_run_control_file = os.urandom(8).encode("hex")
        config["run_control_file"] = self.fake_run_control_file

    def test_sigint_removes_file(self):
        agent = Agent()

        with nested(
            patch.object(utility, "remove_file"),
            patch.object(agent, "stop")
        ) as (remove_file, _):
            agent.sigint_handler()

        remove_file.assert_called_with(
            self.fake_run_control_file, retry_on_exit=True, raise_=False)

    def test_sigint_callback(self):
        agent = Agent()
        stop_deferred = Deferred()

        with nested(
            patch.object(agent, "stop", return_value=stop_deferred),
            patch.object(reactor, "stop", return_value=None)
        ) as (agent_stop, reactor_stop):
            stop_deferred.callback(None)
            agent.sigint_handler()

        agent_stop.assert_called_once()
        reactor_stop.assert_called_once()

    def test_sigint_errback(self):
        agent = Agent()
        stop_deferred = Deferred()
        failure = Exception("foobar")

        with nested(
            patch.object(svclog, "error"),
            patch.object(agent, "stop", return_value=stop_deferred),
            patch.object(reactor, "stop", return_value=None),
            patch.object(sys, "exit", return_value=None),
        ) as (error_log, agent_stop, reactor_stop, sys_exit):
            stop_deferred.errback(failure)
            agent.sigint_handler()

        error_log.assert_called_once()
        agent_stop.assert_called_once()
        reactor_stop.assert_called_once()

        # Manually test the call args because assert_called_with would
        # require the exact instance of the Failure object.
        self.assertEqual(
            error_log.call_args[0][0],
            "Error while attempting to shutdown the agent: %s")
        self.assertIsInstance(error_log.call_args[0][1], Failure)
        sys_exit.assert_called_with(1)


class TestStop(TestCase):
    POP_CONFIG_KEYS = ["run_control_file"]

    def setUp(self):
        super(TestStop, self).setUp()
        self.fake_agent_lock_file = os.urandom(6).encode("hex")
        config["agent_lock_file"] = self.fake_agent_lock_file

    @inlineCallbacks
    def test_already_stopped(self):
        agent = Agent()
        agent.stopped = True

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(svclog, "warning")
        ) as (stop_lock_acquire, stop_lock_release, warning_log):
            result = yield agent.stop()

        self.assertIsNone(result)
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()
        warning_log.assert_called_once()
        warning_log.assert_called_with("Agent is already stopped")

    @inlineCallbacks
    def test_removes_file(self):
        agent = Agent()
        post_shutdown_deferred = Deferred()
        post_shutdown_deferred.callback(None)

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(utility, "remove_file"),
            patch.object(
                agent, "post_shutdown_to_master",
                return_value=post_shutdown_deferred)
        ) as (
            stop_lock_acquire, stop_lock_release, remove_file, _
        ):
            result = yield agent.stop()

        self.assertIsNone(result)
        remove_file.assert_called_with(
            self.fake_agent_lock_file, retry_on_exit=True, raise_=False)
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()

    @inlineCallbacks
    def test_no_agent_api(self):
        agent = Agent()
        post_shutdown_deferred = Deferred()
        post_shutdown_deferred.callback(None)

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(svclog, "warning"),
            patch.object(agent, "agent_api", return_value=None),
            patch.object(
                agent, "post_shutdown_to_master",
                return_value=post_shutdown_deferred)
        ) as (
            stop_lock_acquire, stop_lock_release, warning_log,
            agent_api, post_shutdown
        ):
            result = yield agent.stop()

        self.assertIsNone(result)
        agent_api.assert_called_once()
        warning_log.assert_called_with(
            "Cannot post shutdown, agent_api() returned None")
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()
        self.assertFalse(post_shutdown.called)

    @inlineCallbacks
    def test_stops_and_removes_jobtypes(self):
        agent = Agent()
        post_shutdown_deferred = Deferred()
        post_shutdown_deferred.callback(None)

        jobtype_a = Mock(
            stop=Mock(),
            _has_running_processes=Mock(return_value=False))
        jobtype_b = Mock(
            stop=Mock(),
            _has_running_processes=Mock(return_value=False))
        config["jobtypes"] = {
            uuid.uuid4(): jobtype_a,
            uuid.uuid4(): jobtype_b
        }

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(svclog, "warning"),
            patch.object(
                agent, "post_shutdown_to_master",
                return_value=post_shutdown_deferred)
        ) as (
            stop_lock_acquire, stop_lock_release, warning_log,
            post_shutdown
        ):
            result = yield agent.stop()

        self.assertIsNone(result)
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()
        post_shutdown.assert_called_once()
        jobtype_a.stop.assert_called_once()
        jobtype_b.stop.assert_called_once()
        jobtype_a._has_running_processes.assert_called_once()
        jobtype_b._has_running_processes.assert_called_once()
        self.assertEqual(config["jobtypes"], {})
        warning_log.assert_any_call(
            "%r has not removed itself, forcing removal", jobtype_a)
        warning_log.assert_any_call(
            "%r has not removed itself, forcing removal", jobtype_b)

    @inlineCallbacks
    def test_removes_jobtype_on_stop_error(self):
        agent = Agent()
        post_shutdown_deferred = Deferred()
        post_shutdown_deferred.callback(None)

        # These 'jobtypes' will cause an error which should
        # cause stop() to remove them from the dictionary.
        id_a = uuid.uuid4()
        id_b = uuid.uuid4()
        config["jobtypes"] = {
            id_a: None,
            id_b: None
        }

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(agent, "agent_api", return_value=None)

        ) as (
            stop_lock_acquire, stop_lock_release, agent_api
        ):
            result = yield agent.stop()

        self.assertIsNone(result)
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()
        self.assertEqual(config["jobtypes"], {})

    @inlineCallbacks
    def test_timeout(self):
        agent = Agent()
        config["agent_shutdown_timeout"] = -50
        jobtype_a = Mock(
            stop=Mock(),
            _has_running_processes=Mock(return_value=False))
        jobtype_b = Mock(
            stop=Mock(),
            _has_running_processes=Mock(return_value=False))
        config["jobtypes"] = {
            uuid.uuid4(): jobtype_a,
            uuid.uuid4(): jobtype_b
        }

        with nested(
            patch.object(agent.stop_lock, "acquire"),
            patch.object(agent.stop_lock, "release"),
            patch.object(agent, "agent_api", return_value=None),
        ) as (
            stop_lock_acquire, stop_lock_release,
            agent_api
        ):
            result = yield agent.stop()

        self.assertIsNone(result)
        agent_api.assert_called_once()
        stop_lock_acquire.assert_called_once()
        stop_lock_release.assert_called_once()


<<<<<<< HEAD
class TestShouldReannounce(TestCase):
    POP_CONFIG_KEYS = [
        "agent_master_reannounce", "last_master_contact"
    ]

    def test_should_reannounce_locked(self):
        agent = Agent()
        agent.reannouce_lock.acquire()
        self.assertFalse(agent.should_reannounce())

    def test_should_reannounce_shutting_down(self):
        agent = Agent()
        agent.shutting_down = True
        self.assertFalse(agent.should_reannounce())

    def test_should_reannounce_master_contacted(self):
        config["last_master_contact"] = datetime.utcnow() - timedelta(seconds=5)
        config["agent_master_reannounce"] = 3
        agent = Agent()
        self.assertTrue(agent.should_reannounce())

    def test_returns_true_if_not_contacted(self):
        agent = Agent()
        with patch.object(config, "master_contacted", return_value=None):
            self.assertTrue(agent.should_reannounce())


class TestReannounce(TestCase):
    POP_CONFIG_KEYS = [
        "last_announce", "last_master_contact"
    ]

    def setUp(self):
        super(TestReannounce, self).setUp()
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
            "state": config["state"],
            "current_assignments": {},
            "free_ram": 424242
        }

    @inlineCallbacks
    def tearDown(self):
        super(TestReannounce, self).tearDown()
        self.free_ram_mock.stop()
        yield self.server.loseConnection()

    @inlineCallbacks
    def test_should_not_reannounce(self):
        agent = Agent()

        with nested(
            patch.object(agent, "should_reannounce", return_value=False),
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release")
        ) as (_, acquire_lock, release_lock):
            result = yield agent.reannounce(force=False)

        self.assertIsNone(result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()

    @inlineCallbacks
    def test_reannounce_force(self):
        self.fake_api.code = OK
        agent = Agent()

        with nested(
            patch.object(agent, "should_reannounce", return_value=False),
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(svclog, "debug")
        ) as (_, acquire_lock, release_lock, debug_log):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        debug_log.assert_called_once_with(
            "Announcing %s to master", config["agent_hostname"])

    @inlineCallbacks
    def test_ok(self):
        self.fake_api.code = OK
        agent = Agent()

        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release")
        ) as (acquire_lock, release_lock):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()

    @inlineCallbacks
    def test_internal_server_error_shutting_down(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR
        agent = Agent()
        agent.shutting_down = True

        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(svclog, "warning")
        ) as (acquire_lock, release_lock, warning_log):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        warning_log.assert_called_once_with(
            "Could not announce to master. Not retrying because of pending "
            "shutdown."
        )

    @inlineCallbacks
    def test_internal_server_error_retry(self):
        self.fake_api.code = INTERNAL_SERVER_ERROR
        agent = Agent()
        agent.shutting_down = False

        def shutdown():
            agent.shutting_down = True

        reactor.callLater(3, shutdown)

        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(svclog, "warning")
        ) as (acquire_lock, release_lock, warning_log):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        warning_log.assert_any_call(
            "Could not announce self to the master server, internal server "
            "error: %s.  Retrying in %s seconds.", self.normal_result, 1.0
        )
        self.assertGreaterEqual(warning_log.call_count, 2)

    @inlineCallbacks
    def test_not_found(self):
        self.fake_api.code = NOT_FOUND
        agent = Agent()

        post_deferred = Deferred()
        post_deferred.callback(None)
        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(
                    agent, "post_agent_to_master", return_value=post_deferred),
        ) as (acquire_lock, release_lock, post_agent):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        post_agent.assert_called_once()

    @inlineCallbacks
    def test_bad_request(self):
        self.fake_api.code = BAD_REQUEST
        agent = Agent()

        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(svclog, "error")
        ) as (acquire_lock, release_lock, error_log):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        error_log.assert_called_once_with(
            "Failed to announce self to the master, bad "
            "request: %s.  This request will not be retried.",
            self.normal_result
        )

    @inlineCallbacks
    def test_other(self):
        self.fake_api.code = 42
        agent = Agent()

        with nested(
            patch.object(agent.reannouce_lock, "acquire"),
            patch.object(agent.reannouce_lock, "release"),
            patch.object(svclog, "error")
        ) as (acquire_lock, release_lock, error_log):
            result = yield agent.reannounce(force=True)

        self.assertEqual(result, self.normal_result)
        acquire_lock.assert_called_once()
        release_lock.assert_called_once()
        error_log.assert_called_once_with(
            "Unhandled error when posting self to the "
            "master: %s (code: %s).  This request will not be "
            "retried.",
            self.normal_result, 42
        )


class TestBuildHTTPResource(TestCase):
    def test_root_resources(self):
        agent = Agent()
        resource = agent.build_http_resource()

        root = resource.children.pop("")
        self.assertIsInstance(root, Index)

        api = resource.children.pop("api")
        self.assertIsInstance(api, APIRoot)

        configuration = resource.children.pop("configuration")
        self.assertIsInstance(configuration, Configuration)

        favicon = resource.children.pop("favicon.ico")
        self.assertIsInstance(favicon, StaticPath)
        self.assertEqual(
            favicon.path, join(config["agent_static_root"], "favicon.ico"))
        self.assertEqual(favicon.defaultType, "image/x-icon")

        static = resource.children.pop("static")
        self.assertIsInstance(static, StaticPath)
        self.assertEqual(static.path, config["agent_static_root"])

        # We should not have anything remaining.  If we do it means there's
        # a new root resource we're not testing above.
        self.assertNot(resource.children)

    def test_api_children(self):
        agent = Agent()
        resource = agent.build_http_resource()

        api = resource.children.pop("api")
        self.assertIsInstance(api, APIRoot)

        v1 = api.children.pop("v1")
        self.assertIsInstance(v1, APIRoot)

        versions = api.children.pop("versions")
        self.assertIsInstance(versions, Versions)

        self.assertNot(api.children)

    def test_endpoints(self):
        agent = Agent()
        resource = agent.build_http_resource()
        api = resource.children.pop("api")
        v1 = api.children.pop("v1")

        assign = v1.children.pop("assign")
        self.assertIsInstance(assign, Assign)
        self.assertIs(assign.agent, agent)

        tasks = v1.children.pop("tasks")
        self.assertIsInstance(tasks, Tasks)

        configapi = v1.children.pop("config")
        self.assertIsInstance(configapi, Config)

        task_logs = v1.children.pop("task_logs")
        self.assertIsInstance(task_logs, TaskLogs)

        status = v1.children.pop("status")
        self.assertIsInstance(status, Status)

        stop = v1.children.pop("stop")
        self.assertIsInstance(stop, Stop)

        restart = v1.children.pop("restart")
        self.assertIsInstance(restart, Restart)

        update = v1.children.pop("update")
        self.assertIsInstance(update, Update)

        self.assertNot(v1.children)

