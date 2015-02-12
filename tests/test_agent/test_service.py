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
import time
import uuid
from platform import platform

try:
    from httplib import OK, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED

from mock import patch
from twisted.internet.defer import Deferred, DeferredList, inlineCallbacks

from pyfarm.agent.sysinfo.system import operating_system
from pyfarm.agent.sysinfo import cpu
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.config import config
from pyfarm.agent.service import Agent
from pyfarm.agent.sysinfo import network, graphics


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
    def test_now(self):
        agent = Agent()
        deferred = Deferred()
        agent.repeating_call(
            0, lambda: deferred.callback(None), now=True, repeat=0)

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

        agent.repeating_call(0, callback, now=False, repeat=2)
        yield DeferredList([deferred1, deferred2])
        self.assertFalse(deferred3.called)
        agent.repeating_call(0, callback, now=False, repeat=1)
        yield deferred3

    @inlineCallbacks
    def test_timed(self):
        agent = Agent()
        deferred1 = Deferred()
        deferred2 = Deferred()

        def callback():
            if not deferred1.called:
                deferred1.callback(None)

            elif not deferred2.called:
                deferred2.callback(None)

        start = time.time()
        agent.repeating_call(.5, callback, now=False, repeat=2)
        yield DeferredList([deferred1, deferred2])
        elapsed = time.time() - start
        self.assertTrue(
            1 <= elapsed <= 2, "Test did not take between 1 and 2 seconds")


class TestRunAgent(TestCase):
    def test_created(self):
        self.skipTest("NOT IMPLEMENTED")

    def test_updated(self):
        self.skipTest("NOT IMPLEMENTED")
