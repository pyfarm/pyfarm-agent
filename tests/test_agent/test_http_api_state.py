# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from json import loads
from datetime import datetime, timedelta

try:
    from httplib import ACCEPTED, OK, BAD_REQUEST
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, OK, BAD_REQUEST

import psutil
from twisted.internet.defer import Deferred
from twisted.web.server import NOT_DONE_YET

from pyfarm.core.enums import AgentState
from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.api.state import Stop, Status
from pyfarm.agent.sysinfo import memory
from pyfarm.agent.testutil import TestCase, FakeRequest


class FakeAgent(object):
    def __init__(self, stopped=None):
        if stopped is None:
            stopped = Deferred()
        self.stopped = stopped

    def stop(self):
        if isinstance(self.stopped, Deferred):
            self.stopped.callback(None)
        return self.stopped


class TestStop(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.agent = config["agent"] = FakeAgent()

    def test_leaf(self):
        self.assertFalse(Stop.isLeaf)

    def test_parent(self):
        self.assertIsInstance(Stop(), APIResource)

    def test_invalid_type_for_data(self):
        request = FakeRequest(self)
        stop = Stop()
        result = stop.post(request=request, data=None)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, BAD_REQUEST)
        self.assertTrue(request.finished)

    def test_stops_agent(self):
        request = FakeRequest(self)
        stop = Stop()
        result = stop.post(request=request, data={})
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, ACCEPTED)
        self.assertTrue(request.finished)
        return self.agent.stopped

    def test_stops_and_waits_for_agent(self):
        request = FakeRequest(self)
        stop = Stop()
        result = stop.post(request=request, data={"wait": True})
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)
        self.assertTrue(request.finished)
        return self.agent.stopped

    def test_stops_if_agent_returns_none(self):
        config["agent"] = FakeAgent(stopped=False)
        request = FakeRequest(self)
        stop = Stop()
        result = stop.post(request=request, data={"wait": True})
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)
        self.assertTrue(request.finished)


class TestStatus(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        config.update(
            state=AgentState.ONLINE,
            pids=[1, 2, 3],
            start=time.time())

    def test_leaf(self):
        self.assertFalse(Status.isLeaf)

    def test_parent(self):
        self.assertIsInstance(Status(), APIResource)

    def test_get_requires_no_input(self):
        status = Status()
        status.get()

    def test_get_result(self):
        process = psutil.Process()
        direct_child_processes = len(process.children(recursive=False))
        all_child_processes = len(process.children(recursive=True))
        grandchild_processes = all_child_processes - direct_child_processes

        # Determine the last time we talked to the master (if ever)
        contacted = config.master_contacted(update=False)
        if isinstance(contacted, datetime):
            contacted = datetime.utcnow() - contacted

        # Determine the last time we announced ourselves to the
        # master (if ever)
        last_announce = config.get("last_announce", None)
        if isinstance(last_announce, datetime):
            last_announce = datetime.utcnow() - last_announce

        expected_data = {
            "state": config["state"],
            "agent_hostname": config["agent_hostname"],
            "free_ram": int(memory.ram_free()),
            "agent_process_ram": int(memory.process_memory()),
            "consumed_ram": int(memory.total_consumption()),
            "child_processes": direct_child_processes,
            "grandchild_processes": grandchild_processes,
            "pids": config["pids"],
            "id": config.get("agent-id", None),
            "agent_systemid": config["agent_systemid"],
            "last_master_contact": contacted,
            "last_announce": last_announce,
            "agent_lock_file": config["agent_lock_file"],
            "uptime": timedelta(
               seconds=time.time() - config["start"]).total_seconds(),
            "jobs": list(config["jobtypes"].keys())}
        status = Status()
        data = loads(status.get())

        # Pop off and test keys which are 'close'
        self.assertApproximates(
            data.pop("uptime"), expected_data.pop("uptime"), .5)
        self.assertApproximates(
            data.pop("free_ram"), expected_data.pop("free_ram"), 5)

        self.assertEqual(data, expected_data)
