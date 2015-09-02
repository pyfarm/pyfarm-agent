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
from contextlib import nested
from datetime import datetime, timedelta
from json import loads

try:
    from httplib import ACCEPTED, OK, BAD_REQUEST
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, OK, BAD_REQUEST

import mock
import psutil
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor

from pyfarm.core.enums import AgentState
from pyfarm.agent.config import config
from pyfarm.agent.http.api.state import Stop, Status
from pyfarm.agent.sysinfo import memory
from pyfarm.agent.testutil import BaseAPITestCase
from pyfarm.agent.utility import total_seconds


class TestStop(BaseAPITestCase):
    URI = "/stop"
    CLASS = Stop

    def prepare_config(self):
        super(TestStop, self).prepare_config()
        config.update(run_control_file="/tmp/pyfarm/agent/should_be_running")

    def test_invalid_type_for_data(self):
        request = self.post(
            data={"foo": 1},)
        stop = Stop()
        result = stop.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertEqual(len(request.written), 1)
        self.assertIn(
            "Failed to validate the request data against the schema",
            loads(request.written[0])["error"])

    def test_stops_agent(self):
        self.patch(reactor, 'stop', mock.Mock())
        request = self.post(data={})
        stop = Stop()
        result = stop.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, ACCEPTED)
        self.assertTrue(self.agent.stopped)
        return self.agent.stopped

    def test_stops_and_waits_for_agent(self):
        self.patch(reactor, 'stop', mock.Mock())
        request = self.post(data={"wait": True})
        stop = Stop()
        result = stop.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertTrue(self.agent.stopped)
        return self.agent.stopped


class TestStatus(BaseAPITestCase):
    URI = "/status"
    CLASS = Status

    def setUp(self):
        super(TestStatus, self).setUp()
        self._config = config.copy()

    def tearDown(self):
        super(TestStatus, self).tearDown()
        config.update(self._config)

    def prepare_config(self):
        super(TestStatus, self).prepare_config()
        config.update(
            state=AgentState.ONLINE, pids=[1, 2, 3])

    def test_get_requires_no_input(self):
        request = self.get()
        status = Status()
        result = status.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(len(request.written), 1)
        self.assertIsInstance(loads(request.written[0]), dict)

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

        future_time = config["start"] + 30
        process_memory = memory.process_memory()
        total_consumption = memory.total_consumption()
        expected_data = {
            "state": config["state"],
            "agent_hostname": config["agent_hostname"],
            "agent_process_ram": process_memory,
            "consumed_ram": total_consumption,
            "child_processes": direct_child_processes,
            "grandchild_processes": grandchild_processes,
            "pids": config["pids"],
            "agent_id": str(config["agent_id"]),
            "last_master_contact": contacted,
            "last_announce": last_announce,
            "agent_lock_file": config["agent_lock_file"],
            "free_ram": 4242,
            "uptime": total_seconds(
                timedelta(seconds=future_time - config["start"])),
            "jobs": list(config["jobtypes"].keys())}

        request = self.get()
        status = Status()

        with nested(
            mock.patch.object(memory, "free_ram", return_value=4242),
            mock.patch.object(time, "time", return_value=future_time),
            mock.patch.object(
                memory, "process_memory", return_value=process_memory),
            mock.patch.object(
                memory, "total_consumption", return_value=total_consumption)
        ):
            response = status.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(len(request.written), 1)
        self.assertEqual(loads(request.written[0]), expected_data)
