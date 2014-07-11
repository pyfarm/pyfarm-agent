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

from os import urandom
from random import randint
from uuid import UUID

try:
    from httplib import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE


from twisted.web.server import NOT_DONE_YET
from voluptuous import Invalid

from pyfarm.agent.config import config
from pyfarm.agent.http.api.assign import Assign, validate_environment
from pyfarm.agent.sysinfo.memory import total_ram
from pyfarm.agent.sysinfo.cpu import total_cpus
from pyfarm.agent.testutil import BaseAPITestCase, TestCase
from pyfarm.jobtypes.core.jobtype import JobType

FAKE_JOBTYPE = """
from twisted.internet.defer import Deferred
from pyfarm.jobtypes.core.jobtype import JobType

class FakeJobType(JobType):
    def __init__(self, assignment):
        super(FakeJobType, self).__init__(assignment)
        self.star_called = False
        self.started = Deferred()

    def start(self):
        self.star_called = True
        return self.started
"""

FAKE_JOBTYPE_BAD_TYPE = """
from twisted.internet.defer import Deferred

class FakeJobType(object):
    def __init__(self, assignment):
        self.star_called = False
        self.started = Deferred()

    def start(self):
        self.star_called = True
        return self.started
"""


class TestValidateEnvironment(TestCase):
    def test_type(self):
        with self.assertRaisesRegexp(Invalid, "Expected a dictionary"):
            validate_environment(None)

    def test_value(self):
        with self.assertRaisesRegexp(Invalid, "Value.*string.*"):
            validate_environment({"foo": None})

    def test_key(self):
        with self.assertRaisesRegexp(Invalid, "Key.*string.*"):
            validate_environment({1: None})


class TestAssign(BaseAPITestCase):
    URI = "/assign"
    CLASS = Assign

    def setUp(self):
        super(TestAssign, self).setUp()
        self.data = {
            "job": {
                "title": urandom(16).encode("hex"),
                "id": randint(0, 1024),
                "by": randint(0, 10)},
            "jobtype": {
                "name": "TestJobType" + urandom(16).encode("hex"),
                "version": randint(1, 256)},
            "tasks": [
                {"id": randint(0, 1024), "frame": randint(0, 1024)},
                {"id": randint(0, 1024), "frame": randint(0, 1024)},
                {"id": randint(0, 1024), "frame": randint(0, 1024)}]}

    def prepare_config(self):
        super(TestAssign, self).prepare_config()
        config.update({
            "cpus": randint(1, 16),
            "agent-id": randint(1, 2048)})

    def test_restarting(self):
        config["restart_requested"] = True
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, SERVICE_UNAVAILABLE)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(
            request.response()["error"],
            "Agent cannot accept assignments because of a pending restart")

    def test_agent_id_not_set(self):
        config.pop("agent-id", None)
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, SERVICE_UNAVAILABLE)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(
            request.response()["error"],
            "agent-id has not been set in the config")

    def test_not_enough_ram(self):
        self.data["job"]["ram"] = total_ram() * 10
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, BAD_REQUEST)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.response()["error"], "Not enough ram")

    def test_not_enough_cpus(self):
        self.data["job"]["cpus"] = int(total_cpus() * 10)
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, BAD_REQUEST)
        self.assertEqual(result, NOT_DONE_YET)
        response = request.response()
        self.assertEqual(response["agent_cpus"], config["cpus"])
        self.assertEqual(response["requires_cpus"], int(total_cpus() * 10))
        self.assertEqual(response["error"], "Not enough cpus")

    def test_duplicate_task(self):
        # Add the assignments in data to the config so we can make sure
        # the endpoint will reject the request
        tasks = []
        config["current_assignments"] = {}
        assignment = config["current_assignments"][
            self.data["job"]["id"]] = {"tasks": []}
        for task in self.data["tasks"]:
            tasks.append(task["id"])
            assignment["tasks"].append(task)

        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, CONFLICT)
        self.assertEqual(result, NOT_DONE_YET)
        response = request.response()
        self.assertEqual(set(response["duplicate_tasks"]), set(tasks))
        self.assertEqual(response["error"], "Double assignment of tasks")

    def test_accepted(self):
        # Cache the fake job type and make sure the config
        # turns off caching
        jobtype = {
            "classname": "FakeJobType",
            "code": FAKE_JOBTYPE,
            "name": self.data["jobtype"]["name"],
            "version": self.data["jobtype"]["version"]}
        JobType.cache[(self.data["jobtype"]["name"],
                       self.data["jobtype"]["version"])] = (jobtype, None)
        config.update(
            jobtype_enable_cache=False,
            current_assignments={})
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, ACCEPTED)
        response = request.response()
        response_id = UUID(response["id"])
        self.assertIn(response_id, config["current_assignments"])
        self.assertEqual(
            config["current_assignments"][response_id], self.data)

    def test_accepted_type_error(self):
        # Cache the fake job type and make sure the config
        # turns off caching
        jobtype = {
            "classname": "FakeJobType",
            "code": FAKE_JOBTYPE_BAD_TYPE,
            "name": self.data["jobtype"]["name"],
            "version": self.data["jobtype"]["version"]}
        JobType.cache[(self.data["jobtype"]["name"],
                       self.data["jobtype"]["version"])] = (jobtype, None)

        # with self.assertRaises(TypeError):
        config.update(
            jobtype_enable_cache=False,
            current_assignments={})
        request = self.post(
            data=self.data,
            headers={"User-Agent": config["master_user_agent"]})
        assign = Assign()
        result = assign.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.code, ACCEPTED)
        response = request.response()
        response_id = UUID(response["id"])
        self.assertNotIn(response_id, config["current_assignments"])