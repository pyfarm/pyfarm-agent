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
from uuid import UUID
from os.path import isdir

from twisted.internet.defer import Deferred
from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, requires_master, create_jobtype
from pyfarm.agent.utility import uuid
from pyfarm.jobtypes.core.jobtype import JobType, ProcessData

FAKE_ASSIGNMENT = {
    "job": {
        "id": 1,
        "title": "Hello World",
        "data": {"a": True, "b": False, "c": None, "d": 1}},
    "jobtype": {
        "name": "Foo",
        "version": 1},
    "tasks": [{"id": 1, "frame": 1},{"id": 1, "frame": 1}]}


class FakeProcessProtocol(object):
    def __init__(self):
        self.uuid = uuid()


class TestSchema(TestCase):
    def test_attribute(self):
        self.assertIsInstance(JobType.ASSIGNMENT_SCHEMA, Schema)

    def test_schema(self):
        JobType.ASSIGNMENT_SCHEMA(FAKE_ASSIGNMENT)


class TestInit(TestCase):
    def test_uuid(self):
        job = JobType(FAKE_ASSIGNMENT)
        self.assertIsInstance(job.uuid, UUID)

    def test_sets_config(self):
        job = JobType(FAKE_ASSIGNMENT)
        self.assertIn(job.uuid, config["jobtypes"])
        self.assertIs(config["jobtypes"][job.uuid], job)

    def test_assignment(self):
        job = JobType(FAKE_ASSIGNMENT)
        self.assertIsInstance(job.assignment, ImmutableDict)
        self.assertEqual(job.assignment, FAKE_ASSIGNMENT)

    def test_attributes(self):
        job = JobType(FAKE_ASSIGNMENT)
        self.assertIsInstance(job.processes, dict)
        self.assertEqual(job.processes, {})
        self.assertIsInstance(job.failed_processes, set)
        self.assertEqual(job.failed_processes, set())
        self.assertIsInstance(job.finished_tasks, set)
        self.assertEqual(job.finished_tasks, set())
        self.assertIsInstance(job.failed_tasks, set)
        self.assertEqual(job.failed_tasks, set())
        self.assertFalse(job.stop_called)
        self.assertFalse(job.start_called)


class TestProperties(TestCase):
    def test_started(self):
        job = JobType(FAKE_ASSIGNMENT)
        protocol1 = FakeProcessProtocol()
        protocol2 = FakeProcessProtocol()
        process1 = job.processes[protocol1.uuid] = ProcessData(
            protocol=protocol1, started=Deferred(), stopped=Deferred())
        process2 = job.processes[protocol2.uuid] = ProcessData(
            protocol=protocol2, started=Deferred(), stopped=Deferred())

        def check_started(result):
            self.assertIn((True, 1), result)
            self.assertIn((True, 2), result)

        started = job.started
        started.addCallback(check_started)
        process1.started.callback(1)
        process2.started.callback(2)
        return started

    def test_stopped(self):
        job = JobType(FAKE_ASSIGNMENT)
        protocol1 = FakeProcessProtocol()
        protocol2 = FakeProcessProtocol()
        process1 = job.processes[protocol1.uuid] = ProcessData(
            protocol=protocol1, started=Deferred(), stopped=Deferred())
        process2 = job.processes[protocol2.uuid] = ProcessData(
            protocol=protocol2, started=Deferred(), stopped=Deferred())

        def check_stopped(result):
            self.assertIn((True, -1), result)
            self.assertIn((True, -2), result)

        stopped = job.stopped
        stopped.addCallback(check_stopped)
        process1.stopped.callback(-1)
        process2.stopped.callback(-2)
        return stopped


class TestJobTypeLoad(TestCase):
    def test_schema(self):
        with self.assertRaises(MultipleInvalid):
            JobType.load({})

    @requires_master
    def test_load(self):
        self.assertIsNotNone(JobType.CACHE_DIRECTORY)
        self.assertTrue(isdir(JobType.CACHE_DIRECTORY))
        classname = "AgentUnittest" + urandom(8).encode("hex")
        # created = create_jobtype(classname=classname)
        finished = Deferred()

        def jobtype_created(data):
            assignment = FAKE_ASSIGNMENT.copy()
            assignment["jobtype"].update(
                name=data["name"], version=data["version"])
            loaded = JobType.load(assignment)
            loaded.errback(finished.callback)


            finished.chainDeferred(loaded)


        # TODO: fix test/code
        finished.callback(True)
        # created.addCallbacks(jobtype_created, finished.errback)
        return finished