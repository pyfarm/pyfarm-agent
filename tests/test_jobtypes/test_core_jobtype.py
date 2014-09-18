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

import sys
from os import urandom
from uuid import UUID, uuid4
from os.path import isdir, isfile

from twisted.internet.defer import Deferred
from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, requires_master, create_jobtype
from pyfarm.agent.utility import uuid
from pyfarm.jobtypes.core.jobtype import JobType
from pyfarm.jobtypes.core.internals import ProcessData


def fake_assignment():
    assignment_id = uuid4()
    assignment = {
        "id": assignment_id,
        "job": {
            "id": 1,
            "by": 1,
            "title": "Hello World",
            "data": {"a": True, "b": False, "c": None, "d": 1}},
        "jobtype": {
            "name": "Foo",
            "version": 1},
        "tasks": [{"id": 1, "frame": 1, "attempt": 1},
                  {"id": 1, "frame": 1, "attempt": 1}]}
    config["current_assignments"][assignment_id] = assignment
    return assignment


class FakeProcessProtocol(object):
    def __init__(self):
        self.uuid = uuid()


class TestSchema(TestCase):
    def test_attribute(self):
        self.assertIsInstance(JobType.ASSIGNMENT_SCHEMA, Schema)

    def test_schema(self):
        JobType.ASSIGNMENT_SCHEMA(fake_assignment())


class TestInit(TestCase):
    def test_uuid(self):
        job = JobType(fake_assignment())
        self.assertIsInstance(job.uuid, UUID)

    def test_sets_config(self):
        job = JobType(fake_assignment())
        self.assertIn(job.uuid, config["jobtypes"])
        self.assertIs(config["jobtypes"][job.uuid], job)

    def test_assignment(self):
        assignment = fake_assignment()
        job = JobType(assignment)
        self.assertIsInstance(job.assignment, ImmutableDict)
        assignment["jobtype"].pop("id")
        self.assertEqual(job.assignment, assignment)

    def test_attributes(self):
        job = JobType(fake_assignment())
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


class TestJobTypeLoad(TestCase):
    def test_schema(self):
        with self.assertRaises(MultipleInvalid):
            JobType.load({})

    @requires_master
    def test_load(self):
        self.skipTest("NOT IMPLEMENTED")
        # TODO: There's something wrong with this test the causes it to
        # fail in different ways depending on invocation.
        #
        # self.assertIsNotNone(JobType.CACHE_DIRECTORY)
        # self.assertTrue(isdir(JobType.CACHE_DIRECTORY))
        # classname = "AgentUnittest" + urandom(8).encode("hex")
        # created = create_jobtype(classname=classname)
        # finished = Deferred()
        #
        # def check_loaded(jobtype, module_name, jobtype_data):
        #     self.assertEqual(jobtype.__name__, classname)
        #     self.assertIs(
        #         getattr(sys.modules[module_name], jobtype.__name__), jobtype)
        #     self.assertTrue(isfile(sys.modules[module_name].__file__))
        #
        #     with open(sys.modules[module_name].__file__, "r") as cache_file:
        #         cached_code = cache_file.read()
        #
        #     self.assertEqual(jobtype_data["code"], cached_code)
        #
        # def jobtype_created(data):
        #     assignment = fake_assignment().copy()
        #     assignment["jobtype"].update(
        #         name=data["name"], version=data["version"])
        #     loaded = JobType.load(assignment)
        #     module_name = "pyfarm.jobtypes.cached.%s%s%s" % (
        #         classname, data["name"], data["version"])
        #     loaded.addCallback(check_loaded, module_name, data)
        #     loaded.chainDeferred(finished)
        #
        # created.addCallback(jobtype_created)
        # return finished
