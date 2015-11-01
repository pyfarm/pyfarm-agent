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

import os
import re
from uuid import UUID, uuid4

from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WINDOWS
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.jobtypes.core.internals import USER_GROUP_TYPES
from pyfarm.jobtypes.core.jobtype import (
    FROZEN_ENVIRONMENT, JobType, CommandData)

IS_ADMIN = is_administrator()


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
        self.uuid = uuid4()


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


class TestCommandData(TestCase):
    def test_set_basic_attributes(self):
        command = os.urandom(12)
        arguments = (1, None, True, "foobar")
        data = CommandData(command, *arguments)
        self.assertEqual(data.command, command)

        for argument in data.arguments:
            self.assertIsInstance(argument, str)

        self.assertIsInstance(data.arguments, tuple)
        self.assertEqual(data.arguments, ("1", "None", "True", "foobar"))
        self.assertIsNone(data.env)
        self.assertIsNone(data.cwd)
        self.assertIsNone(data.user)
        self.assertIsNone(data.group)

    def test_set_kwargs(self):
        data = CommandData(
            "", env={"foo": "bar"}, cwd="/", user="usr", group="grp")
        self.assertEqual(data.env, {"foo": "bar"})
        self.assertEqual(data.cwd, "/")
        self.assertEqual(data.user, "usr")
        self.assertEqual(data.group, "grp")

    def test_unknown_kwarg(self):
        with self.assertRaises(ValueError):
            CommandData("", foobar=True)

    def test_validate_command_type(self):
        with self.assertRaisesRegexp(
                TypeError, re.compile(".*string.*command.*")):
            CommandData(None).validate()

    def test_validate_env_type(self):
        with self.assertRaisesRegexp(
                TypeError, re.compile(".*dictionary.*env.*")):
            CommandData("", env=1).validate()

    def test_user_group_types(self):
        self.assertEqual(
            USER_GROUP_TYPES,
            tuple(list(STRING_TYPES) + list(INTEGER_TYPES) + [type(None)]))

    def test_validate_user_type(self):
        with self.assertRaisesRegexp(
                TypeError, re.compile(".*user.*")):
            CommandData("", user=1.0).validate()

    def test_validate_group_type(self):
        with self.assertRaisesRegexp(
                TypeError, re.compile(".*group.*")):
            CommandData("", group=1.0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(IS_ADMIN, "Is Administrator")
    def test_validate_change_user_non_admin_failure(self):
        with self.assertRaises(EnvironmentError):
            CommandData("", user=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(IS_ADMIN, "Is Administrator")
    def test_validate_change_group_non_admin_failure(self):
        with self.assertRaises(EnvironmentError):
            CommandData("", group=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(not IS_ADMIN, "Not Administrator")
    def test_validate_change_user_admin(self):
        CommandData("", user=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(not IS_ADMIN, "Not Administrator")
    def test_validate_change_group_admin(self):
        CommandData("", group=0).validate()

    def test_validate_cwd_default(self):
        initial_cwd = os.getcwd()
        config["agent_chdir"] = None
        data = CommandData("")
        data.validate()
        self.assertEqual(data.cwd, os.getcwd())
        self.assertEqual(initial_cwd, os.getcwd())

    def test_validate_cwd_config(self):
        initial_cwd = os.getcwd()
        testdir, _ = self.create_directory(count=0)
        config["agent_chdir"] = testdir
        data = CommandData("")
        data.validate()
        self.assertEqual(data.cwd, testdir)
        self.assertEqual(initial_cwd, os.getcwd())

    def test_validate_cwd_does_not_exist(self):
        data = CommandData("", cwd=os.urandom(4).encode("hex"))
        with self.assertRaises(OSError):
            data.validate()

    def test_validate_cwd_invalid_type(self):
        data = CommandData("", cwd=1)
        with self.assertRaises(TypeError):
            data.validate()

    def test_set_default_environment_noop(self):
        data = CommandData("", env={"foo": "bar"})
        data.set_default_environment({"a": "b"})
        self.assertEqual(data.env, {"foo": "bar"})

    def test_set_default_environment(self):
        data = CommandData("", env=None)
        data.set_default_environment({"a": "b"})
        self.assertEqual(data.env, {"a": "b"})


class TestJobTypeLoad(TestCase):
    def test_schema(self):
        with self.assertRaises(MultipleInvalid):
            JobType.load({})


class TestJobTypeGetEnvironment(TestCase):
    POP_CONFIG_KEYS = [
        "jobtype_default_environment",
        "jobtype_include_os_environ"
    ]

    def assertEnvironmentContains(self, target, contains):
        for key, value in target.iteritems():
            self.assertIn(key, contains)
            self.assertEqual(target[key], contains[key])

    def assertDoesNotEnvironmentContains(self, target, contains):
        for key, value in target.iteritems():
            self.assertNotIn(key, contains)

    def test_includes_os_environ(self):
        config["jobtype_include_os_environ"] = True
        jobtype = JobType(fake_assignment())
        self.assertEnvironmentContains(
            FROZEN_ENVIRONMENT, jobtype.get_environment())

    def test_does_not_include_os_environ(self):
        config["jobtype_include_os_environ"] = False
        jobtype = JobType(fake_assignment())
        self.assertDoesNotEnvironmentContains(
            FROZEN_ENVIRONMENT, jobtype.get_environment())

    def test_include_config_environment(self):
        config_env = config["jobtype_default_environment"] = {
            "foo": "1", "bar": "2"
        }
        jobtype = JobType(fake_assignment())
        self.assertEnvironmentContains(jobtype.get_environment(), config_env)

    def test_bad_config_raises_type_error(self):
        config["jobtype_default_environment"] = [""]
        jobtype = JobType(fake_assignment())

        with self.assertRaises(TypeError):
            jobtype.get_environment()

    def test_converts_non_string_key_to_string(self):
        jobtype = JobType(fake_assignment())
        config["jobtype_default_environment"] = {
            1: "yes"
        }
        for key in jobtype.get_environment().keys():
            self.assertIsInstance(key, str)

    def test_converts_non_string_value_to_string(self):
        jobtype = JobType(fake_assignment())
        config["jobtype_default_environment"] = {
            "yes": 1
        }
        for value in jobtype.get_environment().values():
            self.assertIsInstance(value, str)