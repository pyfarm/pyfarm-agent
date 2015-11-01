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
from contextlib import nested
from uuid import UUID, uuid4

from mock import Mock, patch
from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WINDOWS
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.jobtypes.core.internals import USER_GROUP_TYPES
from pyfarm.jobtypes.core.jobtype import JobType, CommandData
from pyfarm.jobtypes.core.log import logpool

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


# NOTE: These tests test code flow rather than function
class TestJobTypeHandleStdoutLine(TestCase):
    def test_preprocess_can_stop_handling(self):
        jobtype = JobType(fake_assignment())
        protocol = Mock(id=1)

        with nested(
            patch.object(jobtype, "preprocess_stdout_line", return_value=False),
            patch.object(jobtype, "format_stdout_line"),
        ) as (_, mocked_format):
            jobtype.handle_stdout_line(protocol, "stdout 1")

        self.assertEqual(mocked_format.call_count, 0)

    def test_preprocess_replaces_output(self):
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)
        protocol = Mock(id=2)

        with nested(
            patch.object(jobtype, "preprocess_stdout_line", return_value="foo"),
            patch.object(jobtype, "format_stdout_line"),
        ) as (_, mocked):
            jobtype.handle_stdout_line(protocol, "stdout 2")

        mocked.assert_called_with(protocol, "foo")

    def test_format_replaces_output(self):
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)
        protocol = Mock(id=3)

        with nested(
            patch.object(jobtype, "format_stdout_line", return_value="bar"),
            patch.object(jobtype, "log_stdout_line"),
            patch.object(jobtype, "process_stdout_line"),
        ) as (_, log_mock, process_mock):
            jobtype.handle_stdout_line(protocol, "stdout 3")

        log_mock.assert_called_with(protocol, "bar")
        process_mock.assert_called_with(protocol, "bar")


# NOTE: These tests test code flow rather than function
class TestJobTypeHandleStderrLine(TestCase):
    def test_preprocess_can_stop_handling(self):
        jobtype = JobType(fake_assignment())
        protocol = Mock(id=1)

        with nested(
            patch.object(jobtype, "preprocess_stderr_line", return_value=False),
            patch.object(jobtype, "format_stderr_line"),
        ) as (_, mocked_format):
            jobtype.handle_stderr_line(protocol, "stderr 1")

        self.assertEqual(mocked_format.call_count, 0)

    def test_preprocess_replaces_output(self):
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)
        protocol = Mock(id=2)

        with nested(
            patch.object(jobtype, "preprocess_stderr_line", return_value="foo"),
            patch.object(jobtype, "format_stderr_line"),
        ) as (_, mocked):
            jobtype.handle_stderr_line(protocol, "stderr 2")

        mocked.assert_called_with(protocol, "foo")

    def test_format_replaces_output(self):
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)
        protocol = Mock(id=3)

        with nested(
            patch.object(jobtype, "format_stderr_line", return_value="bar"),
            patch.object(jobtype, "log_stderr_line"),
            patch.object(jobtype, "process_stderr_line"),
        ) as (_, log_mock, process_mock):
            jobtype.handle_stderr_line(protocol, "stderr 3")

        log_mock.assert_called_with(protocol, "bar")
        process_mock.assert_called_with(protocol, "bar")