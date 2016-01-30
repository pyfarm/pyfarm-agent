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
from twisted.internet.error import ProcessTerminated, ProcessDone
from twisted.python.failure import Failure
from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WINDOWS
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.jobtypes.core.internals import USER_GROUP_TYPES
from pyfarm.jobtypes.core.jobtype import (
    FROZEN_ENVIRONMENT, JobType, CommandData,
    JobType, CommandData, process_stdout, process_stderr, logger)
from pyfarm.jobtypes.core.log import STDOUT, STDERR, logpool

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

class TestJobTypeFormatError(TestCase):
    def test_process_terminated(self):
        error = Failure(exc_value=Exception())
        error.type = ProcessTerminated
        error.value = Mock(exitCode=1, message="foobar")

        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.format_error(error),
            "Process may have terminated abnormally, please check "
            "the logs.  Message from error "
            "was 'foobar'"
        )

    def test_process_done(self):
        error = Failure(exc_value=Exception())
        error.type = ProcessDone
        error.value = Mock(exitCode=1, message="foobar")

        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.format_error(error),
            "Process has finished with no apparent errors."
        )

    def test_exception(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.format_error(TypeError("foobar 2")),
            "foobar 2"
        )

    def test_string(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.format_error("This is a string"),
            "This is a string"
        )

    def test_none(self):
        jobtype = JobType(fake_assignment())

        with patch.object(logger, "error") as mocked_error:
            self.assertIsNone(jobtype.format_error(None))

        mocked_error.assert_called_with(
            "No error was defined for this failure.")

    def test_other(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(jobtype.format_error(42), str(42))

    def test_other_conversion_problem(self):
        jobtype = JobType(fake_assignment())

        class OtherType(object):
            def __str__(self):
                raise NotImplementedError("__str__ not implemented")

        other = OtherType()

        try:
            str(other)
        except NotImplementedError as str_error:
            pass

        with patch.object(logger, "error") as mocked_error:
            self.assertIsNone(jobtype.format_error(other))

        mocked_error.assert_called_with(
            "Don't know how to format %r as a string.  Error while calling "
            "str(%r) was %s.", other, str(str_error))


class TestJobTypeLogLine(TestCase):
    def prepare_config(self):
        super(TestJobTypeLogLine, self).prepare_config()
        config["jobtype_capture_process_output"] = False

    def test_capure_stdout(self):
        config["jobtype_capture_process_output"] = True
        jobtype = JobType(fake_assignment())

        with patch.object(process_stdout, "info") as mocked:
            jobtype.log_stdout_line(Mock(id=1), "stdout")

        mocked.assert_called_once_with("task %r: %s", 1, "stdout")

    def test_capure_stderr(self):
        config["jobtype_capture_process_output"] = True
        jobtype = JobType(fake_assignment())

        with patch.object(process_stderr, "info") as mocked:
            jobtype.log_stderr_line(Mock(id=2), "stderr")

        mocked.assert_called_once_with("task %r: %s", 2, "stderr")

    def test_no_capure_stdout(self):
        config["jobtype_capture_process_output"] = False
        protocol = Mock(id=3, pid=33)
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)

        with patch.object(logpool, "log") as mocked:
            jobtype.log_stdout_line(protocol, "stdout")

        mocked.assert_called_once_with(jobtype.uuid, STDOUT, "stdout", 33)

    def test_no_capure_stderr(self):
        config["jobtype_capture_process_output"] = False
        protocol = Mock(id=3, pid=33)
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)

        with patch.object(logpool, "log") as mocked:
            jobtype.log_stderr_line(protocol, "stderr")

        mocked.assert_called_once_with(jobtype.uuid, STDERR, "stderr", 33)


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

    def test_process_stderr_line_calls_stdout_line_processing(self):
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, self.create_file(), ignore_existing=True)
        protocol = Mock(id=4)

        with patch.object(jobtype, "process_stdout_line") as process_mock:
            jobtype.process_stderr_line(protocol, "stderr 4")

        process_mock.assert_called_with(protocol, "stderr 4")

