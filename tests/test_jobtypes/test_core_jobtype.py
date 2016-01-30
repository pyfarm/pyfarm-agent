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
import tempfile
from contextlib import nested
from datetime import datetime, timedelta
from os.path import join, isdir, dirname, basename
from uuid import UUID, uuid4

try:
    import grp
except ImportError:
    pass

try:
    import pwd
except ImportError:
    pass


from mock import Mock, patch
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone
from voluptuous import Schema, MultipleInvalid

from pyfarm.core.utility import ImmutableDict
from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WINDOWS, WorkState
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.sysinfo import system, memory, user
from pyfarm.agent.utility import remove_directory
from pyfarm.jobtypes.core.log import STDOUT, STDERR, logpool
from pyfarm.jobtypes.core.internals import USER_GROUP_TYPES
from pyfarm.jobtypes.core.jobtype import (
    FROZEN_ENVIRONMENT, JobType, CommandData, logger, process_stderr,
    process_stdout)

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
        "tasks": [
            {"id": 1, "frame": 1, "attempt": 1},
            {"id": 2, "frame": 2, "attempt": 1}]}
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
    @skipIf(user.is_administrator(), "Is Administrator")
    def test_validate_change_user_non_admin_failure(self):
        with self.assertRaises(EnvironmentError):
            CommandData("", user=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(user.is_administrator(), "Is Administrator")
    def test_validate_change_group_non_admin_failure(self):
        with self.assertRaises(EnvironmentError):
            CommandData("", group=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(not user.is_administrator(), "Not Administrator")
    def test_validate_change_user_admin(self):
        CommandData("", user=0).validate()

    @skipIf(WINDOWS, "Non-Windows only")
    @skipIf(not user.is_administrator(), "Not Administrator")
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
    # TODO: test to ensure load() does something useful, the below
    # are just unittests

    def test_schema(self):
        with self.assertRaises(MultipleInvalid):
            JobType.load({})

    def test_download_called(self):
        assignment = fake_assignment()
        deferred = Deferred()

        class JobTypePatch(JobType):
            @classmethod
            def _download_jobtype(cls, name, version):
                self.assertEqual(assignment["jobtype"]["name"], name)
                self.assertEqual(assignment["jobtype"]["version"], version)
                return deferred

        with nested(
            patch.object(
                JobType, "_download_jobtype", JobTypePatch._download_jobtype),
            patch.object(deferred, "addCallback")
        ) as (_, patched_addCallback):
            JobType.load(assignment)

        patched_addCallback.assert_called_with(
            JobType._jobtype_download_complete,
            JobType._cache_key(assignment)
        )


class TestJobTypeEmptyMethodSignatures(TestCase):
    # Some methods do not have any code in them so while we can't
    # write tests against any code we can ensure their APIs, which
    # are considered public, don't change.

    def test_prepare_for_job(self):
        JobType.prepare_for_job(None)

    def test_cleanup_after_job(self):
        JobType.cleanup_after_job(None)

    def spawn_persistent_process(self):
        JobType.spawn_persistent_process(None, None)

    def test_process_stdout_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.process_stdout_line(None, None)

    def test_process_stderr_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.process_stderr_line(None, None)

        with patch.object(jobtype, "process_stdout_line") as patched:
            jobtype.process_stderr_line("a", "b")

        patched.assert_called_with("a", "b")

    def test_preprocess_stdout_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.preprocess_stdout_line(None, None)

    def test_preprocess_stderr_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.preprocess_stderr_line(None, None)

    def test_format_stdout_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.format_stdout_line(None, None)

    def test_format_stderr_line(self):
        jobtype = JobType(fake_assignment())
        jobtype.format_stderr_line(None, None)

    def test_spawn_persistent_process(self):
        JobType.spawn_persistent_process(None, None)

    def test_get_command_data(self):
        with self.assertRaises(NotImplementedError):
            jobtype = JobType(fake_assignment())
            jobtype.get_command_data()

    def test_before_start(self):
        jobtype = JobType(fake_assignment())
        jobtype.before_start()

    def test_before_spawn_process(self):
        path = self.create_file()
        jobtype = JobType(fake_assignment())
        logpool.open_log(jobtype.uuid, path, ignore_existing=True)
        command = CommandData("foobar", ("a", "b", "c"))

        with patch.object(logger, "info") as mocked:
            jobtype.before_spawn_process(command, None)

        mocked.assert_called_once_with(
            "Starting command: %s", "foobar ('a', 'b', 'c')")


class TestJobTypeCloseLogs(TestCase):
    def test_close_logs(self):
        jobtype = JobType(fake_assignment())

        with patch.object(logpool, "close_log") as patched:
            jobtype._close_logs()

        patched.assert_called_with(jobtype.uuid)


class TestJobTypeNode(TestCase):
    def test_reraises_notimplemented(self):
        def side_effect():
            raise NotImplementedError

        with nested(
            patch.object(
              system, "machine_architecture", side_effect=side_effect),
            self.assertRaises(NotImplementedError)
        ):
            jobtype = JobType(fake_assignment())
            jobtype.node()

    def test_output(self):
        jobtype = JobType(fake_assignment())

        with nested(
            patch.object(memory, "total_ram", return_value=1.1),
            patch.object(memory, "free_ram", return_value=2.1),
            patch.object(memory, "total_consumption", return_value=3.1)
        ):
            self.assertEqual(
                jobtype.node(),
                {
                    "master_api": config.get("master-api"),
                    "hostname": config["agent_hostname"],
                    "agent_id": config["agent_id"],
                    "id": config["agent_id"],
                    "cpus": int(config["agent_cpus"]),
                    "ram": int(config["agent_ram"]),
                    "total_ram": 1,
                    "free_ram": 2,
                    "consumed_ram": 3,
                    "admin": user.is_administrator(),
                    "user": user.username(),
                    "case_sensitive_files":
                        system.filesystem_is_case_sensitive(),
                    "case_sensitive_env":
                        system.environment_is_case_sensitive(),
                    "machine_architecture":
                        system.machine_architecture(),
                    "operating_system": system.operating_system()
                }
            )


class TestJobTypeAssignments(TestCase):
    def test_assignments(self):
        assignment = fake_assignment()
        jobtype = JobType(assignment)
        self.assertEqual(jobtype.assignments(), assignment["tasks"])


class TestJobTypeTempDir(TestCase):
    def test_not_new_and_tempdir_already_set(self):
        jobtype = JobType(fake_assignment())
        jobtype._tempdir = "foobar"
        self.assertEqual(jobtype.tempdir(), "foobar")

    def test_creates_directory(self):
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())
        self.assertTrue(isdir(jobtype.tempdir()))

    def test_path_contains_jobtype_uuid(self):
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())
        tempdir = jobtype.tempdir()
        self.assertEqual(basename(dirname(tempdir)), str(jobtype.uuid))

    def test_ignores_eexist(self):
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())
        os.makedirs(config["jobtype_tempdir_root"].replace(
            "$JOBTYPE_UUID", str(jobtype.uuid)))
        jobtype.tempdir()

    def test_makedirs_raises_other_errors(self):
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())

        def side_effect(*args):
            raise OSError("Foo", 4242)

        with nested(
            patch.object(os, "makedirs", side_effect=side_effect),
            self.assertRaises(OSError)
        ):
            jobtype.tempdir()

    def test_remove_on_finish_does_not_add_parent(self):
        # It's important that we do not add the parent directory
        # as the path to cleanup.  Otherwise we could end up
        # cleaning up more than needed.
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())
        jobtype.tempdir()

        for value in jobtype._tempdirs:
            self.assertNotEqual(value, dirname(config["jobtype_tempdir_root"]))

    def test_remove_on_finish_adds_child_directory(self):
        root_directory = tempfile.mkdtemp()
        self.addCleanup(remove_directory, root_directory)
        config["jobtype_tempdir_root"] = join(root_directory, "$JOBTYPE_UUID")
        jobtype = JobType(fake_assignment())
        tempdir = jobtype.tempdir()
        self.assertIn(tempdir, jobtype._tempdirs)

    def test_sets_tempdir(self):
        jobtype = JobType(fake_assignment())
        tempdir = jobtype.tempdir()
        self.assertEqual(tempdir, jobtype._tempdir)

    def test_new_tempdir_does_not_reset_default(self):
        jobtype = JobType(fake_assignment())
        tempdir1 = jobtype.tempdir()
        tempdir2 = jobtype.tempdir(new=True)
        self.assertNotEqual(tempdir1, tempdir2)
        self.assertEqual(jobtype._tempdir, tempdir1)


class TestJobTypeGetUidGid(TestCase):
    @skipIf(WINDOWS, "Non-Windows only")
    def test_get_user_id(self):
        jobtype = JobType(fake_assignment())

        for pwd_struct in pwd.getpwall():
            uid, gid = jobtype.get_uid_gid(pwd_struct.pw_name, None)
            self.assertEqual(pwd.getpwuid(uid).pw_uid, uid)
            self.assertIsNone(gid)

    @skipIf(WINDOWS, "Non-Windows only")
    def test_get_group_id(self):
        jobtype = JobType(fake_assignment())

        for grp_struct in grp.getgrall():
            uid, gid = jobtype.get_uid_gid(None, grp_struct.gr_name)
            self.assertIsNone(uid)
            self.assertEqual(grp.getgrgid(gid).gr_gid, gid)


class TestJobTypeGetCommandList(TestCase):
    def test_result_is_tuple(self):
        jobtype = JobType(fake_assignment())
        self.assertIsInstance(jobtype.get_command_list([]), tuple)

    def test_does_not_modify_contents(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.get_command_list(["a", "b", "c"]), ("a", "b", "c"))

    def test_expands_variables(self):
        config["jobtype_include_os_environ"] = True
        jobtype = JobType(fake_assignment())

        all_keys = ["$%s" % key for key in os.environ.keys()]
        self.assertEqual(
            jobtype.get_command_list(all_keys),
            tuple(map(jobtype.expandvars, all_keys))
        )


class TestJobTypeGetCSVLogPath(TestCase):
    POP_CONFIG_KEYS = ["agent_time_offset"]

    def setUp(self):
        super(TestJobTypeGetCSVLogPath, self).setUp()
        self.current_value = config.get("jobtype_task_log_filename")
        config["jobtype_task_log_filename"] = \
            "$YEAR-$MONTH-$DAY_$HOUR-$MINUTE-$SECOND_$JOB_$PROCESS"
        config["agent_time_offset"] = 0

    def tearDown(self):
        super(TestJobTypeGetCSVLogPath, self).tearDown()
        config["jobtype_task_log_filename"] = self.current_value

    def test_uses_utcnow_for_create_time(self):
        jobtype = JobType(fake_assignment())
        now = datetime.utcnow()
        filename = basename(jobtype.get_csvlog_path(uuid4()))
        year, month, day_hour, minute, second_job_process = filename.split("-")
        day, hour = day_hour.split("_")
        second, job, uuid_str = second_job_process.split("_")
        log_time = datetime(
            year=int(year), month=int(month), day=int(day),
            hour=int(hour), minute=int(minute), second=int(second)
        )
        self.assertDateAlmostEqual(
            now, log_time, microsecond_deviation=1000000)

    def test_includes_agent_time_offset(self):
        config["agent_time_offset"] = 120
        jobtype = JobType(fake_assignment())
        now = datetime.utcnow() + timedelta(seconds=120)
        filename = basename(jobtype.get_csvlog_path(uuid4()))
        year, month, day_hour, minute, second_job_process = filename.split("-")
        day, hour = day_hour.split("_")
        second, job, uuid_str = second_job_process.split("_")
        log_time = datetime(
            year=int(year), month=int(month), day=int(day),
            hour=int(hour), minute=int(minute), second=int(second)
        )
        self.assertDateAlmostEqual(now, log_time, microsecond_deviation=1000000)

    def test_uses_provided_create_time(self):
        assignment = fake_assignment()
        jobtype = JobType(assignment)
        now = datetime.utcnow()
        uid = uuid4()
        filename = basename(jobtype.get_csvlog_path(uid, create_time=now))
        self.assertEqual(
            "%s-%02d-%02d_%02d-%02d-%02d_%s_%s" % (
                now.year, now.month, now.day,
                now.hour, now.minute, now.second,
                assignment["job"]["id"], str(uid).replace("-", "")
            ),
            filename
        )


class TestJobTypeMapPath(TestCase):
    def test_path_path_expands_vars(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.map_path("$PATH/foo"),
            os.path.expandvars("$PATH/foo"))


class TestJobTypeExpandVars(TestCase):
    def prepare_config(self):
        super(TestJobTypeExpandVars, self).prepare_config()
        config["jobtype_expandvars"] = FROZEN_ENVIRONMENT
        config["jobtype_default_environment"] = {
            "PATH": os.environ["PATH"]
        }

    def tearDown(self):
        super(TestJobTypeExpandVars, self).tearDown()
        config["jobtype_expandvars"] = True

    def test_respects_expand_from_config(self):
        config["jobtype_expandvars"] = False
        jobtype = JobType(fake_assignment())
        self.assertEqual(jobtype.expandvars("$PATH"), "$PATH")

    def test_respects_expand_keyword_expandvars(self):
        config["jobtype_expandvars"] = False
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.expandvars("$PATH", expand=True),
            os.path.expandvars("$PATH"))

        config["jobtype_expandvars"] = True
        jobtype = JobType(fake_assignment())
        self.assertEqual(jobtype.expandvars("$PATH", expand=False), "$PATH")

    def test_passed_custom_environment(self):
        jobtype = JobType(fake_assignment())
        self.assertEqual(
            jobtype.expandvars("$FOOBAR", environment={"FOOBAR": "foo"}),
            "foo")


class TestJobTypeGetLocalTaskState(TestCase):
    def test_done(self):
        jobtype = JobType(fake_assignment())
        jobtype.finished_tasks.add(1)
        self.assertEqual(jobtype.get_local_task_state(1), WorkState.DONE)

    def test_failed(self):
        jobtype = JobType(fake_assignment())
        jobtype.failed_tasks.add(2)
        self.assertEqual(jobtype.get_local_task_state(2), WorkState.FAILED)

    def test_unknown(self):
        jobtype = JobType(fake_assignment())
        self.assertIsNone(jobtype.get_local_task_state(3))


class TestJobTypeIsSuccessful(TestCase):
    def test_reason_is_zero(self):
        jobtype = JobType(fake_assignment())
        self.assertTrue(jobtype.is_successful(None, 0))

    def test_reason_non_zero_and_is_integer(self):
        jobtype = JobType(fake_assignment())
        self.assertFalse(jobtype.is_successful(None, 1))

    def test_reason_has_type_and_exit_code(self):
        jobtype = JobType(fake_assignment())
        reason = Mock(type=ProcessDone, value=Mock(exitCode=0))
        self.assertTrue(jobtype.is_successful(None, reason))

    def test_reason_is_not_an_integer(self):
        jobtype = JobType(fake_assignment())

        with self.assertRaises(NotImplementedError):
            self.assertFalse(jobtype.is_successful(None, None))


class TestJobTypeProcessStarted(TestCase):
    def test_calls_set_task_state(self):
        jobtype = JobType(fake_assignment())

        with patch.object(jobtype, "set_task_state") as mocked:
            jobtype.process_started(None)

        for task in jobtype.assignment["tasks"]:
            mocked.assert_any_call(task, WorkState.RUNNING)


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

