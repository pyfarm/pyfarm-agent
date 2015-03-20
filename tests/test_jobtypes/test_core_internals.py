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

import atexit
import re
import os
import tempfile
from collections import namedtuple
from os.path import isdir, join, isfile
from errno import EEXIST
from uuid import uuid4

try:
    from httplib import CREATED, OK
except ImportError:  # pragma: no cover
    from http.client import CREATED, OK

try:
    import pwd
except ImportError:  # pragma: no cover
    pwd = NotImplemented

try:
    import grp
except ImportError:
    grp = NotImplemented

from mock import patch
from twisted.internet import reactor
from twisted.internet.defer import Deferred

from pyfarm.core.enums import STRING_TYPES, LINUX, MAC, WINDOWS, BSD, WorkState
from pyfarm.agent.testutil import (
    TestCase, skipIf, requires_master, create_jobtype)
from pyfarm.agent.config import config
from pyfarm.agent.sysinfo import user
from pyfarm.jobtypes.core.internals import (
    ITERABLE_CONTAINERS, Cache, Process, System, TypeChecks, pwd, grp,
    logger)
from pyfarm.jobtypes.core.log import logpool

FakeExitCode = namedtuple("FakeExitCode", ("exitCode", ))
FakeProcessResult = namedtuple("FakeProcessResult", ("value", ))
FakeProcessData = namedtuple(
    "ProcessData", ("protocol", "started", "stopped", "log_identifier"))


class FakeProtocol(object):
    def __init__(self):
        self.uuid = uuid4()
        self.pid = 1000


class FakeProcess(Process, System):
    def __init__(self):
        self.start_called = False
        self.stop_called = False
        self.failed_processes = set()
        self.processes = {}
        self.assignment = {
            "job":  {"id": 1},
            "tasks": [{"id": 1, "attempt": 1}]
            }
        self._stopped_deferred = None
        self._start_deferred = None
        self.uuid = uuid4()

    def start(self):
        self.start_called = True

    def stop(self):
        self.stop_called = True

    def is_successful(self, success):
        return success.value.exitCode == 0

    def before_start(self):
        pass

    def process_started(self, protocol):
        pass

    def get_csvlog_path(self, uuid):
        return "/tmp/%s" % uuid


class TestImports(TestCase):
    @skipIf(not WINDOWS, "Not Windows")
    def test_windows(self):
        self.assertIs(pwd, NotImplemented)
        self.assertIs(grp, NotImplemented)

    @skipIf(not any([LINUX, MAC]), "Not Linux/Mac")
    def test_posix(self):
        self.assertIsNot(pwd, NotImplemented)
        self.assertIsNot(grp, NotImplemented)


class TestCache(TestCase):
    def test_cache_directory(self):
        self.assertTrue(isdir(Cache.CACHE_DIRECTORY))

    @requires_master
    def test_download(self):
        classname = "AgentUnittest" + os.urandom(8).encode("hex")
        created = create_jobtype(classname=classname)
        cache = Cache()
        finished = Deferred()

        def post_success(data):
            download = cache._download_jobtype(
                data["name"], data["version"])

            def downloaded(response):
                self.assertEqual(response.code, OK)
                data = response.json()
                self.assertEqual(data["name"], classname)
                self.assertEqual(data["classname"], classname)
                self.assertEqual(data["version"], 1)
                finished.callback(None)

            download.addCallback(downloaded)
            download.addErrback(finished.errback)

        created.addCallbacks(post_success, finished.errback)

        return finished

    def test_filename(self):
        cache = Cache()
        self.assertEqual(
            cache._cache_filepath("foobar", "someclass", 1),
            str(join(
                Cache.CACHE_DIRECTORY, "foobar_someclass_v1.py")))

    def test_cache(self):
        cache = Cache()
        classname = "Test%s" % os.urandom(8).encode("hex")
        version = 1
        code = os.urandom(8).encode("hex")
        cache_key = "Key%s" % classname
        filepath = cache._cache_filepath(cache_key, classname, version)
        jobtype = {"classname": classname, "code": code, "version": version}

        def written(data):
            self.assertEqual(data[0]["classname"], classname)
            if data[1] is not None:
                self.assertEqual(data[1], filepath)
            self.assertTrue(isfile(filepath))

        cached = cache._cache_jobtype(cache_key, jobtype)
        cached.addCallback(written)
        return cached


class TestProcessStartStopDeferredProperties(TestCase):
    def test_get_start_not_called(self):
        p = Process()
        p.start_called = False

        with self.assertRaises(RuntimeError):
            p.started_deferred

        with self.assertRaises(RuntimeError):
            p.stopped_deferred

    def test_get_start_called(self):
        p = Process()
        p.start_called = True
        p._started_deferred = 123
        p._stopped_deferred = 456
        self.assertEqual(p.started_deferred, 123)
        self.assertEqual(p.stopped_deferred, 456)

    def test_setter_start_not_called(self):
        p = Process()
        p.start_called = False

        with self.assertRaises(RuntimeError):
            p.stopped_deferred = None

        with self.assertRaises(RuntimeError):
            p.started_deferred = None

    def test_setter_deferred_already_set(self):
        p = Process()
        p.start_called = True
        p._started_deferred = Deferred()
        p._stopped_deferred = Deferred()

        with self.assertRaises(ValueError):
            p.stopped_deferred = None

        with self.assertRaises(ValueError):
            p.started_deferred = None

    def test_setter_expected_deferred(self):
        p = Process()
        p.start_called = True
        p._started_deferred = None
        p._stopped_deferred = None

        with self.assertRaises(TypeError):
            p.stopped_deferred = None

        with self.assertRaises(TypeError):
            p.started_deferred = None


class TestProcess(TestCase):
    def setUp(self):
        TestCase.setUp(self)

        self.protocol = FakeProtocol()
        self.process = FakeProcess()
        self.process.log_identifier = "%s.csv" % self.process.uuid
        self.process.processes[self.protocol.uuid] = FakeProcessData(
            self.protocol, Deferred(), Deferred(), "logid")

        # Make sure the logfile actually exists on disk, otherwise the
        # _process_stopped tests will fail
        try:
            os.makedirs(config["jobtype_task_logs"])
        except OSError as e:
            if e.errno != EEXIST:
                raise
        with open(join(config["jobtype_task_logs"],
                       self.process.log_identifier), "wb"):
            pass

        # Clear the logger pool each time and make
        # sure it won't flush
        logpool.flush_lines = 1e10
        logpool.logs.clear()
        logpool.stopped = False

        # Create a dummy logfile
        logfile_path = join(config["jobtype_task_logs"], "logid")
        with open(logfile_path, "w+") as fakelog:
            fakelog.write("test")

    def test_stop_called(self):
        self.process._stop()
        self.assertTrue(self.process.stop_called)

    def test_get_uid_gid_value_type(self):
        with self.assertRaises(TypeError):
            self.process._get_uid_gid_value(None, None, None, None, None)

    def test_get_uid_gid_value_no_module(self):
        self.assertIsNone(
            self.process._get_uid_gid_value(
                "", None, None, NotImplemented, None))

    @skipIf(not any([MAC, LINUX, BSD]), "Not Linux/Mac/BSD")
    def test_get_uid_gid_value_grp(self):
        import grp  # platform specific import

        for grp_struct in grp.getgrall():
            self.assertEqual(
                self.process._get_uid_gid_value(
                    grp_struct.gr_name,
                    "group", "get_gid", grp, "grp"),
                grp_struct.gr_gid)

    @skipIf(not any([MAC, LINUX, BSD]), "Not Linux/Mac/BSD")
    def test_get_uid_gid_value_pwd(self):
        import pwd  # platform specific import

        for pwd_struct in pwd.getpwall():
            self.assertEqual(
                self.process._get_uid_gid_value(
                    pwd_struct.pw_name,
                    "username", "get_uid", pwd, "pwd"),
                pwd_struct.pw_uid)

    @skipIf(not any([MAC, LINUX, BSD]), "Not Linux/Mac/BSD")
    def test_invalid_get_uid_gid_value_grp(self):
        import grp  # platform specific import

        config["jobtype_ignore_id_mapping_errors"] = True

        for grp_struct in grp.getgrall():
            self.assertIsNone(
                self.process._get_uid_gid_value(
                    grp_struct.gr_name + "foo",
                    "group", "get_gid", grp, "grp"))

        config.pop("jobtype_ignore_id_mapping_errors")

        for grp_struct in grp.getgrall():
            with self.assertRaises(KeyError):
                self.assertIsNone(
                    self.process._get_uid_gid_value(
                        grp_struct.gr_name + "foo",
                        "group", "get_gid", grp, "grp"))

    @skipIf(not any([MAC, LINUX, BSD]), "Not Linux/Mac/BSD")
    def test_invalid_get_uid_gid_value_pwd(self):
        import pwd  # platform specific import

        config["jobtype_ignore_id_mapping_errors"] = True

        for pwd_struct in pwd.getpwall():
            self.assertIsNone(
                self.process._get_uid_gid_value(
                    pwd_struct.pw_name + "foo",
                    "username", "get_uid", pwd, "pwd"))

        config.pop("jobtype_ignore_id_mapping_errors")

        for pwd_struct in pwd.getpwall():
            with self.assertRaises(KeyError):
                self.assertIsNone(
                    self.process._get_uid_gid_value(
                        pwd_struct.pw_name + "foo",
                        "username", "get_uid", pwd, "pwd"))


# TODO: fix these tests to use the new CommandData.validate
# class TestSpawnProcessTypeChecks(TestCase):
    # def test_command(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*command.*"):
    #         checks._check_spawn_process_inputs(
    #             None, None, None, None, None, None)
    #
    # def test_arguments(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*arguments.*"):
    #         checks._check_spawn_process_inputs(
    #             "", None, None, None, None, None)
    #
    # def test_workdir_not_string(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*working_.*"):
    #         checks._check_spawn_process_inputs(
    #             "", [], None, None, None, None)
    #
    # def test_workdir_missing(self):
    #     checks = TypeChecks()
    #     with self.assertRaises(OSError):
    #         checks._check_spawn_process_inputs(
    #             "", [], urandom(8).encode("hex"), {}, "foo", "foo")
    #
    # def test_environment(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*environment.*"):
    #         checks._check_spawn_process_inputs(
    #             "", [], gettempdir(), None, None, None)
    #
    # def test_user(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*user.*"):
    #         checks._check_spawn_process_inputs(
    #             "", [], gettempdir(), {}, bool, None)
    #
    # def test_group(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(TypeError, ".*for.*group.*"):
    #         checks._check_spawn_process_inputs(
    #             "", [], gettempdir(), {}, 0, bool)
    #
    # @skipIf(is_administrator() or WINDOWS, "Cannot run on Windows or as root")
    # def test_cannot_change_user(self):
    #     checks = TypeChecks()
    #     with self.assertRaisesRegexp(EnvironmentError, ".*change user or.*"):
    #         checks._check_spawn_process_inputs(
    #             "", [], gettempdir(), {}, "foo", "foo")


class TestMiscTypeChecks(TestCase):
    def test_expandvars_value_not_string(self):
        checks = TypeChecks()
        checks._check_expandvars_inputs("", {})

        with self.assertRaisesRegexp(TypeError,
                                     re.compile(".*for.*value.*")):
            checks._check_expandvars_inputs(None, None)

    def test_expandvars_environment_not_dict(self):
        checks = TypeChecks()
        checks._check_expandvars_inputs("", None)

        with self.assertRaisesRegexp(TypeError,
                                     re.compile(".*for.*environment.*")):
            checks._check_expandvars_inputs("", 1)

    def test_map(self):
        checks = TypeChecks()
        for objtype in STRING_TYPES:
            checks._check_map_path_inputs(objtype())

        with self.assertRaisesRegexp(TypeError, re.compile(".*for.*path.*")):
            checks._check_map_path_inputs(None)

    def test_csvlog_path_tasks(self):
        checks = TypeChecks()
        checks._check_csvlog_path_inputs(uuid4(), None)

        with self.assertRaisesRegexp(
                TypeError, re.compile("Expected UUID for `protocol_uuid`")):
            checks._check_csvlog_path_inputs(None, None)

    def test_csvlog_path_time(self):
        checks = TypeChecks()
        checks._check_csvlog_path_inputs(uuid4(), None)

        with self.assertRaisesRegexp(
                TypeError, re.compile("Expected UUID for `protocol_uuid`")):
            checks._check_csvlog_path_inputs([], "")

    def test_command_list(self):
        checks = TypeChecks()
        checks._check_command_list_inputs(tuple())
        checks._check_command_list_inputs([])

        with self.assertRaisesRegexp(TypeError, re.compile(".*for.*cmdlist.*")):
            checks._check_command_list_inputs(None)

    def test_set_state_tasks(self):
        checks = TypeChecks()
        for objtype in ITERABLE_CONTAINERS:
            checks._check_set_states_inputs(objtype(), WorkState.DONE)

        with self.assertRaisesRegexp(TypeError, re.compile(".*for.*tasks.*")):
            checks._check_set_states_inputs(None, None)

    def test_set_state_state(self):
        checks = TypeChecks()
        for state in WorkState:
            checks._check_set_states_inputs(ITERABLE_CONTAINERS[0](), state)

        with self.assertRaisesRegexp(ValueError,
                                     re.compile(".*Expected.*state.*")):
            checks._check_set_states_inputs(ITERABLE_CONTAINERS[0](), None)


class TestSystemMisc(TestCase):
    def test_log_assertion(self):
        system = System()
        for entry in ("", 1, None, [], tuple(), dict(), 1.0):
            system.uuid = entry
            with self.assertRaises(AssertionError):
                system._log("")

    def test_log_to_logpool(self):
        system = System()
        system.uuid = uuid4()

        with patch.object(logpool, "log") as log:
            system._log("Hello, World.")

        self.assertEqual(log.call_count, 1)
        log.assert_called_with(system.uuid, "jobtype", "Hello, World.")

    def test_no_parent_class(self):
        # The system class is meant to be a mixin.
        self.assertEqual(System.__bases__, (object, ))

    def test_not_implemented_attributes(self):
        self.assertIs(System._tempdirs, NotImplemented)
        self.assertIs(System.uuid, NotImplemented)


class TestSystemUidGid(TestCase):
    def test_value_type(self):
        system = System()
        for entry in (1, None, [], tuple(), dict(), 1.0):
            with self.assertRaises(TypeError):
                system._get_uid_gid_value(entry, None, None, None, None)

    def test_module_not_implemented(self):
        system = System()

        with patch.object(logger, "warning") as warning:
            system._get_uid_gid_value(
                "", None, "function", NotImplemented, "module")

        warning.assert_called_with(
            "This platform does not implement the %r module, skipping %s()",
            "module", "function"
        )

    @skipIf(grp is NotImplemented, "grp module is NotImplemented")
    def test_get_grp_from_string(self):
        system = System()
        success = True
        for group_id in os.getgroups():
            group_entry = grp.getgrgid(group_id)

            try:
                self.assertEqual(
                    group_entry.gr_gid,
                    system._get_uid_gid_value(
                        group_entry.gr_name, None, None, "getgrnam", "grp"
                    )
                )
                success = True

            # There's numerous ways this could happen including
            # the user being a part of the group however the group
            # itself is undefined.
            except KeyError:
                pass

        self.failUnless(success, "Expected at least one successful resolution")

    @skipIf(pwd is NotImplemented, "pwd module is NotImplemented")
    def test_get_pwd_from_string(self):
        system = System()
        username = user.username()
        self.assertEqual(
            pwd.getpwnam(username).pw_uid,
            system._get_uid_gid_value(
                username, None, None, "getpwnam", "pwd"
            )
        )

    def test_string_no_such_module(self):
        system = System()
        with self.assertRaises(ValueError):
            system._get_uid_gid_value(
                "", None, None, "getpwnam", "foo"
            )

    # TODO: tests for when pwd/grp raises KeyError (for strings)
    # TODO: tests for when pwd/grp raises KeyError (for integers)



class TestSystemTempDirs(TestCase):
    def test_remove_directories_exception(self):
        system = System()
        for entry in ("", 1, None, 1.0):
            with self.assertRaises(AssertionError):
                system._remove_directories(entry)

    def test_removes_directories(self):
        directories = [
            tempfile.mkdtemp(), tempfile.mkdtemp(),
            tempfile.mkdtemp(), tempfile.mkdtemp(),
            "THIS PATH DOES NOT EXIST"
        ]
        system = System()
        system._remove_directories(directories)

        for directory in directories:

            # This is here because the default behavior for the
            # underlying remove_directory() function is to ignore
            # ENOENT
            if directory == "THIS PATH DOES NOT EXIST":
                continue

            if not isdir(directory):
                continue

            # Maybe the directory was not removed?  If not
            # then we should expect it to be in atexit
            for function, args, keywords in atexit._exithandlers:
                if directory in args:
                    break
            else:
                self.fail("Directory %s not removed" % directory)

    def test_remote_tempdirs_assertion(self):
        system = System()
        for entry in ("", 1, None, [], tuple(), dict()):
            system._tempdirs = entry
            with self.assertRaises(AssertionError):
                system._remove_tempdirs()

    def test_does_nothing_if_empty_tempdirs(self):
        system = System()
        system._tempdirs = set()

        with patch.object(reactor, "callInThread") as callInThread:
            system._remove_tempdirs()

        self.assertEqual(callInThread.call_count, 0)

    def test_calls_removes_directories(self):
        system = System()
        system._tempdirs = set([None])

        with patch.object(reactor, "callInThread") as callInThread:
            system._remove_tempdirs()

        self.assertEqual(callInThread.call_count, 1)
        self.assertEqual(system._tempdirs, set())
        callInThread.assert_called_with(system._remove_directories, set([None]))


