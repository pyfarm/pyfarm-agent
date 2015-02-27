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


import re
from collections import namedtuple
from os import urandom, devnull, makedirs
from os.path import isdir, join, isfile
from errno import EEXIST
from uuid import uuid4

try:
    from httplib import CREATED, OK
except ImportError:  # pragma: no cover
    from http.client import CREATED, OK


from twisted.internet.defer import Deferred

from pyfarm.core.enums import STRING_TYPES, LINUX, MAC, WINDOWS, BSD, WorkState
from pyfarm.agent.testutil import (
    TestCase, skipIf, requires_master, create_jobtype)
from pyfarm.agent.config import config
from pyfarm.jobtypes.core.internals import (
    ITERABLE_CONTAINERS, Cache, Process, System, TypeChecks, pwd, grp)
from pyfarm.jobtypes.core.log import logpool, CSVLog

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
        classname = "AgentUnittest" + urandom(8).encode("hex")
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
        classname = "Test%s" % urandom(8).encode("hex")
        version = 1
        code = urandom(8).encode("hex")
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
            makedirs(config["jobtype_task_logs"])
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
