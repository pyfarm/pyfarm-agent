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


from collections import namedtuple
from os import urandom, devnull
from os.path import isdir, join, isfile
from tempfile import gettempdir
from textwrap import dedent

try:
    from httplib import CREATED, OK
except ImportError:  # pragma: no cover
    from http.client import CREATED, OK


from twisted.internet.defer import Deferred

from pyfarm.core.enums import STRING_TYPES, LINUX, MAC, WINDOWS, WorkState
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import post
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.utility import uuid
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.jobtypes.core.internals import (
    ITERABLE_CONTAINERS, Cache, Process, TypeChecks, pwd, grp)
from pyfarm.jobtypes.core.log import logpool, CSVLog

FakeExitCode = namedtuple("FakeExitCode", ("exitCode", ))
FakeProcessResult = namedtuple("FakeProcessResult", ("value", ))


class FakeProtocol(object):
    def __init__(self):
        self.uuid = uuid()


class FakeProcess(Process):
    def __init__(self):
        self.start_called = False
        self.stop_called = False
        self.failed_processes = []

    def start(self):
        self.start_called = True

    def stop(self):
        self.stop_called = True

    def is_successful(self, success):
        return success.value.exitCode == 0


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

    def test_download(self):
        classname = "Test%s" % urandom(8).encode("hex")
        sourcecode = dedent("""
        from pyfarm.jobtypes.core.jobtype import JobType
        class %s(JobType):
            pass""" % classname)

        cache = Cache()
        finished = Deferred()

        def post_success(response):
            if response.code == CREATED:
                data = response.json()
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

            else:
                finished.errback(response.json())

        post(config["master-api"] + "/jobtypes/",
             callback=post_success, errback=finished.errback,
             data={"name": classname,
                   "classname": classname,
                   "code": sourcecode})
        return finished

    def test_filename(self):
        cache = Cache()
        self.assertEqual(
            cache._cache_filepath("foobar", "someclass"),
            str(join(
                Cache.CACHE_DIRECTORY, "foobar_someclass.py")))

    def test_cache(self):
        cache = Cache()
        classname = "Test%s" % urandom(8).encode("hex")
        code = urandom(8).encode("hex")
        cache_key = "Key%s" % classname
        filepath = cache._cache_filepath(cache_key, classname)
        jobtype = {"classname": classname, "code": code}

        def written(data):
            self.assertEqual(data[0]["classname"], classname)
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

        # Clear the logger pool each time and make
        # sure it won't flush
        logpool.flush_lines = 1e10
        logpool.logs.clear()
        logpool.logs[self.protocol.uuid] = CSVLog(open(devnull))
        logpool.stopped = False

    def test_start_called(self):
        self.process._start()
        self.assertTrue(self.process.start_called)

    def test_stop_called(self):
        self.process._stop()
        self.assertTrue(self.process.stop_called)

    @skipIf(grp is NotImplemented, "grp module is NotImplemented")
    def test_uid_gid_mapper(self):
        uid, gid = self.process._get_uid_gid(
            "root", grp.getgrnam("root").gr_name)
        self.assertEqual(uid, 0)
        self.assertEqual(gid, 0)

    def test_process_started(self):
        self.process._process_started(self.protocol)
        self.assertEqual(len(logpool.logs[self.protocol.uuid].messages), 1)

    def test_process_stopped_success(self):
        result = FakeProcessResult(value=FakeExitCode(exitCode=0))
        self.process._process_stopped(self.protocol, result)
        self.assertEqual(len(logpool.logs[self.protocol.uuid].messages), 1)
        self.assertEqual(self.process.failed_processes, [])

    def test_process_stopped_failure(self):
        result = FakeProcessResult(value=FakeExitCode(exitCode=1))
        self.process._process_stopped(self.protocol, result)
        self.assertEqual(len(logpool.logs[self.protocol.uuid].messages), 1)
        self.assertEqual(
            self.process.failed_processes, [(self.protocol, result)])

    def test_get_uid_gid_value_type(self):
        with self.assertRaises(TypeError):
            self.process._get_uid_gid_value(None, None, None, None, None)

    def test_get_uid_gid_value_no_module(self):
        self.assertIsNone(
            self.process._get_uid_gid_value(
                "", None, None, NotImplemented, None))

    def test_get_uid_gid_value_grp(self):
        self.assertEqual(
            self.process._get_uid_gid_value(
                "root", "group", "get_gid", grp, "grp"), 0)

    def test_get_uid_gid_value_pwd(self):
        self.assertEqual(
            self.process._get_uid_gid_value(
                "root", "username", "get_uid", pwd, "pwd"), 0)


class TestSpawnProcessTypeChecks(TestCase):
    def test_command(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*command.*"):
            checks._check_spawn_process_inputs(
                None, None, None, None, None, None)

    def test_arguments(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*arguments.*"):
            checks._check_spawn_process_inputs(
                "", None, None, None, None, None)

    def test_workdir_not_string(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*working_.*"):
            checks._check_spawn_process_inputs(
                "", [], None, None, None, None)

    def test_workdir_missing(self):
        checks = TypeChecks()
        with self.assertRaises(OSError):
            checks._check_spawn_process_inputs(
                "", [], urandom(8).encode("hex"), {}, "foo", "foo")

    def test_environment(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*environment.*"):
            checks._check_spawn_process_inputs(
                "", [], gettempdir(), None, None, None)

    def test_user(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*user.*"):
            checks._check_spawn_process_inputs(
                "", [], gettempdir(), {}, bool, None)

    def test_group(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(TypeError, ".*for.*group.*"):
            checks._check_spawn_process_inputs(
                "", [], gettempdir(), {}, 0, bool)

    @skipIf(is_administrator() or WINDOWS, "Cannot run on Windows or as root")
    def test_cannot_change_user(self):
        checks = TypeChecks()
        with self.assertRaisesRegexp(EnvironmentError, ".*change user or.*"):
            checks._check_spawn_process_inputs(
                "", [], gettempdir(), {}, "foo", "foo")


class TestMiscTypeChecks(TestCase):
    def test_expandvars_value_not_string(self):
        checks = TypeChecks()
        checks._check_expandvars_inputs("", {})

        with self.assertRaisesRegexp(TypeError, ".*for.*value.*"):
            checks._check_expandvars_inputs(None, None)

    def test_expandvars_environment_not_dict(self):
        checks = TypeChecks()
        checks._check_expandvars_inputs("", None)

        with self.assertRaisesRegexp(TypeError, ".*for.*environment.*"):
            checks._check_expandvars_inputs("", 1)

    def test_map(self):
        checks = TypeChecks()
        for objtype in STRING_TYPES:
            checks._check_map_path_inputs(objtype())

        with self.assertRaisesRegexp(TypeError, ".*for.*path.*"):
            checks._check_map_path_inputs(None)

    def test_csvlog_path_tasks(self):
        checks = TypeChecks()
        for objtype in ITERABLE_CONTAINERS:
            checks._check_csvlog_path_inputs(objtype(), None)

        with self.assertRaisesRegexp(TypeError, ".*for.*tasks.*"):
            checks._check_csvlog_path_inputs(None, None)

    def test_csvlog_path_time(self):
        checks = TypeChecks()
        for objtype in ITERABLE_CONTAINERS:
            checks._check_csvlog_path_inputs(objtype(), None)

        with self.assertRaisesRegexp(TypeError, ".*for.*now.*"):
            checks._check_csvlog_path_inputs([], "")

    def test_command_list(self):
        checks = TypeChecks()
        checks._check_command_list_inputs(tuple())
        checks._check_command_list_inputs([])

        with self.assertRaisesRegexp(TypeError, ".*for.*cmdlist.*"):
            checks._check_command_list_inputs(None)

    def test_set_state_tasks(self):
        checks = TypeChecks()
        for objtype in ITERABLE_CONTAINERS:
            checks._check_set_states_inputs(objtype(), WorkState.DONE)

        with self.assertRaisesRegexp(TypeError, ".*for.*tasks.*"):
            checks._check_set_states_inputs(None, None)

    def test_set_state_state(self):
        checks = TypeChecks()
        for state in WorkState:
            checks._check_set_states_inputs(ITERABLE_CONTAINERS[0](), state)

        with self.assertRaisesRegexp(ValueError, ".*Expected.*state.*"):
            checks._check_set_states_inputs(ITERABLE_CONTAINERS[0](), None)
