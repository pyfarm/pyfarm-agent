# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import atexit
import os
from os.path import abspath, isfile, join, dirname

import psutil

from pyfarm.core.config import read_env
from pyfarm.core.sysinfo import network
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.entrypoints.utility import (
    get_json, get_default_ip, write_pid_file, get_process)


class TestGetJson(TestCase):
    HTTP_SCHEME = read_env(
        "PYFARM_AGENT_TEST_HTTP_SCHEME", "http")
    BASE_URL = read_env(
        "PYFARM_AGENT_TEST_URL", "%(scheme)s://httpbin.org")
    base_url = BASE_URL % {"scheme": HTTP_SCHEME}

    def test_connection_error(self):
        self.assertIsNone(get_json("http://%s" % os.urandom(16).encode("hex")))

    def test_not_ok(self):
        self.assertIsNone(get_json(self.base_url + "/status/404"))

    def test_ok(self):
        data = get_json(self.base_url + "/get")
        self.assertIsInstance(data, dict)


class TestIp(TestCase):
    def test_get_default_ip(self):
        try:
            expected = network.ip()
        except ValueError:
            expected = "127.0.0.1"
        self.assertEqual(get_default_ip(), expected)


class PidFile(TestCase):
    filenames = set()

    def setUp(self):
        TestCase.setUp(self)
        self.filename = abspath(join(
            os.urandom(16).encode("hex"),
            "%s.pid" % os.urandom(16).encode("hex")))
        self.filenames.add(self.filename)
        self.add_cleanup_path(self.filename)
        self.add_cleanup_path(dirname(self.filename))

    def test_file_should_not_exist(self):
        path = self.create_test_file()
        self.assertRaises(
            AssertionError, lambda: write_pid_file(path, os.getpid()))

    def test_writes_file(self):
        write_pid_file(self.filename, os.getpid())
        self.assertTrue(isfile(self.filename))
        with open(self.filename, "r") as stream:
            data = stream.read()

        self.assertEqual(data, str(os.getpid()))

    def test_registers_exit_handler(self):
        self.test_writes_file()

        func_names = []
        arguments = []
        for function, args, kwargs in atexit._exithandlers:
            func_names.append(getattr(function, "func_name"))
            if len(args) == 1:
                arguments.append(args[0])

        self.assertIn("remove_pid_file", func_names)

        # atexit's contents may be recreated between tests but
        # we should be able to find at least one filename
        for filename in self.filenames:
            if filename in arguments:
                break
        else:
            self.fail("None of the file we've created are an argument to an "
                      "exit handling function")

    def test_retrieve_process_id_from_empty(self):
        self.test_writes_file()
        with open(self.filename, "w") as stream:
            stream.write("")

        pid, process = get_process(self.filename)
        self.assertIsNone(pid)
        self.assertIsNone(process)

    def test_retrieve_from_missing_file(self):
        pid, process = get_process(os.urandom(16).encode("hex"))
        self.assertIsNone(pid)
        self.assertIsNone(process)

    def test_retrieve_process_id(self):
        self.test_writes_file()
        pid, process = get_process(self.filename)
        self.assertIsInstance(pid, int)
        self.assertIsInstance(process, psutil.Process)
