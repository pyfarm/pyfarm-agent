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

from collections import deque
from datetime import datetime
from os import urandom
from tempfile import mkdtemp
from os.path import join, isfile, isdir

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.utility import UnicodeCSVWriter
from pyfarm.jobtypes.core.log import (
    CREATE_LOG_LOCK, STDOUT, STDERR, STREAMS, CSVLog, open_log)


class TestModuleLevel(TestCase):
    def test_stdout(self):
        self.assertEqual(STDOUT, 0)

    def test_stderr(self):
        self.assertEqual(STDERR, 1)

    def test_streams(self):
        self.assertEqual(STREAMS, set([STDERR, STDOUT]))

    def test_lock_type(self):
        # We only want one thread to run open_log
        # can't use isinstance check....
        self.assertEqual(CREATE_LOG_LOCK.__class__.__name__, "lock")

    def test_open_log_creates_dir(self):
        outdir = join(mkdtemp(), urandom(8).encode("hex"))
        outfile = join(outdir, "test.log")
        self.add_cleanup_path(outdir)
        open_log(outfile)
        self.assertTrue(isdir(outdir))

    def test_open_log_creates_file(self):
        outdir = join(mkdtemp(), urandom(8).encode("hex"))
        outfile = join(outdir, "test.log")
        self.add_cleanup_path(outdir)
        result = open_log(outfile)
        self.assertTrue(isfile(outfile))
        self.assertIsInstance(result, file)
        self.assertEqual(result.mode, "w")

    def test_file_exists(self):
        outdir = join(mkdtemp(), urandom(8).encode("hex"))
        outfile = join(outdir, "test.log")
        self.add_cleanup_path(outdir)
        open_log(outfile)

        with self.assertRaisesRegexp(OSError, ".*exists.*"):
            open_log(outfile)


class TestCSVLog(TestCase):
    def setUp(self):
        super(TestCSVLog, self).setUp()
        self.log = CSVLog(
            open_log(self.create_test_file(), ignore_existing=True))

    def test_lock_type(self):
        # same thread should have access to its own resources
        # can't use isinstance check....
        self.assertEqual(self.log.lock.__class__.__name__, "_RLock")

    def test_messages(self):
        self.assertIsInstance(self.log.messages, deque)
        self.assertEqual(self.log.messages, deque())

    def test_lines(self):
        self.assertEqual(self.log.lines, 0)

    def test_written(self):
        self.assertEqual(self.log.written, 0)

    def test_writer(self):
        self.assertIsInstance(self.log.csv, UnicodeCSVWriter)

    def test_file(self):
        self.assertIsInstance(self.log.file, file)

    def test_not_a_file(self):
        with self.assertRaises(TypeError):
            CSVLog("")

    def test_write(self):
        data = (datetime.utcnow(), STDOUT, 1, "hello")
        self.log.write(data)
        self.assertEqual(self.log.written, 1)