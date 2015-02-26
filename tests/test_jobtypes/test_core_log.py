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
from collections import deque
from datetime import datetime
from os import urandom, remove
from os.path import join, isfile, isdir, abspath
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from pyfarm.core.enums import PY26
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.utility import UnicodeCSVWriter
from pyfarm.agent.sysinfo.cpu import total_cpus
from pyfarm.jobtypes.core.log import (
    CREATE_LOG_LOCK, STDOUT, STDERR, STREAMS, CSVLog, LoggerPool, logpool,
    open_log)


class FakeProtocol(object):
    def __init__(self):
        self.uuid = uuid4()


class TestModuleLevel(TestCase):
    def test_stdout(self):
        self.assertEqual(STDOUT, 0)

    def test_stderr(self):
        self.assertEqual(STDERR, 1)

    def test_streams(self):
        self.assertEqual(STREAMS, set([STDERR, STDOUT]))

    @skipIf(PY26, "Python 2.7+")
    def test_lock_type(self):
        # We only want one thread to run open_log
        # can't use isinstance check....
        self.assertEqual(CREATE_LOG_LOCK.__class__.__name__, "lock")

    def test_open_log_creates_dir(self):
        outdir, _ = self.create_directory(0)
        outfile = join(outdir, "test.log")
        open_log(outfile)
        self.assertTrue(isdir(outdir))

    def test_open_log_creates_file(self):
        outdir, _ = self.create_directory(0)
        outfile = join(outdir, "test.log")
        result = open_log(outfile)
        self.assertTrue(isfile(outfile))
        self.assertIsInstance(result, file)
        self.assertEqual(result.mode, "wb")

    def test_file_exists(self):
        outdir, _ = self.create_directory(0)
        outfile = join(outdir, "test.log")
        open_log(outfile)

        with self.assertRaisesRegexp(OSError, re.compile(".*exists.*")):
            open_log(outfile)


class TestCSVLog(TestCase):
    def setUp(self):
        super(TestCSVLog, self).setUp()
        self.log = CSVLog(
            open_log(self.create_file(), ignore_existing=True))

    @skipIf(PY26, "Python 2.7+")
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


class TestLoggerPool(TestCase):
    def setUp(self):
        super(TestLoggerPool, self).setUp()
        self.pool = None
        config["jobtype_logging_threadpool"]["min_threads"] = 1
        config["jobtype_logging_threadpool"]["max_threads"] = 2

    def tearDown(self):
        if self.pool is not None:
            self.pool.stop()

    def create_file(self, create=True):
        path = super(TestLoggerPool, self).create_file()
        if not create:
            remove(path)
        return path

    def test_existing_pool(self):
        self.assertIsInstance(logpool, LoggerPool)

    def test_invalid_minthreads(self):
        config["jobtype_logging_threadpool"]["min_threads"] = 0

        with self.assertRaises(ValueError):
            LoggerPool()

    def test_auto_max_maxthreads(self):
        config["jobtype_logging_threadpool"]["max_threads"] = "auto"
        pool = LoggerPool()
        self.assertEqual(
            pool.max, max(min(int(total_cpus() * 1.5), 20), pool.min))

    def test_minthreads_greater_than_maxthreads(self):
        config["jobtype_logging_threadpool"]["min_threads"] = 5
        config["jobtype_logging_threadpool"]["max_threads"] = 1

        with self.assertRaises(ValueError):
            LoggerPool()

    def test_protocol_already_open(self):
        protocol = FakeProtocol()
        pool = LoggerPool()
        pool.logs[protocol.uuid] = None
        with self.assertRaises(KeyError):
            pool.open_log(protocol, self.create_file())

    def test_creates_log(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.start()
        log_created = pool.open_log(protocol, path)

        def created(result):
            proto, log = result
            self.assertEqual(proto, protocol.uuid)
            self.assertIsInstance(log, CSVLog)
            self.assertTrue(isfile(log.file.name))
            self.assertEqual(abspath(log.file.name), abspath(path))

        log_created.addCallback(created)
        return log_created

    def test_no_log_when_stopped(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.start()
        log_created = pool.open_log(protocol, path)

        def created(_):
            pool.stop()
            pool.log(protocol.uuid, STDOUT, "")
            self.assertEqual(pool.logs, {})

        log_created.addCallback(created)
        return log_created

    def test_log(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.start()
        log_created = pool.open_log(protocol, path)

        def created(_):
            message = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[0][-1], message)
            self.assertEqual(pool.logs[protocol.uuid].lines, 1)

        log_created.addCallback(created)
        return log_created

    def test_flush_from_log(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.max_queued_lines = 2
        pool.flush_lines = 1
        pool.start()
        log_created = pool.open_log(protocol, path)
        finished = Deferred()

        def created(_):
            # log two messages
            message1 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message1)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[0][-1], message1)
            message2 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message2)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[1][-1], message2)
            self.assertEqual(pool.logs[protocol.uuid].lines, 2)

            # log a third message (which should cause a flush)
            message3 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message3)

            # Keep checking to see if the data has been flushed
            def check_for_flush():
                if list(pool.logs[protocol.uuid].messages) == []:
                    self.assertEqual(pool.logs[protocol.uuid].written, 0)
                    finished.callback(True)
                else:
                    # not flushed yet maybe?
                    reactor.callLater(.1, check_for_flush)

            reactor.callLater(.1, check_for_flush)

        log_created.addCallback(created)
        return finished

    def test_flush_log_object(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.flush_lines = 1
        pool.start()
        log_created = pool.open_log(protocol, path)

        def created(_):
            # log two messages
            message1 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message1)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[0][-1], message1)
            message2 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message2)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[1][-1], message2)
            self.assertEqual(pool.logs[protocol.uuid].lines, 2)

            result = pool.flush(pool.logs[protocol.uuid])
            self.assertEqual(list(pool.logs[protocol.uuid].messages), [])
            self.assertIs(result, pool.logs[protocol.uuid])
            self.assertEqual(pool.logs[protocol.uuid].written, 0)

        log_created.addCallback(created)
        return log_created

    def test_stop(self):
        path = self.create_file(create=False)
        protocol = FakeProtocol()
        pool = self.pool = LoggerPool()
        pool.start()
        log_created = pool.open_log(protocol, path)

        def created(_):
            # log two messages
            message1 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message1)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[0][-1], message1)
            message2 = urandom(16).encode("hex")
            pool.log(protocol.uuid, STDOUT, message2)
            self.assertEqual(
                list(pool.logs[protocol.uuid].messages)[1][-1], message2)
            self.assertEqual(pool.logs[protocol.uuid].lines, 2)

            log = pool.logs[protocol.uuid]
            self.assertFalse(pool.stopped)
            pool.stop()
            self.assertTrue(pool.stopped)
            self.assertNotIn(protocol.uuid, pool.logs)
            self.assertTrue(log.file.closed)

        log_created.addCallback(created)
        return log_created

    def test_start(self):
        existing_entries = reactor._eventTriggers["shutdown"].before[:]
        pool = self.pool = LoggerPool()
        pool.start()
        self.assertTrue(pool.started)

        for entry in reactor._eventTriggers["shutdown"].before:
            if entry not in existing_entries and entry[0] == pool.stop:
                break
        else:
            self.fail("Shutdown even trigger not added")
