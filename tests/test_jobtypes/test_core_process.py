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

import os
from collections import namedtuple

import psutil
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol

from pyfarm.agent.testutil import TestCase
from pyfarm.jobtypes.core.process import ReplaceEnvironment, ProcessProtocol

DummyInputs = namedtuple("DummyInputs", ("task", ))


class FakeJobType(object):
    def __init__(self):
        self.done = Deferred()
        self.started = False
        self.stopped = False
        self.stdout = []
        self.stderr = []

    def process_started(self, protocol):
        self.started = True

    def process_stopped(self, protocol, reason):
        self.stopped = True
        self.done.callback(True)

    def received_stdout(self, data):
        self.stdout.append(data)

    def received_stderr(self, data):
        self.stderr.append(data)


class TestProtocol(TestCase):
    def launch_process(self, jobtype, args=None):
        if args is None:
            args = ["python", "-c", "import time; time.sleep(3600)"]

        protocol = ProcessProtocol(jobtype, *[None] * 6)
        reactor.spawnProcess(
            protocol, "python", args)
        return protocol

    def test_subclass(self):
        protocol = ProcessProtocol(*[None] * 7)
        self.assertIsInstance(protocol, _ProcessProtocol)

    def test_pid(self):
        fake_jobtype = FakeJobType()
        protocol = self.launch_process(fake_jobtype)
        self.assertEqual(protocol.pid, protocol.transport.pid)
        protocol.transport.signalProcess("KILL")
        return fake_jobtype.done

    def test_process(self):
        fake_jobtype = FakeJobType()
        protocol = self.launch_process(fake_jobtype)
        self.assertIs(protocol.process, protocol.transport)
        protocol.transport.signalProcess("KILL")
        return fake_jobtype.done

    def test_psutil_process_after_exit(self):
        fake_jobtype = FakeJobType()
        protocol = self.launch_process(fake_jobtype)

        def exited(*_):
            self.assertIsNone(protocol.psutil_process)

        fake_jobtype.done.addCallback(exited)
        protocol.transport.signalProcess("KILL")

        return fake_jobtype.done

    def test_psutil_process_running(self):
        fake_jobtype = FakeJobType()
        protocol = self.launch_process(fake_jobtype)
        self.assertIsInstance(protocol.psutil_process, psutil.Process)
        self.assertEqual(protocol.psutil_process.pid, protocol.pid)
        protocol.transport.signalProcess("KILL")
        return fake_jobtype.done

    # TODO: add tests for remaining protocol attributes


class TestReplaceEnvironment(TestCase):
    original_environment = os.environ.copy()

    def tearDown(self):
        super(TestReplaceEnvironment, self).tearDown()
        os.environ.clear()
        os.environ.update(self.original_environment)

    def test_uses_os_environ(self):
        os.environ.clear()
        os.environ.update(
            {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")})
        env = ReplaceEnvironment(None)
        self.assertEqual(env.environment, os.environ)
        self.assertIs(env.environment, os.environ)

    def test_enter(self):
        original = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        frozen = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        os.environ.clear()
        os.environ.update(original)

        with ReplaceEnvironment(frozen, os.environ) as env:
            self.assertEqual(env.original_environment, original)
            self.assertEqual(os.environ, frozen)

    def test_exit(self):
        original = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        original_copy = original.copy()
        frozen = {os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}
        os.environ.clear()
        os.environ.update(original)

        with ReplaceEnvironment(frozen, os.environ) as env:
            pass

        self.assertEqual(os.environ, original_copy)
