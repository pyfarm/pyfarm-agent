# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
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

import logging
import os
import re
import socket
import sys
import tempfile
from functools import wraps
from random import randint, choice
from urllib import urlopen

try:
    from unittest.case import _AssertRaisesContext

except ImportError:  # copied from Python 2.7's source
    class _AssertRaisesContext(object):
        def __init__(self, expected, test_case, expected_regexp=None):
            self.expected = expected
            self.failureException = test_case.failureException
            self.expected_regexp = expected_regexp

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb):
            if exc_type is None:
                try:
                    exc_name = self.expected.__name__
                except AttributeError:
                    exc_name = str(self.expected)
                raise self.failureException(
                    "{0} not raised".format(exc_name))
            if not issubclass(exc_type, self.expected):
                # let unexpected exceptions pass through
                return False
            self.exception = exc_value # store for later retrieval
            if self.expected_regexp is None:
                return True

            expected_regexp = self.expected_regexp
            if isinstance(expected_regexp, STRING_TYPES):
                expected_regexp = re.compile(expected_regexp)
            if not expected_regexp.search(str(exc_value)):
                raise self.failureException('"%s" does not match "%s"' %
                         (expected_regexp.pattern, str(exc_value)))
            return True

from twisted.internet.base import DelayedCall
from twisted.trial.unittest import TestCase as _TestCase, SkipTest

from pyfarm.agent.logger import start_logging

start_logging()

from pyfarm.core.config import read_env, read_env_bool
from pyfarm.core.enums import AgentState, PY26, STRING_TYPES
from pyfarm.agent.config import config, logger as config_logger
from pyfarm.agent.entrypoints.commands import STATIC_ROOT
from pyfarm.agent.sysinfo import memory, cpu, system
from pyfarm.agent.utility import rmpath

ENABLE_LOGGING = read_env_bool("PYFARM_AGENT_TEST_LOGGING", False)
PYFARM_AGENT_MASTER = read_env("PYFARM_AGENT_TEST_MASTER", "127.0.0.1:80")

if ":" not in PYFARM_AGENT_MASTER:
    raise ValueError("$PYFARM_AGENT_TEST_MASTER's format should be `ip:port`")

os.environ["PYFARM_AGENT_TEST_RUNNING"] = str(os.getpid())


class skipIf(object):
    """
    Wrapping a test with this class will allow the test to
    be skipped if ``should_skip`` evals as True.
    """
    def __init__(self, should_skip, reason):
        self.should_skip = should_skip
        self.reason = reason

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if callable(self.should_skip) and self.should_skip() \
                    or self.should_skip:
                raise SkipTest(self.reason)
            return func(*args, **kwargs)
        return wrapper


class TestCase(_TestCase):
    RAND_LENGTH = 8

    # Global timeout for all test cases.  If an individual test takes
    # longer than this amount of time to execute it will be stopped.  This
    # value should always be set to a value that's *much* longer than the
    # expected duration of the longest test case.
    timeout = 15

    # Override the default `assertRaises` which does not provide
    # context management.
    def assertRaises(self, excClass, callableObj=None, *args, **kwargs):
        if excClass is AssertionError and sys.flags.optimize:
            self.skipTest(
                "AssertionError will never be raised, running in optimized "
                "mode.")

        context = _AssertRaisesContext(excClass, self)
        if callableObj is None:
            return context
        with context:
            callableObj(*args, **kwargs)

    # Override the default `assertRaisesRegexp` which does not provide
    # context management.
    def assertRaisesRegexp(self, expected_exception, expected_regexp,
                           callable_obj=None, *args, **kwargs):
        if expected_exception is AssertionError and sys.flags.optimize:
            self.skipTest(
                "AssertionError will never be raised, running in optimized "
                "mode.")

        context = _AssertRaisesContext(
            expected_exception, self, expected_regexp)
        if callable_obj is None:
            return context
        with context:
            callable_obj(*args, **kwargs)

    # back ports of some of Python 2.7's unittest features
    if PY26:
        def assertIsNone(self, obj, msg=None):
            if obj is not None:
                self.fail(self._formatMessage(msg, "%r is not None" % obj))

        def assertIsNotNone(self, obj, msg=None):
            if obj is None:
                self.fail(self._formatMessage(msg, "unexpectedly None"))

        def assertIsInstance(self, obj, cls, msg=None):
            if not isinstance(obj, cls):
                self.fail(
                    self._formatMessage(
                        msg, "%r is not an instance of %r" % (obj, cls)))

        def assertNotIsInstance(self, obj, cls, msg=None):
            if isinstance(obj, cls):
                self.fail(
                    self._formatMessage(
                        msg, "%r is an instance of %r" % (obj, cls)))

        def assertIn(self, containee, container, msg=None):
            if containee not in container:
                raise self.failureException(msg or "%r not in %r"
                                            % (containee, container))
            return containee

        def assertNotIn(self, containee, container, msg=None):
            if containee in container:
                raise self.failureException(msg or "%r in %r"
                                            % (containee, container))
            return containee

        def skipTest(self, reason):
            raise SkipTest(reason)

    def setUp(self):
        DelayedCall.debug = True
        if not ENABLE_LOGGING:
            logging.getLogger("pf").setLevel(logging.CRITICAL)
        config_logger.disabled = 1
        config.pop("agent", None)
        config.pop("agent-id", None)
        config.update({
            "systemid": system.system_identifier(),
            "ram-report-delta": 100,
            "http-retry-delay": 1,
            "persistent-http-connections": False,
            "master-api": "http://%s/api/v1" % PYFARM_AGENT_MASTER,
            "master": PYFARM_AGENT_MASTER.split(":")[0],
            "hostname": os.urandom(self.RAND_LENGTH).encode("hex"),
            "ram": int(memory.total_ram()),
            "cpus": cpu.total_cpus(),
            "port": randint(10000, 50000),
            "free-ram": int(memory.ram_free()),
            "time-offset": randint(-50, 50),
            "state": choice(AgentState),
            "pretty-json": True,
            "ntp-server": "pool.ntp.org",
            "html-templates-reload": True,
            "static-files": STATIC_ROOT,
            "master-reannounce": randint(5, 15)})
        config_logger.disabled = 0

    def add_cleanup_path(self, path):
        self.addCleanup(rmpath, path, exit_retry=True)

    def create_test_file(self, content="Hello, World!", suffix=".txt"):
        fd, path = tempfile.mkstemp(suffix=suffix)
        self.add_cleanup_path(path)
        with open(path, "w") as stream:
            stream.write(content)
        return path

    def create_test_directory(self, count=10):
        directory = tempfile.mkdtemp()
        self.add_cleanup_path(directory)
        files = []
        for i in range(count):
            fd, tmpfile = tempfile.mkstemp(dir=directory)
            files.append(tmpfile)
        return directory, files


class BaseRequestTestCase(TestCase):
    HTTP_SCHEME = read_env("PYFARM_AGENT_TEST_HTTP_SCHEME", "http")
    DNS_HOSTNAME = config["agent_unittest"]["dns_test_hostname"]
    TEST_URL = config[
        "agent_unittest"]["client_api_test_url_%s" % HTTP_SCHEME]
    REDIRECT_TARGET = config["agent_unittest"]["client_redirect_target"]

    # DNS working?
    try:
        socket.gethostbyname(DNS_HOSTNAME)
    except socket.gaierror:
        RESOLVED_DNS_NAME = False
    else:
        RESOLVED_DNS_NAME = True

    # Basic http request working?
    try:
        urlopen(TEST_URL)
    except IOError:
        HTTP_REQUEST_SUCCESS = False
    else:
        HTTP_REQUEST_SUCCESS = True

    def setUp(self):
        if not self.RESOLVED_DNS_NAME:
            self.skipTest("Could not resolve hostname %s" % self.DNS_HOSTNAME)

        if not self.HTTP_REQUEST_SUCCESS:
            self.skipTest(
                "Failed to send an http request to %s" % self.TEST_URL)
