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

import atexit
import logging
import json
import os
import re
import socket
import sys
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from functools import wraps, partial
from os import urandom
from os.path import basename, isfile
from random import randint, choice
from StringIO import StringIO
from textwrap import dedent
from urllib import urlopen

try:
    from httplib import OK, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, CREATED

from twisted.internet.base import DelayedCall
from twisted.trial.unittest import TestCase as _TestCase, SkipTest, FailTest

from pyfarm.core.config import read_env, read_env_bool
from pyfarm.core.enums import AgentState, PY26, STRING_TYPES
from pyfarm.agent.http.core.client import post
from pyfarm.agent.config import config, logger as config_logger
from pyfarm.agent.sysinfo import memory, cpu

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

from voluptuous import Schema
from twisted.internet.defer import Deferred, succeed
from pyfarm.agent.entrypoints.parser import AgentArgumentParser
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.core.template import DeferredTemplate
from pyfarm.agent.utility import dumps

ENABLE_LOGGING = read_env_bool("PYFARM_AGENT_TEST_LOGGING", False)
PYFARM_AGENT_MASTER = read_env("PYFARM_AGENT_TEST_MASTER", "127.0.0.1:80")

if ":" not in PYFARM_AGENT_MASTER:
    raise ValueError("$PYFARM_AGENT_TEST_MASTER's format should be `ip:port`")

os.environ["PYFARM_AGENT_TEST_RUNNING"] = str(os.getpid())


try:
    response = urlopen("http://" + PYFARM_AGENT_MASTER)
    PYFARM_MASTER_API_ONLINE = response.code == OK
except Exception as e:
    PYFARM_MASTER_API_ONLINE = False


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


def random_port(bind="127.0.0.1"):
    """Returns a random port which is not in use"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((bind, 0))
        _, port = sock.getsockname()
        return port
    finally:
        sock.close()


def requires_master(function):
    """
    Any test decorated with this function will fail if the master could
    not be contacted or returned a response other than 200 OK for "/"
    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        if not PYFARM_MASTER_API_ONLINE:
            raise FailTest("Could not connect to master")
        return function(*args, **kwargs)
    return wrapper


def create_jobtype(classname=None, sourcecode=None):
    """Creates a job type on the master and fires a deferred when finished"""
    if classname is None:
        classname = "Test%s" % urandom(8).encode("hex")

    if sourcecode is None:
        sourcecode = dedent("""
        from pyfarm.jobtypes.core.jobtype import JobType
        class %s(JobType):
            pass""" % classname)

    finished = Deferred()

    def posted(response):
        if response.code == CREATED:
            finished.callback(response.json())
        else:
            finished.errback(response.json())

    post(config["master_api"] + "/jobtypes/",
         callback=posted, errback=finished.errback,
         data={"name": classname,
               "classname": classname,
               "code": sourcecode})

    return finished


class FakeRequestHeaders(object):
    def __init__(self, test, headers):
        self.test = test
        self.test.assertIsInstance(headers, dict)

        for key, value in headers.items():
            headers[key.lower()] = value

        self.headers = headers

    def getRawHeaders(self, header):
        return self.headers.get(header)


class FakeRequest(object):
    def __init__(self, test, method, uri, headers=None, data=None):
        if headers is None:
            headers = {}

        if "Content-Type" not in headers:
            headers.update({"Content-Type": ["application/json"]})

        if data is not None:
            data = dumps(data)

        self.test = test
        self.method = method
        self.uri = uri
        self.code = None
        self.finished = None
        self.requestHeaders = FakeRequestHeaders(test, headers)
        self.content = StringIO()
        self._response = StringIO()

        if isinstance(data, STRING_TYPES):
            self.content.write(data)
            self.content.seek(0)

    def getHeader(self, header):
        return self.requestHeaders.getRawHeaders(header)

    def setResponseCode(self, code):
        self.test.assertIsNone(
            self.finished, "finished() called before setResponseCode()")
        self.code = code

    def write(self, data):
        self.test.assertIsNone(
            self.finished, "finished() called before write()")
        if not isinstance(data, STRING_TYPES):
            data = dumps(data)
        self._response.write(data)

    def finish(self):
        self.test.assertIsNone(self.finished, "finish() already called")
        self._response.seek(0)
        self.finished = True

    def response(self):
        self.test.assertIsNotNone(self.finished, "finish() not called")
        if not self._response.len:
            raise ValueError("Not content.")

        try:
            response = json.load(self._response)
        except ValueError:
            self._response.seek(0)
            response = self._response.read()

        self._response.seek(0)
        return response


class FakeAgent(object):
    def __init__(self, stopped=None):
        if stopped is None:
            stopped = Deferred()
        self.stopped = stopped

    def stop(self):
        if isinstance(self.stopped, Deferred):
            self.stopped.callback(None)
        return self.stopped


class ErrorCapturingParser(AgentArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ErrorCapturingParser, self).__init__(*args, **kwargs)
        self.errors = []

    def error(self, message):
        self.errors.append(message)


class TestCase(_TestCase):
    longMessage = True
    POP_CONFIG_KEYS = []
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

    def assertDateAlmostEqual(
            self, date1, date2,
            second_deviation=0, microsecond_deviation=1000000 / 2):
        self.assertIsInstance(date1, datetime)
        self.assertIsInstance(date2, datetime)
        self.assertEqual(date1.year, date2.year)
        self.assertEqual(date1.month, date2.month)
        self.assertEqual(date1.day, date2.day)
        self.assertEqual(date1.hour, date2.hour)
        self.assertEqual(date1.minute, date2.minute)
        self.assertEqual(date1.second, date2.second)
        self.assertApproximates(
            date1.second, date2.second, second_deviation)
        self.assertApproximates(
            date1.microsecond, date2.microsecond, microsecond_deviation)

    # back ports of some of Python 2.7's unittest features
    if PY26:
        def _formatMessage(self, msg, standardMsg):
            if not self.longMessage:
                return msg or standardMsg
            if msg is None:
                return standardMsg
            try:
                return '%s : %s' % (standardMsg, msg)
            except UnicodeDecodeError:
                return '%s : %s' % (standardMsg, msg)

        def assertLessEqual(self, a, b, msg=None):
            if not a <= b:
                self.fail(
                    self._formatMessage(
                        msg, '%s not less than or equal to %s' % (a, b)))

        def assertGreaterEqual(self, a, b, msg=None):
            if not a >= b:
                self.fail(
                    self._formatMessage(
                        msg, '%s not greater than or equal to %s' % (a, b)))

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
        super(TestCase, self).setUp()

        try:
            self._pop_config_keys
        except AttributeError:
            self._pop_config_keys = []

        self._pop_config_keys.extend(self.POP_CONFIG_KEYS)
        self._pop_config_keys.extend([
            "agent",
            "jobs",
            "jobtypes",
            "restart_requested",
            "current_assignments",
            "last_master_contact"])

        DelayedCall.debug = True
        if not ENABLE_LOGGING:
            logging.getLogger("pf").setLevel(logging.CRITICAL)

        config_logger_disabled = config_logger.disabled
        config_logger.disabled = True
        self.prepare_config()
        config_logger.disabled = config_logger_disabled

    def prepare_config(self):
        for key in self._pop_config_keys:
            config.pop(key, None)

        config.update({
            "shutting_down": False,
            "jobtypes": {},
            "current_assignments": {},
            "agent_id": uuid.uuid4(),
            "agent_http_retry_delay": 1,
            "agent_http_persistent_connections": False,
            "master": PYFARM_AGENT_MASTER,
            "agent_hostname": os.urandom(self.RAND_LENGTH).encode("hex"),
            "agent_ram": memory.total_ram(),
            "agent_cpus": cpu.total_cpus(),
            "agent_api_port": randint(10000, 50000),
            "free_ram": memory.free_ram(),
            "agent_time_offset": randint(-50, 50),
            "state": choice(AgentState),
            "start": time.time(),
            "agent_pretty_json": False,
            "agent_html_template_reload": True,
            "agent_master_reannounce": randint(5, 15)})

    def _rmdir(self, path, on_exit=True):
        try:
            shutil.rmtree(path)
        except (IOError, OSError):
            if on_exit:
                atexit.register(self._rmdir, path, on_exit=False)

    def create_file(self, content=None, dir=None, suffix=""):
        """
        Creates a test file on disk using :func:`tempfile.mkstemp`
        and uses the lower level file interfaces to manage it.  This
        is done to ensure we have more control of the file descriptor
        itself so on platforms such as Windows we don't have to worry
        about running out of file handles.
        """
        fd, path = tempfile.mkstemp(suffix=suffix, dir=dir, text=True)

        if content is not None:
            stream = os.fdopen(fd, "w")
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

            try:
                os.close(stream.fileno())
            except (IOError, OSError):
                pass
        else:
            try:
                os.close(fd)
            except (IOError, OSError):
                pass

        # self.addCleanup(self._closefd, fd)
        return path

    def create_directory(self, count=10):
        directory = tempfile.mkdtemp()
        self.addCleanup(self._rmdir, directory)

        files = []
        for _ in range(count):
            files.append(self.create_file(dir=directory))

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


class BaseHTTPTestCase(TestCase):
    URI = NotImplemented
    CLASS = NotImplemented
    CLASS_FACTORY = NotImplemented
    CONTENT_TYPES = NotImplemented

    # Only run the real _run if we're inside a child
    # class.
    def _run(self, methodName, result):
        if self.CLASS is NotImplemented:
            return succeed(True)

        if self.CLASS is not NotImplemented and self.URI is NotImplemented:
            self.fail("URI not set")

        self.assertIsInstance(self.CONTENT_TYPES, list, "CONTENT_TYPES not set")
        return super(BaseHTTPTestCase, self)._run(methodName, result)

    def setUp(self):
        super(BaseHTTPTestCase, self).setUp()
        self.agent = config["agent"] = FakeAgent()
        self.assertIsNotNone(self.CLASS, "CLASS not set")

    def instance_class(self):
        if self.CLASS_FACTORY is not NotImplemented:
            return self.CLASS_FACTORY()
        else:
            return self.CLASS()

    def test_instance(self):
        self.instance_class()

    def test_leaf(self):
        if self.URI.endswith("/"):
            self.assertTrue(self.CLASS.isLeaf)
        else:
            self.assertFalse(self.CLASS.isLeaf)

    def test_implements_methods(self):
        instance = self.instance_class()
        for method_name in instance.methods:
            if method_name == "head":
                continue

            self.assertTrue(
                hasattr(instance, method_name),
                "%s does not have method %s" % (self.CLASS, method_name))
            self.assertTrue(callable(getattr(instance, method_name)))

    def test_content_types(self):
        self.assertIsInstance(self.CLASS.CONTENT_TYPES, set)
        for content_type in self.CONTENT_TYPES:
            self.assertIn(content_type, self.CLASS.CONTENT_TYPES,
                          "missing content type %s" % content_type)

    def test_methods_exist_for_schema(self):
        self.assertIsInstance(self.CLASS.SCHEMAS, dict)
        instance = self.instance_class()
        methods = set(method.upper() for method in instance.methods)
        for method, schema in self.CLASS.SCHEMAS.items():
            self.assertIsInstance(schema, Schema)
            self.assertEqual(
                method.upper(), method,
                "method name in schema must be upper case")
            self.assertNotEqual(method, "GET", "cannot have schema for GET")
            self.assertIn(method, methods)

    def test_missing_schemas(self):
        missing_schemas = []
        for method in self.CLASS.LOAD_DATA_FOR_METHODS:
            if method not in self.CLASS.SCHEMAS:
                missing_schemas.append(method)

        if missing_schemas:
            self.skipTest(
                "WARNING: Missing schema(s) for %s"
                % ", ".join(missing_schemas))


class BaseAPITestCase(BaseHTTPTestCase):
    CONTENT_TYPES = ["application/json"]

    def setUp(self):
        super(BaseAPITestCase, self).setUp()
        self.assertIsNotNone(self.URI, "URI not set")
        self.get = partial(FakeRequest, self, "GET", self.URI)
        self.post = partial(FakeRequest, self, "POST", self.URI)
        self.put = partial(FakeRequest, self, "PUT", self.URI)

    def test_parent(self):
        self.assertIsInstance(self.instance_class(), APIResource)


class BaseHTMLTestCase(BaseHTTPTestCase):
    CONTENT_TYPES = ["text/html", "application/json"]

    def setUp(self):
        super(BaseHTMLTestCase, self).setUp()
        self.get = partial(FakeRequest, self, "GET")
        self.post = partial(FakeRequest, self, "POST")
        self.put = partial(FakeRequest, self, "PUT")

    def test_template_set(self):
        self.assertIsNot(self.CLASS.TEMPLATE, NotImplemented)

    def test_template_loaded(self):
        instance = self.instance_class()
        template = instance.template
        self.assertIsInstance(template, DeferredTemplate)
        self.assertEqual(basename(template.filename), self.CLASS.TEMPLATE)
        self.assertTrue(isfile(template.filename))
