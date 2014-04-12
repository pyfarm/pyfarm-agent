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

import os
import re
import logging
from random import randint, choice

from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import succeed
from twisted.trial.unittest import TestCase as _TestCase, SkipTest

from pyfarm.core.config import read_env, read_env_bool
from pyfarm.core.enums import AgentState, UseAgentAddress, PY26, STRING_TYPES
from pyfarm.core.sysinfo import memory, cpu
from pyfarm.agent.entrypoints.commands import STATIC_ROOT
from pyfarm.agent.config import config, logger as config_logger

ENABLE_LOGGING = read_env_bool("PYFARM_AGENT_TEST_LOGGING", False)
PYFARM_AGENT_MASTER = read_env("PYFARM_AGENT_TEST_MASTER", "127.0.0.1:80")
if ":" not in PYFARM_AGENT_MASTER:
    raise ValueError("$PYFARM_AGENT_TEST_MASTER's format should be `ip:port`")


def safe_repr(obj, short=False):
    try:
        result = repr(obj)
    except Exception:
        result = object.__repr__(obj)
    if not short or len(result) < 80:
        return result
    return result[:80] + ' [truncated]...'


class TestCase(_TestCase):
    RAND_LENGTH = 8

    # Global timeout for all test cases.  If an individual test takes
    # longer than this amount of time to execute it will be stopped.  This
    # value should always be set to a value that's *much* longer than the
    # expected duration of the longest test case.
    timeout = 15

    # back ports of some of Python 2.7's unittest features
    if PY26:
        def assertRaisesRegexp(
                self, expected_exception, expected_regexp, callable_obj=None,
                *args, **kwargs):

            exception = None
            try:
                callable_obj(*args, **kwargs)
            except expected_exception, ex:
                exception = ex

            if exception is None:
                self.fail("%s not raised" % str(expected_exception.__name__))

            if isinstance(expected_regexp, STRING_TYPES):
                expected_regexp = re.compile(expected_regexp)

            if not expected_regexp.search(str(exception)):
                self.fail('"%s" does not match "%s"' % (
                    expected_regexp.pattern, str(exception)))

        def assertIsNone(self, obj, msg=None):
            if obj is not None:
                standardMsg = '%s is not None' % (safe_repr(obj),)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsNotNone(self, obj, msg=None):
            if obj is None:
                standardMsg = 'unexpectedly None'
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsInstance(self, obj, cls, msg=None):
            if not isinstance(obj, cls):
                standardMsg = '%s is not an instance of %r' % (
                    safe_repr(obj), cls)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertNotIsInstance(self, obj, cls, msg=None):
            if isinstance(obj, cls):
                standardMsg = '%s is an instance of %r' % (safe_repr(obj), cls)
                self.fail(self._formatMessage(msg, standardMsg))

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
        if not ENABLE_LOGGING:
            logging.getLogger("pf").setLevel(logging.CRITICAL)
        config_logger.disabled = 1
        config.clear(callbacks=True)
        config.update({
            "ram-report-delta": 100,
            "http-retry-delay": 1,
            "persistent-http-connections": False,
            "master-api": "http://%s/api/v1" % PYFARM_AGENT_MASTER,
            "master": PYFARM_AGENT_MASTER.split(":")[0],
            "hostname": os.urandom(self.RAND_LENGTH).encode("hex"),
            "ip": "10.%s.%s.%s" % (
                randint(1, 255), randint(1, 255), randint(1, 255)),
            "use-address": choice(UseAgentAddress),
            "ram": int(memory.total_ram()),
            "cpus": cpu.total_cpus(),
            "port": randint(10000, 50000),
            "free-ram": int(memory.ram_free()),
            "time-offset": randint(-50, 50),
            "state": choice(AgentState),
            "pretty-json": True,
            "ntp-server": "pool.ntp.org",
            "html-templates-reload": True,
            "static-files": STATIC_ROOT})
        config_logger.disabled = 0
