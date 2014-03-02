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

import re
from json import dumps

from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import succeed
from twisted.web.test.test_web import DummyRequest as _DummyRequest
from twisted.trial.unittest import TestCase as _TestCase

from pyfarm.core.enums import PY26, STRING_TYPES

if PY26:
    def safe_repr(obj, short=False):
        try:
            result = repr(obj)
        except Exception:
            result = object.__repr__(obj)
        if not short or len(result) < 80:
            return result
        return result[:80] + ' [truncated]...'


def dummy_request(path="", data=None, http_method="GET", headers=None,
                  session=None, json_dumps=True):
    """
    Wrapper around the base dummy request which does not require direct
    subclassing for different kinds of request or lists for a single
    url request.
    """
    class FakeContent(object):
        def __init__(self):
            self._content = None
            self._read = False

        def read(self):
            if self._read is False:
                self._read = True
                return self._content

        def write(self, data):
            self._content = data

    class DummyRequest(_DummyRequest):
        def __init__(self, postpath, session=None):
            _DummyRequest.__init__(self, postpath, session=session)
            self.method = http_method
            self.requestHeaders = {}
            self.content = FakeContent()

        def getHeader(self, name):
            return self.requestHeaders.get(name)

        def setHeader(self, name, value):
            self.requestHeaders[name] = value

    request = DummyRequest([path], session=session)
    if isinstance(headers, dict):
        for key, value in headers.iteritems():
            request.setHeader(key, value)

    if data is not None:
        if json_dumps:
            data = dumps(data)
        request.content.write(data)

    return request


class TestCase(_TestCase):
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

    def _render(self, resource, request):
        result = resource.render(request)

        if isinstance(result, str):
            request.write(result)
            request.finish()
            return succeed(None)

        elif result is NOT_DONE_YET:
            if request.finished:
                return succeed(None)

            else:
                return request.notifyFinish()
        else:
            raise ValueError("Unexpected return value: %r" % (result,))
