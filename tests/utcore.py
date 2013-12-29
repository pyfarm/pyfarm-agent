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

from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import succeed
from twisted.web.test.test_web import DummyRequest as _DummyRequest
from twisted.trial.unittest import TestCase as _TestCase


def dummy_request(path="", http_method="GET", headers=None, session=None):
    """
    Wrapper around the base dummy request which does not require direct
    subclassing for different kinds of request or lists for a single
    url request.
    """
    class DummyRequest(_DummyRequest):
        method = http_method

    request = DummyRequest([path], session=session)
    if isinstance(headers, dict):
        for key, value in headers.iteritems():
            request.setHeader(key, value)
    return request


class TestCase(_TestCase):
    try:
        _TestCase.assertRaisesRegexp
    except AttributeError:
        def assertRaisesRegexp(
                self, expected_exception, expected_regexp, callable_obj,
                *args, **kwds):

            exception = None
            try:
                callable_obj(*args, **kwds)
            except expected_exception, ex:
                exception = ex

            if exception is None:
                self.fail("%s not raised" % str(expected_exception.__name__))

            if isinstance(expected_regexp, basestring):
                expected_regexp = re.compile(expected_regexp)

            if not expected_regexp.search(str(exception)):
                self.fail('"%s" does not match "%s"' % (
                    expected_regexp.pattern, str(exception)))

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
