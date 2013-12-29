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

from twisted.trial.unittest import TestCase as _TestCase


class TestCase(_TestCase):
    # try:
    #     _TestCase.assertRaisesRegexp
    # except AttributeError:
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
