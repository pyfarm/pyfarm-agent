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

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.testutil import TestCase, BaseHTMLTestCase
from pyfarm.agent.http.system import Index, Configuration, mb, seconds


class TestFormat(TestCase):
    def test_mb_from_float(self):
        self.assertEqual(mb(7.42), "7MB")

    def test_mb_from_int(self):
        self.assertEqual(mb(42), "42MB")

    def test_seconds(self):
        self.assertEqual(seconds(1.234321), "1.23 seconds")


class TestIndex(BaseHTMLTestCase):
    URI = ""
    CLASS = Index

    # TODO: test of response content
    def test_render_get(self):
        request = self.get(self)
        instance = self.instance_class()
        result = instance.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)


class TestConfiguration(BaseHTMLTestCase):
    URI = ""
    CLASS = Configuration

    def test_render(self):
        request = self.get(self)
        instance = self.instance_class()
        result = instance.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)

    def test_missing_hidden_fields(self):
        request = self.get(self)
        instance = self.instance_class()
        result = instance.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)
        response = request.response()

        for hidden_field in self.CLASS.HIDDEN_FIELDS:
            self.assertNotIn("<td>" + hidden_field + "</td>", response)

    def test_editable_fields(self):

        request = self.get(self)
        instance = self.instance_class()
        result = instance.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertEqual(request.code, OK)
        response = request.response()

        in_editable = False
        editable_fields = set(self.CLASS.EDITABLE_FIELDS)
        for line in response.splitlines():
            if "Editable Configuration" in line:
                in_editable = True

            if in_editable:
                for editable_field in editable_fields.copy():
                    if editable_field in line:
                        editable_fields.remove(editable_field)

            if "Read Only Configuration" in line:
                break

        if editable_fields:
            self.fail("Editable fields not present in the rendered "
                      "template: %s" % ", ".join(editable_fields))
