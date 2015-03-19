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

from json import loads, dumps
from datetime import datetime

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.config import config
from pyfarm.agent.testutil import BaseAPITestCase, TestCase, DummyRequest
from pyfarm.agent.http.api.base import APIResource, Versions


class TestAPIResource(BaseAPITestCase):
    URI = "/"
    CLASS = APIResource


class TestVersions(BaseAPITestCase):
    URI = "/versions/"
    CLASS = Versions
    DEFAULT_HEADERS = {"User-Agent": config["master_user_agent"]}

    def test_versions(self):
        request = self.get(headers={"User-Agent": config["master_user_agent"]})
        versions = Versions()
        response = versions.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(len(request.written), 1)
        self.assertEqual(loads(request.written[0]), {"versions": [1]})
        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())


class FakeAPIResource(APIResource):
    def post(self, request=None, data=None):
        return dumps(data)


# The idea here is that if Content-Type/Accept are not defined
# then we still process the request.
class TestAcceptsRequestsWithoutHeaders(TestCase):
    data = {"hello": "world"}

    def test_undefined_headers_processes_json(self):
        request = DummyRequest()
        request.method = "POST"
        request.set_content(dumps(self.data))
        resource = FakeAPIResource()
        resource.render(request)

        self.assertEqual(len(request.written), 1)
        self.assertEqual(loads(request.written[0]), self.data)

    def test_rejects_when_accept_not_json(self):
        request = DummyRequest()
        request.method = "POST"
        request.set_content(dumps(self.data))
        request.requestHeaders.setRawHeaders("Accept", ["foobar"])

        resource = FakeAPIResource()
        resource.render(request)

        expected = {
            "error":
                "Can only handle one of %s "
                "here" % FakeAPIResource.ALLOWED_ACCEPT
        }
        self.assertEqual(len(request.written), 1)
        self.assertEqual(loads(request.written[0]), expected)

