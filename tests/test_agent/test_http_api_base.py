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

from json import loads
from datetime import datetime

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.config import config
from pyfarm.agent.testutil import BaseAPITestCase
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
