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

from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, FakeRequestWithUserAgent
from pyfarm.agent.http.api.base import APIResource, Versions


class TestAPIResource(TestCase):
    def test_leaf(self):
        self.assertTrue(APIResource.isLeaf)

    def test_content_types(self):
        self.assertEqual(APIResource.CONTENT_TYPES, set(["application/json"]))


class TestVersions(TestCase):
    def test_leaf(self):
        self.assertTrue(Versions.isLeaf)

    def test_get_no_request(self):
        versions = Versions()
        self.assertEqual(loads(versions.get()), {"versions": [1]})

    def test_get_request_master_contacted(self):
        versions = Versions()
        request = FakeRequestWithUserAgent(self, config["master_user_agent"])
        self.assertEqual(
            loads(versions.get(request=request)), {"versions": [1]})
        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())