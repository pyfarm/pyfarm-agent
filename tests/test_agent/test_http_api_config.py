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
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.api.config import Config


class TestConfig(TestCase):
    def test_leaf(self):
        self.assertFalse(Config.isLeaf)

    def test_parent(self):
        self.assertIsInstance(Config(), APIResource)

    def test_get_no_request(self):
        config_ = Config()
        self.assertEqual(loads(config_.get()), config)

    def test_get_request_master_contacted(self):
        config_ = Config()
        request = FakeRequestWithUserAgent(self, config["master_user_agent"])
        response = loads(config_.get(request=request))
        response.pop("last_master_contact")
        current_config = config.copy()
        current_config.pop("last_master_contact")
        self.assertEqual(response, current_config)
        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())
