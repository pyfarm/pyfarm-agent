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
from pyfarm.agent.testutil import BaseAPITestCase
from pyfarm.agent.http.api.config import Config


class TestConfig(BaseAPITestCase):
    URI = "/config"
    CLASS = Config

    def test_get_config(self):
        request = self.get(headers={"User-Agent": config["master_user_agent"]})
        config_ = Config()
        response = loads(config_.render(request))
        response.pop("last_master_contact")
        current_config = config.copy()
        current_config.pop("last_master_contact")
        response.pop("agent")

        for key in response:
            self.assertIn(key, current_config)
            self.assertEqual(
                response[key], current_config[key],
                "Data for key %r %r != %r" % (
                    key, response[key], current_config[key]))

        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())
