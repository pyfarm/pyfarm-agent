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
from pyfarm.agent.testutil import TestCase, FakeRequest
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.api.tasks import Tasks


class TestTasks(TestCase):
    def test_leaf(self):
        self.assertTrue(Tasks.isLeaf)

    def test_parent(self):
        self.assertIsInstance(Tasks(), APIResource)

    def test_get_no_request(self):
        tasks = Tasks()
        with self.assertRaises(KeyError):
            self.assertEqual(loads(tasks.get()), config)

    def test_get_request_master_contacted(self):
        tasks = Tasks()
        request = FakeRequest(self, config["master_user_agent"])

        config["current_assignments"] = {}
        assignments = []
        for i in range(5):
            assignments.append(i)
            config["current_assignments"][i] = {"tasks": [i]}

        self.assertEqual(loads(tasks.get(request=request)), assignments)
        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())
