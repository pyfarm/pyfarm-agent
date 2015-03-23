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

from json import loads
from datetime import datetime

from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.config import config
from pyfarm.agent.testutil import BaseAPITestCase
from pyfarm.agent.http.api.tasks import Tasks


class TestTasks(BaseAPITestCase):
    URI = "/tasks/"
    CLASS = Tasks
    DEFAULT_HEADERS = {"User-Agent": config["master_user_agent"]}

    def setUp(self):
        super(TestTasks, self).setUp()
        config["current_assignments"] = {}
        self.assignments = []
        for i in range(5):
            self.assignments.append(i)
            config["current_assignments"][i] = {"tasks": [i]}

    def test_get_tasks(self):
        request = self.get()
        tasks = Tasks()
        response = tasks.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(len(request.written), 1)
        self.assertEqual(loads(request.written[0]), self.assignments)
        self.assertDateAlmostEqual(
            config.master_contacted(update=False), datetime.utcnow())

    def test_delete_tasks(self):
        self.skipTest("TODO: test_delete_tasks()")