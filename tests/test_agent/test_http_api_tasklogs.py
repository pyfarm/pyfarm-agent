# No shebang line, this module is meant to be imported
#
# Copyright 2015 Oliver Palmer
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

import os
from os.path import join
from json import dumps

try:
    from httplib import BAD_REQUEST, OK, NOT_FOUND, INTERNAL_SERVER_ERROR
except ImportError:  # pragma: no cover
    from http.client import BAD_REQUEST, OK, NOT_FOUND, INTERNAL_SERVER_ERROR

from pyfarm.agent.http.api.tasklogs import TaskLogs
from pyfarm.agent.config import config
from pyfarm.agent.testutil import BaseAPITestCase
from pyfarm.agent.utility import remove_directory


try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

class TestTaskLogs(BaseAPITestCase):
    URI = "/tasklogs/"
    CLASS = TaskLogs

    def setUp(self):
        super(TestTaskLogs, self).setUp()
        self.identifier = os.urandom(6).encode("hex")
        self.log = join(config["jobtype_task_logs"], self.identifier)
        self.log_content = os.urandom(64)

    def tearDown(self):
        super(TestTaskLogs, self).tearDown()
        remove_directory(
            config["jobtype_task_logs"], retry_on_exit=True, raise_=False)

    def create_log(self):
        os.makedirs(config["jobtype_task_logs"])
        with open(self.log, "wb") as log:
            log.write(self.log_content)

    def test_methods_exist_for_schema(self):
        self.skipTest("No schemas defined")

    def test_master_contacted(self):
        """
        Make sure that if the request is coming
        from the master that is sets the value in
        the config.
        """
        self.create_log()
        try:
            last_master_contact = config["last_master_contact"]
        except KeyError:
            last_master_contact = None

        request = self.get(
            uri=[self.identifier],
            headers={"User-Agent": config["master_user_agent"]}
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertNotEqual(last_master_contact, config["last_master_contact"])

    def test_fails_postpath_more_than_one(self):
        request = self.get(
            uri=[self.identifier, "foobar"]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(
            request.written,
            ['{"error": "Did not specify a log identifier"}'])

    def test_fails_postpath_less_than_one(self):
        request = self.get(
            uri=[]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(
            request.written,
            ['{"error": "Did not specify a log identifier"}'])

    def test_log_identifier_must_not_contain_forward_slash(self):
        request = self.get(
            uri=[self.identifier + "/"]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(
            request.written,
            ['{"error": "log_identifier must not contain'
             ' directory separators"}'])

    def test_log_identifier_must_not_contain_back_slash(self):
        request = self.get(
            uri=[self.identifier + "\\"]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(
            request.written,
            ['{"error": "log_identifier must not contain'
             ' directory separators"}'])

    def test_receives_log(self):
        self.create_log()
        request = self.get(
            uri=[self.identifier]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, [self.log_content])
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["text/csv"])

    def test_log_does_not_exist(self):
        # don't call self.create_log()
        request = self.get(
            uri=[self.identifier]
        )
        tasklogs = self.instance_class()
        tasklogs.render(request)
        self.assertEqual(request.responseCode, NOT_FOUND)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(
            request.written, [dumps({"error": "%s does not exist" % self.log})])

    def test_unhandled_error_while_opening_log(self):
        # don't call self.create_log()
        request = self.get(
            uri=[self.identifier]
        )

        tasklogs = self.instance_class()

        def raise_(*_):
            raise Exception("foobar failure")

        self.patch(tasklogs, "_open_file", raise_)

        tasklogs.render(request)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/json"])
        self.assertEqual(request.written, ['{"error": "foobar failure"}'])

