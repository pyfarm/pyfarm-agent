# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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
from os.path import basename

from twisted.web.test.requesthelper import DummyChannel, DummyRequest
from twisted.web.error import Error

from pyfarm.core.config import read_env_bool
from pyfarm.agent.http.core.server import Site, StaticPath
from pyfarm.agent.testutil import TestCase


class DummyTransport(object):
    def __init__(self):
        self.data = []

    def writeSequence(self, data):
        self.data.append(data)

    def write(self, data):
        self.data.append(data)

class TestSite(TestCase):
    def test_display_traceback(self):
        self.assertEqual(
            read_env_bool("PYFARM_AGENT_API_DISPLAY_TRACEBACKS", True),
            Site.displayTracebacks)


class TestStaticPath(TestCase):
    def setUp(self):
        super(TestStaticPath, self).setUp()
        self.allow_directory_listing = StaticPath.ALLOW_DIRECTORY_LISTING

    def tearDown(self):
        super(TestStaticPath, self).tearDown()
        StaticPath.ALLOW_DIRECTORY_LISTING = self.allow_directory_listing

    def test_path_does_not_exist(self):
        with self.assertRaises(OSError):
            StaticPath(os.urandom(48).encode("hex"))

    def test_render_cache_header(self):
        path = self.create_file()
        request = DummyRequest(DummyChannel(), 1)
        request.method = "GET"
        static_path = StaticPath(path)
        static_path.render(request)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Cache-Control"),
            ["max-age=%s" % StaticPath.EXPIRES])

    def test_can_list_directory(self):
        directory, files = self.create_directory()
        StaticPath.ALLOW_DIRECTORY_LISTING = True
        static_path = StaticPath(directory)
        lister = static_path.directoryListing()
        self.assertEqual(set(lister.dirs), set(map(basename, files)))

    def test_can_not_list_directory(self):
        directory, files = self.create_directory()
        StaticPath.ALLOW_DIRECTORY_LISTING = False
        static_path = StaticPath(directory)
        with self.assertRaises(Error):
            static_path.directoryListing()
