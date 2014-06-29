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

from os import urandom
from os.path import isdir
from textwrap import dedent

try:
    from httplib import CREATED
except ImportError:  # pragma: no cover
    from http.client import CREATED


from twisted.internet.defer import Deferred

from pyfarm.core.enums import LINUX, MAC, WINDOWS
from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, skipIf
from pyfarm.agent.http.core.client import post, get
from pyfarm.jobtypes.core.internals import Cache, pwd, grp


class TestImports(TestCase):
    @skipIf(not WINDOWS, "Not Windows")
    def test_windows(self):
        self.assertIs(pwd, NotImplemented)
        self.assertIs(grp, NotImplemented)

    @skipIf(not any([LINUX, MAC]), "Not Linux/Mac")
    def test_posix(self):
        self.assertIsNot(pwd, NotImplemented)
        self.assertIsNot(grp, NotImplemented)


class TestCache(TestCase):
    @skipIf(Cache.CACHE_DIRECTORY is None, "Cache.CACHE_DIRECTORY not set")
    def test_cache_directory(self):
        self.assertTrue(isdir(Cache.CACHE_DIRECTORY))

    def test_download_jobtype(self):
        classname = "Test%s" % urandom(8).encode("hex")
        sourcecode = dedent("""
        from pyfarm.jobtypes.core.jobtype import JobType
        class %s(JobType):
            pass""" % classname)

        cache = Cache()
        finished = Deferred()

        def post_success(response):
            if response.code == CREATED:
                data = response.json()
                download = cache._download_jobtype(
                    data["name"], data["version"])
                download.addCallback(finished.callback)
                download.addErrback(finished.errback)

            else:
                finished.errback(response.json())

        post(config["master-api"] + "/jobtypes/",
             callback=post_success, errback=finished.errback,
             data={"name": classname,
                   "classname": classname,
                   "code": sourcecode})
        return finished