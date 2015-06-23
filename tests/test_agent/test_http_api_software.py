# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
# Copyright 2015 Ambient Entertainment GmbH & Co. KG
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

import json

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks
from twisted.web.server import Site, NOT_DONE_YET

from pyfarm.agent.http.api.software import CheckSoftware
from pyfarm.agent.testutil import BaseAPITestCase


class SoftwareAPIRoot(Resource):
    isLeaf = False


class FakeSoftwareVersionAPI(Resource):
    isLeaf = False

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.write({
            "version": "1.0",
            "id": 1,
            "rank": 100,
            "discovery_function_name": "check_example_sw"
            })
        request.finish()
        return NOT_DONE_YET


class FakeSoftwareVersionCodeAPI(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.write("def check_example_sw():\n"
                      "    return True\n")
        request.finish()
        return NOT_DONE_YET


class CheckSoftwareFactory(object):
    def __call__(self):
        return CheckSoftware()


class TestCheckSoftware(BaseAPITestCase):
    URI = "/check_software"
    CLASS_FACTORY = CheckSoftwareFactory()
    CLASS = CheckSoftware

    def setUp(self):
        super(TestCheckSoftware, self).setUp()
        self.resource = Resource()
        sw_api_root = SoftwareAPIRoot()
        api_root_resource = self.resource.putChild("api/v1/software",
                                                   sw_api_root)
        self.fake_software_version_api = FakeSoftwareVersionAPI()
        version_resource = api_root_resource.putChild(
            "example_sw/versions/1.0", self.fake_software_version_api)
        self.fake_discovery_code_api = FakeSoftwareVersionCodeAPI()
        version_resource.putChild("discovery_code",
                                  self.fake_discovery_code_api)
        self.site = Site(self.resource)
        self.server = reactor.listenTCP(random_port(), self.site)
        config["master_api"] = "http://127.0.0.1:%s" % self.server.port

    @inlineCallbacks
    def tearDown(self):
        super(TestCheckSoftware(), self).tearDown()
        yield self.server.loseConnection()

    def test_check_software(self):
        request = self.post(
            data={
                "software": "example_sw",
                "version": "1.0"
                },
            headers={"User-Agent": config["master_user_agent"]})
        check = self.instance_class()
        result = check.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, ACCEPTED)