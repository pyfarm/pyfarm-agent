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
    from httplib import OK, ACCEPTED, NO_CONTENT
except ImportError:  # pragma: no cover
    from http.client import OK, ACCEPTED, NO_CONTENT

from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.web.server import Site, NOT_DONE_YET

from pyfarm.agent.http.api.software import CheckSoftware
from pyfarm.agent.testutil import BaseAPITestCase, random_port
from pyfarm.agent.config import config


class FakeSoftwareVersionAPI(Resource):
    isLeaf = False

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.write(json.dumps({
            "version": "1.0",
            "id": 1,
            "rank": 100,
            "discovery_function_name": "check_example_sw"
            }))
        request.finish()
        return NOT_DONE_YET


class FakeSoftwareVersionCodeAPI(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)
        self.code = ""

    def render_GET(self, request):
        request.setResponseCode(OK)
        request.write(self.code)
        request.finish()
        return NOT_DONE_YET


class AgentAPIRoot(Resource):
    isLeaf = False

    def getChild(self, name, request):
        return Resource.getChild(self, name, request)


class FakeAgentAPI(Resource):
    isLeaf = False

    def getChild(self, name, request):
        return Resource.getChild(self, name, request)


class FakeAgentSoftwareAPI(Resource):
    isLeaf = False

    def __init__(self):
        Resource.__init__(self)
        self.times_posted = 0

    def getChild(self, name, request):
        if request.postpath == []:
            return self
        else:
            return Resource.getChild(self, name, request)

    def render_POST(self, request):
        request.setResponseCode(OK)
        request.write(request.content.read())
        request.finish()
        self.times_posted += 1
        return NOT_DONE_YET


class FakeAgentSoftwareVersionAPI(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)
        self.times_deleted = 0

    def render_DELETE(self, request):
        request.setResponseCode(NO_CONTENT)
        request.finish()
        self.times_deleted += 1
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
        sw_api_root = Resource()
        self.resource.putChild("software", sw_api_root)

        fake_software_api = Resource()
        sw_api_root.putChild("example_sw", fake_software_api)

        fake_version_index_api = Resource()
        fake_software_api.putChild("versions", fake_version_index_api)

        self.fake_version_api = FakeSoftwareVersionAPI()
        fake_version_index_api.putChild("1.0", self.fake_version_api)

        self.fake_discovery_code_api = FakeSoftwareVersionCodeAPI()
        self.fake_version_api.putChild(
            "discovery_code", self.fake_discovery_code_api)

        agent_api_root = AgentAPIRoot()
        self.resource.putChild("agents", agent_api_root)

        fake_agent_api = FakeAgentAPI()
        agent_api_root.putChild(str(config.get("agent_id")), fake_agent_api)

        self.fake_agent_software_api = FakeAgentSoftwareAPI()
        fake_agent_api.putChild("software", self.fake_agent_software_api)

        agent_example_sw_api = Resource()
        self.fake_agent_software_api.putChild("example_sw",
                                              agent_example_sw_api)

        agent_example_sw_version_index_api = Resource()
        agent_example_sw_api.putChild("versions",
                                      agent_example_sw_version_index_api)

        self.agent_example_sw_version_1_0_api = FakeAgentSoftwareVersionAPI()
        agent_example_sw_version_index_api.putChild(
            "1.0",
            self.agent_example_sw_version_1_0_api)

        self.site = Site(self.resource)
        self.server = reactor.listenTCP(random_port(), self.site)
        config["master_api"] = "http://127.0.0.1:%s" % self.server.port

    @inlineCallbacks
    def tearDown(self):
        super(TestCheckSoftware, self).tearDown()
        yield self.server.loseConnection()

    def test_check_software(self):
        self.fake_discovery_code_api.code = ("def check_example_sw():\n"
                                             "    return True\n")
        request = self.post(
            data={
                "software": "example_sw",
                "version": "1.0"
                },
            headers={"User-Agent": config["master_user_agent"]})
        check_software = self.instance_class()
        check_software.testing = True
        result = check_software.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, ACCEPTED)
        check_software.operation_deferred.addCallback(
            lambda _: self.assertEqual(
                self.fake_agent_software_api.times_posted, 1)
        )

        return check_software.operation_deferred

    def test_check_software_not_existing(self):
        self.fake_discovery_code_api.code = ("def check_example_sw():\n"
                                             "    return False\n")
        request = self.post(
            data={
                "software": "example_sw",
                "version": "1.0"
                },
            headers={"User-Agent": config["master_user_agent"]})
        check_software = self.instance_class()
        check_software.testing = True
        result = check_software.render(request)
        self.assertEqual(result, NOT_DONE_YET)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, ACCEPTED)
        check_software.operation_deferred.addCallback(
            lambda _: self.assertEqual(
                self.agent_example_sw_version_1_0_api.times_deleted, 1)
        )

        return check_software.operation_deferred
