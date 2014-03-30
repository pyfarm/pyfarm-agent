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

"""
API Core
--------

Contains the root resources which build the /api/ endpoints.
"""

from httplib import OK

from twisted.internet.defer import Deferred
from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.http.core.resource import Resource


class APIIndex(Resource):
    isLeaf = True

    def get(self, request):
        def cb(content):
            request.write(content)
            request.setResponseCode(OK)
            request.finish()

        deferred = Deferred()
        deferred.addCallback(cb)
        deferred.callback("Hello world")
        return NOT_DONE_YET


class Versions(Resource):
    isLeaf = False

    def get(self, request):
        request.write("1")
        request.setResponseCode(OK)
        request.finish()
