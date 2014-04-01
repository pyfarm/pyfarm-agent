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
Base
++++

Contains the base resources used for building up the root
of the agent's api.
"""

from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.utility import dumps


class APIResource(Resource):
    """Base class for all api resources"""
    isLeaf = True

    # TODO: implement more standardized display of json in an html page


class APIRoot(APIResource):
    isLeaf = False


class Versions(APIResource):
    """
    Returns a list of api versions which this agent will support

    .. http:get:: /api/v1/versions/ HTTP/1.1

        **Request**

        .. sourcecode:: http

            GET /api/v1/versions/HTTP/1.1
            Accept: application/json

        **Response**

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Content-Type: application/json

            {
                "versions": [1]
            }

    """
    isLeaf = True

    def get(self, request):
        return dumps(versions=[1])
