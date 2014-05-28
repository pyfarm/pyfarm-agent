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
Config
++++++

Contains the endpoint for viewing and working with the configuration
on the agent.
"""

# NOTE: This module is named 'config' instead of 'configuration' like
# in the web ui to match the underlying object name and to differentiate
# the names a little more.

from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.config import config
from pyfarm.agent.utility import dumps, request_from_master


# TODO: add POST to modify keys
class Config(APIResource):
    isLeaf = False  # this is not really a collection of things

    def get(self, **kwargs):
        request = kwargs.get("request")

        if request is not None and request_from_master(request):
            config.master_contacted()

        return dumps(dict(config))
