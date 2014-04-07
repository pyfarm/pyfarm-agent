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

from decimal import Decimal
from httplib import ACCEPTED

from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Required, Optional, All, Range, Any

from pyfarm.agent.http.api.base import APIResource


class Assign(APIResource):
    isLeaf = False  # this is not really a collection of things

    SCHEMAS = {
        "POST": Schema({
        Required("job"): Schema({
            Required("id"): Any(int, long),
            Required("by"): All(Any(int, long), Range(min=1)),
            Optional("data"): dict,
            Optional("environ"): dict,
            Optional("title"): Any(str, unicode)}),
        Required("jobtype"): {
            Required("name"): Any(str, unicode),
            Required("version"): Any(int, long)},
        Required("tasks"): lambda values: map(Schema({
            Required("id"): Any(int, long),
            Required("frame"): Any(int, float, long, Decimal)}), values)})}

    def post(self, **kwargs):
        request = kwargs["request"]
        request.setResponseCode(ACCEPTED)
        request.finish()
        return NOT_DONE_YET
