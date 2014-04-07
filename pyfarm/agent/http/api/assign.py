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
from voluptuous import Invalid, Schema, Required, Optional, All, Any

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.http.api.base import APIResource


# We're not using pyfarm.core.enums here because
# we need some specific values
try:
    WHOLE_NUMBER_TYPES = (int, long)
    NUMERIC_TYPES = (int, long, float, Decimal)
except NameError:
    WHOLE_NUMBER_TYPES = (int, )
    NUMERIC_TYPES = (int, float, Decimal)


# used to validate individual tasks in Assign.SCHEMAS["POST"]
TASK_SCHEMA = Schema({
    Required("id"): Any(WHOLE_NUMBER_TYPES),
    Required("frame"): Any(NUMERIC_TYPES)})


def validate_environment(values):
    """
    Ensures that ``values`` is a dictionary and that it only
    contains string keys and values.
    """
    if not isinstance(values, dict):
        raise Invalid("Expected a dictionary")

    for key, value in values.items():
        if not isinstance(key, STRING_TYPES):
            raise Invalid("Key %r must be a string" % key)

        if not isinstance(value, STRING_TYPES):
            raise Invalid("Value %r for key %r must be a string" % (key, value))


class Assign(APIResource):
    isLeaf = False  # this is not really a collection of things

    # Schemas used for validating the request before
    # the target function will handle it.  These make
    # assertions about what kind of input data is required
    # or not based on the agent's internal cod.
    SCHEMAS = {
        "POST": Schema({
        Required("job"): Schema({
            Required("id"): Any(WHOLE_NUMBER_TYPES),
            Required("by"): All(Any(NUMERIC_TYPES)),
            Optional("data"): dict,
            Optional("environ"): validate_environment,
            Optional("title"): Any(STRING_TYPES)}),
        Required("jobtype"): {
            Required("name"): Any(STRING_TYPES),
            Required("version"): Any(WHOLE_NUMBER_TYPES)},
        Required("tasks"): lambda values: map(TASK_SCHEMA, values)})}

    def post(self, **kwargs):
        request = kwargs["request"]
        request.setResponseCode(ACCEPTED)
        request.finish()
        return NOT_DONE_YET
