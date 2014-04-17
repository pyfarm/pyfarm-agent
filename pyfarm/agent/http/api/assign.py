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

try:
    from httplib import ACCEPTED, BAD_REQUEST
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, BAD_REQUEST

from twisted.web.server import NOT_DONE_YET
from voluptuous import Invalid, Schema, Required, Optional, Any

from pyfarm.core.enums import STRING_TYPES
from pyfarm.core.sysinfo.memory import ram_free
from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.jobtypes.core.jobtype import JobType

logger = getLogger("agent.assign")

# Values used by the schema to do type testing
# of input requests
STRINGS = Any(*STRING_TYPES)
try:
    WHOLE_NUMBERS = Any(*(int, long))
    NUMBERS = Any(*(int, long, float, Decimal))
except NameError:  # pragma: no cover
    WHOLE_NUMBERS = int
    NUMBERS = Any(*(int, float, Decimal))


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


class HandlePost(object):
    def __init__(self, data):
        self.data = data


class Assign(APIResource):
    isLeaf = False  # this is not really a collection of things

    # Schemas used for validating the request before
    # the target function will handle it.  These make
    # assertions about what kind of input data is required
    # or not based on the agent's internal cod.
    SCHEMAS = {
        "POST": Schema({
            Required("job"): Schema({
                Required("id"): WHOLE_NUMBERS,
                Required("by"): NUMBERS,
                Optional("batch"): WHOLE_NUMBERS,
                Optional("user"): STRINGS,
                Optional("ram"): WHOLE_NUMBERS,
                Optional("ram_warning"): WHOLE_NUMBERS,
                Optional("ram_max"): WHOLE_NUMBERS,
                Optional("cpus"): WHOLE_NUMBERS,
                Optional("data"): dict,
                Optional("environ"): validate_environment,
                Optional("title"): STRINGS}),
            Required("jobtype"): {
                Required("name"): STRINGS,
                Required("version"): WHOLE_NUMBERS},
            Required("tasks"): lambda values: map(
                Schema({
                    Required("id"): WHOLE_NUMBERS,
                    Required("frame"): NUMBERS}), values)})}

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]

        # First, get the resources we have *right now*.  In some cases
        # this means using the functions in pyfarm.core.sysinfo because
        # entries in `config` could be slightly out of sync with the system.
        memory_free = int(ram_free())
        cpus = config["cpus"]
        requires_ram = data["job"].get("ram")
        requires_cpus = data["job"].get("cpus")

        # Do we have enough ram?
        if requires_ram is not None and requires_ram > memory_free:
            logger.error(
                "Task %s requires %sMB of ram, this agent has %sMB free.  "
                "Rejecting Task %s.",
                data["job"]["id"], requires_ram, memory_free, data["job"]["id"])
            request.write(
                {"error": "Not enough ram",
                 "agent_ram": memory_free,
                 "requires_ram": requires_ram})
            request.setResponseCode(BAD_REQUEST)
            request.finish()

            # touch the config
            config["free_ram"] = memory_free

        # Do we have enough cpus (count wise)?
        elif requires_cpus is not None and requires_cpus > cpus:
            logger.error(
                "Task %s requires %s CPUs, this agent has %s CPUs.  "
                "Rejecting Task %s.",
                data["job"]["id"], requires_cpus, cpus, data["job"]["id"])
            request.write(
                {"error": "Not enough cpus",
                 "agent_cpus": cpus,
                 "requires_cpus": requires_cpus})
            request.setResponseCode(BAD_REQUEST)
            request.finish()

        # In all other cases we have some work to do inside of
        # deferreds so we just have to respond
        else:
            request.setResponseCode(ACCEPTED)
            request.finish()

        # Retrieve the job tpe
        # TODO: attach to a function to instance the class
        # and get the process started
        jobtype = JobType.load(data)
        jobtype.addCallback(logger.info)

        # TODO: Next steps as deferreds.  Some of these are done in the queue,
        #       or will be, but the information the agent has available may be
        #       more up to date and these checks are fast anyway.
        #   - ensure we have enough ram left to serve the optional requirements
        #   - check for number of cpus currently required by other jobs + this
        #     job to see if we've hit a limit
        #   - get job type (and cache if not already)
        #   - instance the job type with env, user, resource requirements, etc
        #   - execute the job type
        #   - (done) - workload handed off to job type including
        #     ram_max/ram_warning check, user -> uid conversion (or warning, on
        #     windows), etc

        return NOT_DONE_YET
