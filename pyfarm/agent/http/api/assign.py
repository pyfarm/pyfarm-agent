# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
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
    from httplib import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE

from twisted.web.server import NOT_DONE_YET
from voluptuous import Invalid, Schema, Required, Optional, Any

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import request_from_master
from pyfarm.agent.sysinfo.memory import ram_free
from pyfarm.jobtypes.core.jobtype import JobType

logger = getLogger("agent.http.assign")

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
    # or not based on the agent's internal code.
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

        if request_from_master(request):
            config.master_contacted()

        # First, get the resources we have *right now*.  In some cases
        # this means using the functions in pyfarm.core.sysinfo because
        # entries in `config` could be slightly out of sync with the system.
        memory_free = int(ram_free())
        cpus = config["cpus"]
        requires_ram = data["job"].get("ram")
        requires_cpus = data["job"].get("cpus")

        if "restart_requested" in config and config["restart_requested"] is True:
            logger.error("Rejecting assignment because of scheduled restart.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "Agent cannot accept assignments because of a "
                          "pending restart"})
            request.finish()
            return NOT_DONE_YET

        if "agent-id" not in config:
            logger.error(
                "Agent has not yet connected to the master or `agent-id` "
                "has not been set yet.")
            request.setResponseCode(BAD_REQUEST)
            request.write(
                {"error": "agent-id has not been set in the config"})
            request.finish()
            return NOT_DONE_YET

        # Do we have enough ram?
        elif requires_ram is not None and requires_ram > memory_free:
            logger.error(
                "Task %s requires %sMB of ram, this agent has %sMB free.  "
                "Rejecting Task %s.",
                data["job"]["id"], requires_ram, memory_free, data["job"]["id"])
            request.setResponseCode(BAD_REQUEST)
            request.write(
                {"error": "Not enough ram",
                 "agent_ram": memory_free,
                 "requires_ram": requires_ram})
            request.finish()

            # touch the config
            config["free_ram"] = memory_free
            return NOT_DONE_YET

        # Do we have enough cpus (count wise)?
        elif requires_cpus is not None and requires_cpus > cpus:
            logger.error(
                "Task %s requires %s CPUs, this agent has %s CPUs.  "
                "Rejecting Task %s.",
                data["job"]["id"], requires_cpus, cpus, data["job"]["id"])
            request.setResponseCode(BAD_REQUEST)
            request.write(
                {"error": "Not enough cpus",
                 "agent_cpus": cpus,
                 "requires_cpus": requires_cpus})
            request.finish()
            return NOT_DONE_YET

        # Check for double assignments
        existing_task_ids = set()
        for assignment in config["current_assignments"].itervalues():
            for task in assignment["tasks"]:
                existing_task_ids.add(task["id"])
        new_task_ids = set()
        for task in data["tasks"]:
            new_task_ids.add(task["id"])
        if existing_task_ids & new_task_ids:
            request.setResponseCode(CONFLICT)
            request.write(
                {"error": "Double assignment of tasks",
                 "duplicate_tasks": list(existing_task_ids & new_task_ids)})
            request.finish()
            return NOT_DONE_YET

        # Seems inefficient, but the assignments dict is unlikely to be large
        index = 0
        while index in config["current_assignments"]:
            index += 1
        config["current_assignments"][index] = data

        # In all other cases we have some work to do inside of
        # deferreds so we just have to respond
        # TODO Mark this agent as running on the master
        request.setResponseCode(ACCEPTED)
        request.finish()

        def remove_assignment(index):
            del config["current_assignments"][index]

        def restart_if_necessary():
            if "restart_requested" in config and config["restart_requested"]:
                config["agent"].stop()

        def loaded_jobtype(jobtype_class):
            instance = jobtype_class(data)

            if not isinstance(instance, JobType):
                raise TypeError(
                    "Expected a subclass of "
                    "pyfarm.jobtypes.core.jobtype.JobType")

            deferred = instance._start()
            deferred.addCallback(lambda _: remove_assignment(index))
            deferred.addErrback(lambda _: remove_assignment(index))
            deferred.addCallback(lambda _: restart_if_necessary())
            deferred.addErrback(lambda _: restart_if_necessary())

        # Load the job type then pass the class along to the
        # callback.  No errback here because all the errors
        # are handled internally in this case.
        jobtype_loader = JobType.load(data)
        jobtype_loader.addCallback(loaded_jobtype)

        return NOT_DONE_YET
