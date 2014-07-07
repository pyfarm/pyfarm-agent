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

try:
    from httplib import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE

from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Required

from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import request_from_master, uuid
from pyfarm.agent.sysinfo.memory import ram_free
from pyfarm.agent.utility import JOBTYPE_SCHEMA, TASKS_SCHEMA, JOB_SCHEMA
from pyfarm.jobtypes.core.jobtype import JobType

logger = getLogger("agent.http.assign")


class Assign(APIResource):
    isLeaf = False  # this is not really a collection of things

    # Schemas used for validating the request before
    # the target function will handle it.  These make
    # assertions about what kind of input data is required
    # or not based on the agent's internal code.
    SCHEMAS = {
        "POST": Schema({
            Required("job"): JOB_SCHEMA,
            Required("jobtype"): JOBTYPE_SCHEMA,
            Required("tasks"): TASKS_SCHEMA})}

    def post(self, **kwargs):
        request = kwargs["request"]
        request_data = kwargs["data"]

        if request_from_master(request):
            config.master_contacted()

        # First, get the resources we have *right now*.  In some cases
        # this means using the functions in pyfarm.core.sysinfo because
        # entries in `config` could be slightly out of sync with the system.
        memory_free = int(ram_free())
        cpus = config["cpus"]
        requires_ram = request_data["job"].get("ram")
        requires_cpus = request_data["job"].get("cpus")

        if "restart_requested" in config \
                and config["restart_requested"] is True:
            logger.error("Rejecting assignment because of scheduled restart.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "Agent cannot accept assignments because of a "
                          "pending restart"})
            request.finish()
            return NOT_DONE_YET

        elif "agent-id" not in config:
            logger.error(
                "Agent has not yet connected to the master or `agent-id` "
                "has not been set yet.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "agent-id has not been set in the config"})
            request.finish()
            return NOT_DONE_YET

        # Do we have enough ram?
        elif requires_ram is not None and requires_ram > memory_free:
            logger.error(
                "Task %s requires %sMB of ram, this agent has %sMB free.  "
                "Rejecting Task %s.",
                request_data["job"]["id"], requires_ram, memory_free,
                request_data["job"]["id"])
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
                request_data["job"]["id"], requires_cpus, cpus,
                request_data["job"]["id"])
            request.setResponseCode(BAD_REQUEST)
            request.write(
                {"error": "Not enough cpus",
                 "agent_cpus": cpus,
                 "requires_cpus": requires_cpus})
            request.finish()
            return NOT_DONE_YET

        # Check for double assignments
        try:
            current_assignments = config["current_assignments"].itervalues
        except AttributeError:  # pragma: no cover
            current_assignments = config["current_assignments"].values

        existing_task_ids = set()
        new_task_ids = set(task["id"] for task in request_data["tasks"])

        for assignment in current_assignments():
            for task in assignment["tasks"]:
                existing_task_ids.add(task["id"])

        if existing_task_ids & new_task_ids:
            request.setResponseCode(CONFLICT)
            request.write(
                {"error": "Double assignment of tasks",
                 "duplicate_tasks": list(existing_task_ids & new_task_ids)})
            request.finish()
            return NOT_DONE_YET

        # Seems inefficient, but the assignments dict is unlikely to be large
        assignment_uuid = uuid()
        request_data.update(id=assignment_uuid)
        config["current_assignments"][assignment_uuid] = request_data

        # In all other cases we have some work to do inside of
        # deferreds so we just have to respond
        # TODO Mark this agent as running on the master
        request.setResponseCode(ACCEPTED)
        request.write({"id": assignment_uuid})
        request.finish()
        logger.info("Accepted assignment %s: %r", assignment_uuid, request_data)

        def assignment_started(result, assign_id):
            # TODO: report error to master
            if hasattr(result, "getTraceback"):
                logger.error(result.getTraceback())
                logger.error(
                    "Assignment %s failed to start, removing.", assign_id)
                config["current_assignments"].pop(assign_id)
            else:
                logger.debug("Assignment %s has started", assign_id)

        def assignment_stopped(result, assign_id):
            # TODO: report error to master
            if hasattr(result, "getTraceback"):
                logger.error(result.getTraceback())
                logger.error("Assignment %s failed, removing.", assign_id)
            else:
                logger.debug("Assignment %s has stopped", assign_id)

            config["current_assignments"].pop(assign_id)

        def restart_if_necessary(_):  # pragma: no cover
            if "restart_requested" in config and config["restart_requested"]:
                config["agent"].stop()

        def loaded_jobtype(jobtype_class, assign_id):
            # TODO: report error to master
            if hasattr(jobtype_class, "getTraceback"):
                logger.error(jobtype_class.getTraceback())
                return

            # Instance the job type and pass in the assignment
            # data.
            instance = jobtype_class(request_data)

            if not isinstance(instance, JobType):
                raise TypeError(
                    "Expected a subclass of "
                    "pyfarm.jobtypes.core.jobtype.JobType")

            start_deferred = instance._start()
            start_deferred.addBoth(assignment_started, assign_id)
            start_deferred.addBoth(restart_if_necessary)
            stopped_deferred = instance._stop()
            stopped_deferred.addBoth(assignment_stopped, assign_id)
            stopped_deferred.addBoth(restart_if_necessary)

        # Load the job type then pass the class along to the
        # callback.  No errback here because all the errors
        # are handled internally in this case.
        jobtype_loader = JobType.load(request_data)
        jobtype_loader.addCallback(loaded_jobtype, assignment_uuid)
        jobtype_loader.addErrback(assignment_stopped, assignment_uuid)

        return NOT_DONE_YET
