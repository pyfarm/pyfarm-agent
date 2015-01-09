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

from uuid import uuid4

try:
    from httplib import (
        ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE, OK)
except ImportError:  # pragma: no cover
    from http.client import (
        ACCEPTED, BAD_REQUEST, CONFLICT, SERVICE_UNAVAILABLE, OK)

from functools import partial

from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from voluptuous import Schema, Required

from pyfarm.core.enums import WorkState
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import post, http_retry_delay
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import request_from_master
from pyfarm.agent.sysinfo.memory import free_ram
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

    def __init__(self, agent):
        self.agent = agent

    def post(self, **kwargs):
        request = kwargs["request"]
        request_data = kwargs["data"]

        if request_from_master(request):
            config.master_contacted()

        # First, get the resources we have *right now*.  In some cases
        # this means using the functions in pyfarm.core.sysinfo because
        # entries in `config` could be slightly out of sync with the system.
        memory_free = free_ram()
        cpus = config["agent_cpus"]
        requires_ram = request_data["job"].get("ram")
        requires_cpus = request_data["job"].get("cpus")

        if self.agent.shutting_down:
            logger.error("Rejecting assignment because the agent is in the "
                         "process of shutting down.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "Agent cannot accept assignments because it is "
                          "shutting down"})
            request.finish()
            return NOT_DONE_YET

        if "restart_requested" in config \
                and config["restart_requested"] is True:
            logger.error("Rejecting assignment because of scheduled restart.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "Agent cannot accept assignments because of a "
                          "pending restart"})
            request.finish()
            return NOT_DONE_YET

        elif "agent_id" not in config:
            logger.error(
                "Agent has not yet connected to the master or `agent_id` "
                "has not been set yet.")
            request.setResponseCode(SERVICE_UNAVAILABLE)
            request.write(
                {"error": "agent_id has not been set in the config"})
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

        assignment_uuid = uuid4()
        request_data.update(id=assignment_uuid)
        config["current_assignments"][assignment_uuid] = request_data

        # In all other cases we have some work to do inside of
        # deferreds so we just have to respond
        # TODO Mark this agent as running on the master
        request.setResponseCode(ACCEPTED)
        request.write({"id": assignment_uuid})
        request.finish()
        logger.info("Accepted assignment %s: %r", assignment_uuid, request_data)

        def assignment_failed(result, assign_id):
            logger.error(
                "Assignment %s failed, removing.", assign_id)
            logger.error(result.getTraceback())
            assignment = config["current_assignments"].pop(assign_id)
            if "jobtype" in assignment:
                jobtype_id = assignment["jobtype"].pop("id", None)
                if jobtype_id:
                    config["jobtypes"].pop(jobtype_id, None)

        def assignment_started(_, assign_id):
            logger.debug("Assignment %s has started", assign_id)

        def assignment_stopped(_, assign_id):
            logger.debug("Assignment %s has stopped", assign_id)
            assignment = config["current_assignments"].pop(assign_id)
            if "jobtype" in assignment:
                jobtype_id = assignment["jobtype"].pop("id", None)
                if jobtype_id:
                    config["jobtypes"].pop(jobtype_id, None)

        def restart_if_necessary(_):  # pragma: no cover
            if "restart_requested" in config and config["restart_requested"]:
                config["agent"].stop()

        def load_jobtype_failed(result, assign_id):
            logger.error(
                "Loading jobtype for assignment %s failed, removing.", assign_id)
            traceback = result.getTraceback()
            logger.debug("Got traceback")
            logger.error(traceback)
            assignment = config["current_assignments"].pop(assign_id)

            # Mark all tasks as failed on master and set an error message
            logger.debug("Marking tasks in assignment as failed")
            def post_update(post_url, post_data, task, delay=0):
                post_func = partial(post, post_url, data=post_data,
                    callback=lambda x: result_callback(
                        post_url, post_data, task, x),
                    errback=lambda x: error_callback(
                        post_url, post_data, task, x))
                reactor.callLater(delay, post_func)

            def result_callback(cburl, cbdata, task, response):
                if 500 <= response.code < 600:
                    logger.error(
                        "Error while marking task %s as failed on master, "
                        "retrying", task["id"])
                    post_update(cburl, cbdata, task, delay=http_retry_delay())

                elif response.code != OK:
                    logger.error(
                        "Could not mark task %s as failed, server response "
                        "code was %s", task["id"], response.code)

                else:
                    logger.info(
                        "Marked task %s as failed on master", task["id"])

            def error_callback(cburl, cbdata, task, failure_reason):
                logger.error(
                    "Error while marking task %s as failed, retrying",
                    task["id"], failure_reason)
                post_update(cburl, cbdata, task, delay=http_retry_delay())

            for task in assignment["tasks"]:
                url = "%s/jobs/%s/tasks/%s" % (
                    config["master_api"], assignment["job"]["id"], task["id"])
                data = {
                    "state": WorkState.FAILED,
                    "last_error": traceback}
                post_update(url, data, task)

            # If the loading was partially successful for some reason, there
            # might already be an entry for this jobtype in the config.
            # Remove it if it exists.
            if "jobtype" in assignment:
                jobtype_id = assignment["jobtype"].pop("id", None)
                if jobtype_id:
                    config["jobtypes"].pop(jobtype_id, None)

        def loaded_jobtype(jobtype_class, assign_id):
            # TODO: report error to master
            if hasattr(jobtype_class, "getTraceback"):
                logger.error(jobtype_class.getTraceback())
                return

            # TODO: add call to prepare_for_job
            # TODO: add call to spawn_persistent_process

            # Instance the job type and pass in the assignment data.
            instance = jobtype_class(request_data)

            if not isinstance(instance, JobType):
                raise TypeError(
                    "Expected a subclass of "
                    "pyfarm.jobtypes.core.jobtype.JobType")

            # TODO: add callback to cleanup_after_job
            # TODO: add callback to stop persistent process
            try:
                started_deferred, stopped_deferred = instance._start()
                started_deferred.addCallback(assignment_started, assign_id)
                started_deferred.addErrback(assignment_failed, assign_id)
                stopped_deferred.addCallback(assignment_stopped, assign_id)
                stopped_deferred.addErrback(assignment_failed, assign_id)
                stopped_deferred.addBoth(restart_if_necessary)
                stopped_deferred.addBoth(
                    lambda *args: instance._remove_tempdirs())
            except Exception as e:
                logger.error("Error on starting jobtype, stopping it now.  "
                             "Error was: %s", e)
                instance.stop(assignment_failed=True,
                              error="Error while loading jobtype: %s" % e)
                assignment = config["current_assignments"].pop(assign_id)
                if "jobtype" in assignment:
                    jobtype_id = assignment["jobtype"].pop("id", None)
                    if jobtype_id:
                        config["jobtypes"].pop(jobtype_id, None)

        # Load the job type then pass the class along to the
        # callback.  No errback here because all the errors
        # are handled internally in this case.
        jobtype_loader = JobType.load(request_data)
        jobtype_loader.addCallback(loaded_jobtype, assignment_uuid)
        jobtype_loader.addErrback(load_jobtype_failed, assignment_uuid)

        return NOT_DONE_YET
