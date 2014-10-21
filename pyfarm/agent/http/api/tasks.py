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

from json import dumps

try:
    from httplib import (
        BAD_REQUEST, NO_CONTENT, INTERNAL_SERVER_ERROR, NOT_FOUND, ACCEPTED)
except ImportError:  # pragma: no cover
    from http.client import (
        BAD_REQUEST, NO_CONTENT, INTERNAL_SERVER_ERROR, NOT_FOUND, ACCEPTED)

from twisted.web.server import NOT_DONE_YET

from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.utility import request_from_master
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.http.api.tasks")


class Tasks(APIResource):
    def get(self, **kwargs):
        request = kwargs["request"]

        if request_from_master(request):
            config.master_contacted()

        try:
            current_assignments = config["current_assignments"].itervalues
        except AttributeError:  # pragma: no cover
            current_assignments = config["current_assignments"].values

        tasks = []
        for assignment in current_assignments():
            tasks += assignment["tasks"]

        return dumps(tasks)

    def delete(self, **kwargs):
        """
        HTTP endpoint for stopping and deleting an individual task from this
        agent.
        ... warning::
        If the specified task is part of a multi-task assignment, all
        tasks in this assignment will be stopped, not just the specified one.

        This will try to asynchronously stop the assignment by killing all its
        child processes. If that isn't successful, this will have no effect.
        """
        request = kwargs["request"]

        # Postpath must be exactly one element, and that needs to be an integer
        if len(request.postpath) != 1:
            request.setResponseCode(BAD_REQUEST)
            request.write({"error": "Did not specify a task id"})
            return NOT_DONE_YET

        try:
            task_id = int(request.postpath[0])
        except ValueError:
            request.setResponseCode(BAD_REQUEST)
            request.write({"error": "Task id was not an integer"})
            return NOT_DONE_YET

        try:
            current_assignments = config["current_assignments"].itervalues
        except AttributeError:  # pragma: no cover
            current_assignments = config["current_assignments"].values

        assignment = None
        for item in current_assignments():
            for task in item["tasks"]:
                if task["id"] == task_id:
                    assignment = item

        if not assignment:
            logger.info("Cannot cancel task %s: not found", task_id)
            request.setResponseCode(NO_CONTENT)
            request.finish()
            return NOT_DONE_YET

        if "jobtype" in assignment and "id" in assignment["jobtype"]:
            jobtype = config["jobtypes"][assignment["jobtype"]["id"]]
            logger.info("Stopping assignment %s", assignment["id"])
            jobtype.stop()
        else:
            logger.error("Tried stopping assigment %s, but found no jobtype "
                         "instance", assignment.uuid)
            request.setResponseCode(INTERNAL_SERVER_ERROR)
            request.write({"error": "Assignment found, but no jobtype instance "
                                    "exists."})
            request.finish()
            return NOT_DONE_YET

        request.setResponseCode(ACCEPTED)
        request.finish()

        return NOT_DONE_YET
