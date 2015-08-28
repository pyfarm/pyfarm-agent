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
        """
        Returns all tasks which are currently being
        processed locally by the agent.

        .. http:get:: /api/v1/tasks/ HTTP/1.1

            **Request**

            .. sourcecode:: http

                GET /api/v1/tasks/ HTTP/1.1

            **Response**

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Content-Type: application/json

                [{
                    "id": "732c1ef0-9488-4914-adef-c29f481f3f5b",
                    "frame": 1,
                    "attempt": 1
                },
                {
                    "id": "34ce3964-b654-4ad4-8416-f5ddba67806e",
                    "frame": 2,
                    "attempt": 1
                }]

        .. note::

            The above output is an example only and may not exactly
            replicate the fields present in the response.

        :statuscode 200: The request was processed successfully
        """
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

        .. warning::
            If the specified task is part of a multi-task assignment, all
            tasks in this assignment will be stopped, not just the
            specified one.

        This will try to asynchronously stop the assignment by killing all its
        child processes. If that isn't successful, this will have no effect.

        .. http:delete:: /api/v1/tasks/<int:task_id> HTTP/1.1

            **Request**

            .. sourcecode:: http

                DELETE /api/v1/tasks/1 HTTP/1.1

            **Response**

            .. sourcecode:: http

                HTTP/1.1 202 ACCEPTED
                Content-Type: application/json


        :statuscode 202: The task was found and will be stopped.
        :statuscode 204: Nothing to do, no task matching the request was found
        :statuscode 400: There was a problem with the request, check the error
        """
        request = kwargs["request"]

        # Postpath must be exactly one element, and that needs to be an integer
        if len(request.postpath) != 1:
            return dumps({"error": "Did not specify a task id"}), BAD_REQUEST

        try:
            task_id = int(request.postpath[0])
        except ValueError:
            return dumps({"error": "Task id was not an integer"}), BAD_REQUEST

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
            return "", NO_CONTENT

        if "jobtype" in assignment and "id" in assignment["jobtype"]:
            jobtype = config["jobtypes"][assignment["jobtype"]["id"]]
            logger.info("Stopping assignment %s", assignment["id"])
            jobtype.stop()
        else:
            logger.error("Tried stopping assigment %s, but found no jobtype "
                         "instance", assignment["id"])
            return dumps(
                {"error": "Assignment found, but no jobtype instance "
                          "exists."}), INTERNAL_SERVER_ERROR

        return "", ACCEPTED
