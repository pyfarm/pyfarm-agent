# No shebang line, this module is meant to be imported
#
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
# Copyright 2015 Oliver Palmer
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
from errno import ENOENT

try:
    from httplib import BAD_REQUEST, OK, NOT_FOUND, INTERNAL_SERVER_ERROR
except ImportError:  # pragma: no cover
    from http.client import BAD_REQUEST, OK, NOT_FOUND, INTERNAL_SERVER_ERROR

from os.path import join

from twisted.web.server import NOT_DONE_YET
from twisted.protocols.basic import FileSender

from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.utility import request_from_master


class TaskLogs(APIResource):
    def _open_file(self, path, mode="rb"):
        """
        Small wrapper around the built-in :func:`open`.  We mainly
        use this for patching behavior in tests but could also use
        this for handling additional modes/exception/etc
        """
        return open(path, mode=mode)

    def get(self, **kwargs):
        """
        Get the contents of the specified task log.  The log will be
        returned in CSV format with the following fields:

            * ISO8601 timestamp
            * Stream number (0 == stdout, 1 == stderr)
            * Line number
            * Parent process ID
            * Message the process produced

        The log file identifier is configurable and relies on the
        `jobtype_task_log_filename` configuration option.  See the
        configuration documentation for more information about the
        default value.

        .. http:get:: /api/v1/tasklogs/<identifier> HTTP/1.1

            **Request**

            .. sourcecode:: http

                GET /api/v1/tasklogs/<identifier> HTTP/1.1

            **Response**

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Content-Type: text/csv

                2015-05-07T23:42:53.730975,0,15,42,Hello world

        :statuscode 200: The log file was found, it's content will be returned.
        """
        request = kwargs["request"]

        if request_from_master(request):
            config.master_contacted()

        if len(request.postpath) != 1:
            request.setResponseCode(BAD_REQUEST)
            request.setHeader("Content-Type", "application/json")
            request.write(dumps({"error": "Did not specify a log identifier"}))
            return NOT_DONE_YET

        log_identifier = request.postpath[0]
        if "/" in log_identifier or "\\" in log_identifier:
            request.setResponseCode(BAD_REQUEST)
            request.setHeader("Content-Type", "application/json")
            request.write(dumps({"error": "log_identifier must not contain "
                                          "directory separators"}))

            return NOT_DONE_YET

        path = join(config["jobtype_task_logs"], log_identifier)

        try:
            logfile = self._open_file(path, "rb")
        except Exception as error:
            request.setHeader("Content-Type", "application/json")

            if getattr(error, "errno", None) == ENOENT:
                request.setResponseCode(NOT_FOUND)
                request.write(dumps({"error": "%s does not exist" % path}))
                return NOT_DONE_YET

            logger.error("GET %s failed: %s", request.postpath, error)
            request.setResponseCode(INTERNAL_SERVER_ERROR)
            request.write(dumps({"error": str(error)}))
            return NOT_DONE_YET

        logfile = open(path, "rb")
        deferred = FileSender().beginFileTransfer(logfile, request)
        def transfer_finished(_):
            request.finish()
            logfile.close()
        deferred.addCallback(transfer_finished)

        return NOT_DONE_YET
