# No shebang line, this module is meant to be imported
#
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
    from httplib import BAD_REQUEST, OK
except ImportError:  # pragma: no cover
    from http.client import BAD_REQUEST, OK

from os.path import join

from twisted.web.server import NOT_DONE_YET
from twisted.protocols.basic import FileSender

from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.utility import request_from_master


class TaskLogs(APIResource):
    def get(self, **kwargs):
        """
        Get the contents of the specified task log
        """
        request = kwargs["request"]

        if request_from_master(request):
            config.master_contacted()

        if len(request.postpath) != 1:
            request.setResponseCode(BAD_REQUEST)
            request.write(dumps({"error": "Did not specify a log identifier"}))

        log_identifier = request.postpath[0]
        if "/" in log_identifier or "\\" in log_identifier:
            request.setResponseCode(BAD_REQUEST)
            request.write(dumps({"error": "log_identifier must not contain"
                                          "directory separators"}))

        path = join(config["jobtype_task_logs"], log_identifier)
        request.setResponseCode(OK)
        request.setHeader("Content-Type", "text/csv")

        logfile = open(path, "rb")
        deferred = FileSender().beginFileTransfer(logfile, request)
        def transfer_finished(_):
            request.finish()
            logfile.close()
        deferred.addCallback(transfer_finished)

        return NOT_DONE_YET
