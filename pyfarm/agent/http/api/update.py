# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Update Endpoint
===============

This endpoint is used to instruct the agent to download and apply an update.
"""

try:
    from httplib import OK, ACCEPTED
except ImportError:  # pragma: no cover
    from http.client import OK, ACCEPTED

from os import makedirs
from os.path import join, isdir

from treq import get, collect
from twisted.internet import reactor
from twisted.web.server import NOT_DONE_YET
import twisted.python.failure

from voluptuous import Schema, Required

from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.http.core.client import http_retry_delay
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import STRINGS

logger = getLogger("agent.http.update")


class Update(APIResource):
    """
    Requests the agent to download and apply the specified version of itself.
    Will make the agent restart at the next opportunity.

    .. http:post:: /api/v1/update HTTP/1.1

        **Request**

        .. sourcecode:: http

            POST /api/v1/update HTTP/1.1
            Accept: application/json

            {
                "version": 1.2.3
            }

        **Response**

        .. sourcecode:: http

            HTTP/1.1 200 ACCEPTED
            Content-Type: application/json

    """
    SCHEMAS = {
        "POST": Schema({
            Required("version"): STRINGS})}
    isLeaf = False  # this is not really a collection of things

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]
        agent = config["agent"]

        # TODO Check version parameter for sanity

        url = "%s/agents/updates/%s" % (config["master_api"], data["version"])
        url = url.encode()

        def download_update(version):
            config["downloading_update"] = True
            deferred = get(url)
            deferred.addCallback(update_downloaded)
            deferred.addErrback(update_failed)

        def update_failed(arg):
            logger.error("Downloading update failed: %r", arg)
            if isinstance(arg, twisted.python.failure.Failure):
                logger.error("Traceback: %s", arg.getTraceback())
            reactor.callLater(http_retry_delay(),
                              download_update,
                              data["version"])

        def update_downloaded(response):
            if response.code == OK:
                update_filename = join(config["agent_updates_dir"],
                                       "pyfarm-agent.zip")
                logger.debug("Writing update to %s", update_filename)
                if not isdir(config["agent_updates_dir"]):
                    makedirs(config["agent_updates_dir"])
                with open(update_filename, "wb+") as update_file:
                    collect(response, update_file.write)
                logger.info("Update file for version %s has been downloaded "
                            "and stored under %s", data["version"],
                            update_filename)
                # TODO Only shut down if we were started by the supervisor
                config["restart_requested"] = True
                if len(config["current_assignments"]) == 0:
                    agent.stop()
            else:
                logger.error("Unexpected return code %s on downloading update "
                             "from master", response.code)

            config["downloading_update"] = False

        if ("downloading_update" not in config or
            not config["downloading_update"]):
            download_update(data["version"])

        request.setResponseCode(ACCEPTED)
        request.finish()

        return NOT_DONE_YET
