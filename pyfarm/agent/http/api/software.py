# No shebang line, this module is meant to be imported
#
# Copyright 2015 Ambient Entertainment GmbH & Co. KG
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
Check Software Endpoint
=======================

This endpoint is used to instruct the agent to check whether a given version of
a software is installed and usable locally.
"""


try:
    from httplib import OK, ACCEPTED, INTERNAL_SERVER_ERROR, NO_CONTENT
except ImportError:  # pragma: no cover
    from http.client import OK, ACCEPTED, INTERNAL_SERVER_ERROR, NO_CONTENT

import treq

from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks

from voluptuous import Schema, Required

from pyfarm.agent.config import config
from pyfarm.agent.utility import STRINGS
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo.software import check_software_availability
from pyfarm.agent.http.core.client import (
    post_direct, delete_direct, http_retry_delay)
from pyfarm.agent.http.api.base import APIResource

logger = getLogger("agent.http.software")


class CheckSoftware(APIResource):
    """
    Requests the agent to check whether a given software version is installed
    locally.

    The agent will asynchronously update its software information on the master.

    .. http:post:: /api/v1/check_software HTTP/1.1

        **Request**

        .. sourcecode:: http

            POST /api/v1/check_software HTTP/1.1
            Accept: application/json

            {
                "software": "Blender",
                "version": "2.72"
            }

        **Response**

        .. sourcecode:: http

            HTTP/1.1 200 ACCEPTED
            Content-Type: application/json

    """
    SCHEMAS = {
        "POST": Schema({
            Required("software"): STRINGS,
            Required("version"): STRINGS})}
    isLeaf = False  # this is not really a collection of things
    testing = False

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]

        logger.info("Checking whether software %s, version %s is present",
                    data["software"], data["version"])

        if self.testing:
            self.operation_deferred = Deferred()

        @inlineCallbacks
        def mark_software_available(software, version):
            url = "{master_api}/agents/{agent}/software/".format(
                master_api=config.get("master_api"),
                agent=config.get("agent_id"))
            while True:
                try:
                    response = yield post_direct(url, data={
                            "software": software,
                            "version": version})
                except Exception as error:
                    delay = http_retry_delay()
                    logger.error(
                        "Failed to post availability of software %s, "
                        "version %s to master: %r. Will retry in %s "
                        "seconds.",
                        software, version, error, delay)
                    deferred = Deferred()
                    reactor.callLater(delay, deferred.callback, None)
                    yield deferred

                else:
                    data = yield treq.content(response)

                    if response.code == OK:
                        logger.info("Posted availability of software %s, "
                                    "version %s to master.",
                                    software, version)
                        break

                    elif response.code >= INTERNAL_SERVER_ERROR:
                        delay = http_retry_delay()
                        logger.warning(
                            "Could not post availability of software %s, "
                            "version %s. The master responded with "
                            "INTERNAL_SERVER_ERROR.  Retrying in %s "
                            "seconds.", software, version, delay)

                        deferred = Deferred()
                        reactor.callLater(delay, deferred.callback, None)
                        yield deferred

                    else:
                        logger.error(
                            "Failed to post availability of software %s, "
                            "version %s: "
                            "Unexpected status from server %s. Data: %s",
                            software, version, response.code, data)
                        break

            if self.testing:
                self.operation_deferred.callback(None)

        @inlineCallbacks
        def mark_software_not_available(software, version):
            url = ("{master_api}/agents/{agent}/software/{software}/"
                    "versions/{version}").format(
                        master_api=config.get("master_api"),
                        agent=config.get("agent_id"),
                        software=software,
                        version=version)
            while True:
                try:
                    response = yield delete_direct(url)
                except Exception as error:
                    delay = http_retry_delay()
                    logger.error(
                        "Failed to remove software %s, version %s from this "
                        "agent on master: %r. Will retry in %s seconds.",
                        software, version, error, delay)
                    deferred = Deferred()
                    reactor.callLater(delay, deferred.callback, None)
                    yield deferred

                else:
                    data = yield treq.content(response)

                    if response.code in [OK, ACCEPTED, NO_CONTENT]:
                        logger.info("Removed software %s, version %s from this "
                                    "agent on master.", software, version)
                        break

                    elif response.code >= INTERNAL_SERVER_ERROR:
                        delay = http_retry_delay()
                        logger.warning(
                            "Could not remove software %s, version %s from "
                            "this agent. The master responded with "
                            "INTERNAL_SERVER_ERROR.  Retrying in %s "
                            "seconds.", software, version, delay)

                        deferred = Deferred()
                        reactor.callLater(delay, deferred.callback, None)
                        yield deferred

                    else:
                        logger.error(
                            "Failed to remove software %s, version %s from "
                            "this agent: "
                            "Unexpected status from server %s. Data: %s",
                            software, version, response.code, data)
                        break

            if self.testing:
                self.operation_deferred.callback(None)

        def on_check_software_return(result):
            if result is True:
                mark_software_available(data["software"], data["version"])
            else:
                mark_software_not_available(data["software"], data["version"])

        deferred = check_software_availability(data["software"],
                                               data["version"])
        deferred.addCallback(on_check_software_return)
        if self.testing:
            deferred.addErrback(lambda _: self.operation_deferred.errback())

        request.setResponseCode(ACCEPTED)
        request.finish()
        return NOT_DONE_YET
