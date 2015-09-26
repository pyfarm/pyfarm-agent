# No shebang line, this module is meant to be imported
#
# Copyright 2015 Ambient Entertainment GmbH & Co. KG
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

"""
Software
--------

Contains utilities to check the availability of certain software on the local
machine.
"""

try:
    from httplib import OK, INTERNAL_SERVER_ERROR, NOT_FOUND
except ImportError:  # pragma: no cover
    from http.client import OK, INTERNAL_SERVER_ERROR, NOT_FOUND

import imp, sys

import treq

from twisted.internet import reactor, threads
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from pyfarm.agent.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get_direct, http_retry_delay

logger = getLogger("agent.sysinfo.software")


class VersionNotFound(Exception):
    pass


@inlineCallbacks
def get_software_version_data(software, version):
    """
    Asynchronously fetches the known data about the given software version from
    the master.

    :param str software:
        The name of the software to get data for

    :param str version:
        The name of the version to get data for

    :return:
        Returns information about the given software version from
        the master
    """
    url = "{master_api}/software/{software}/versions/{version}".\
        format(master_api=config.get("master_api"),
               software=software, version=version)

    while True:
        try:
            response = yield get_direct(url)

        except Exception as error:
            delay = http_retry_delay()
            logger.error(
                "Failed to get data about software %s, version %s: %r.  Will "
                "retry in %s seconds.", software, version, error, delay)
            deferred = Deferred()
            reactor.callLater(delay, deferred.callback, None)
            yield deferred

        else:
            if response.code == OK:
                data = yield treq.json_content(response)
                returnValue(data)

            elif response.code >= INTERNAL_SERVER_ERROR:
                delay = http_retry_delay()
                logger.warning(
                    "Could not get data for software %s, version %s, server "
                    "responded with INTERNAL_SERVER_ERROR.  Retrying in %s "
                    "seconds.", software, version, delay)

                deferred = Deferred()
                reactor.callLater(delay, deferred.callback, None)
                yield deferred

            elif response.code == NOT_FOUND:
                logger.error("Got 404 NOT FOUND from server on getting data "
                             "for software %s, version %s", software, version)
                raise VersionNotFound("This software version was not found or "
                                      "has no discovery code.")

            else:
                logger.error(
                    "Failed to get data for software %s, version %s: "
                    "Unexpected status from server %s", software, version,
                    response.code)
                raise Exception("Unknown return code from master: %s" %
                                response.code)


@inlineCallbacks
def get_discovery_code(software, version):
    """
    Asynchronously fetches the discovery code for the given software version
    from the master.

    :param str software:
        The name of the software to get the discovery code for

    :param str version:
        The name of the version to get the discovery code for

    :return:
        Returns the discovery code from the master/
    """
    url = "{master_api}/software/{software}/versions/{version}/discovery_code".\
        format(master_api=config.get("master_api"),
               software=software, version=version)

    while True:
        try:
            response = yield get_direct(url)

        except Exception as error:
            delay = http_retry_delay()
            logger.error(
                "Failed to get discovery code for software %s, version %s: %r. "
                " Will retry in %s seconds.", software, version, error, delay)
            deferred = Deferred()
            reactor.callLater(delay, deferred.callback, None)
            yield deferred

        else:
            data = yield treq.content(response)
            if response.code == OK:
                returnValue(data)

            elif response.code >= INTERNAL_SERVER_ERROR:
                delay = http_retry_delay()
                logger.warning(
                    "Could not get discovery code for software %s, version %s, "
                    "server responded with INTERNAL_SERVER_ERROR.  Retrying in "
                    "%s seconds.", software, version, delay)

                deferred = Deferred()
                reactor.callLater(delay, deferred.callback, None)
                yield deferred

            elif response.code == NOT_FOUND:
                logger.error("Got 404 NOT FOUND from server on getting "
                             "discovery code for software %s, version %s",
                             software, version)
                raise VersionNotFound("This software version was not found or "
                                      "has no discovery code.")

            else:
                logger.error(
                    "Failed to get discovery code for software %s, version %s: "
                    "Unexpected status from server %s", software, version,
                    response.code)
                raise Exception("Unknown return code from master: %s" %
                                response.code)


@inlineCallbacks
def check_software_availability(software, version):
    """
    Asynchronously checks for the availability of a given software in a given
    version.  Will pass True to its callback function if the software could be
    found, False otherwise.
    Works only for software versions that have a discovery registered on the
    master.

    :param str software:
        The name of the software to check for

    :param str version:
        The name of the version to check for
    """
    module_name = \
        "pyfarm.agent.sysinfo.software.{software}_{version}".format(
            software=software, version=version)

    module = sys.modules.get(module_name)
    if module is None:
        module = imp.new_module(module_name)
        sys.modules[module_name] = module

    # Retrieve information about the software/version combination,
    # update the module with the discovery code and then execute
    # the discovery function.
    version_data = yield get_software_version_data(software, version)
    discovery_code = yield get_discovery_code(software, version)
    exec discovery_code in module.__dict__
    result = yield threads.deferToThread(
        getattr(module, version_data["discovery_function_name"]))
    returnValue(result)
