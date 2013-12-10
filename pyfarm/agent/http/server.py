# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
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
HTTP Server
-----------

HTTP server responsible for serving requests that
control or query the running agent.  This file produces
a service that the  :class:`pyfarm.agent.manager.service.ManagerServiceMaker`
class can consume on start.
"""

import time
from datetime import datetime, timedelta
from httplib import FORBIDDEN

from twisted.web.server import Site as _Site, NOT_DONE_YET
from twisted.web.static import File
from twisted.web.error import Error
from twisted.application.internet import TCPServer

from pyfarm.core.enums import AgentState
from pyfarm.core.sysinfo import memory, cpu
from pyfarm.agent.http.resource import Resource


class Site(_Site):
    """
    Site object similar to Twisted's except it also carries along
    some of the internal agent data.
    """
    def __init__(self, resource, config, logPath=None, timeout=60*60*12):
        self.config = config
        _Site.__init__(self, resource, logPath=logPath, timeout=timeout)


# TODO: index documentation
class Index(Resource):
    TEMPLATE = "pyfarm/index.html"

    def get(self, request):
        # TODO: handle other kinds of requests
        template = self.template()

        # write out the results from the template back
        # to the original request
        def cb(content):
            request.write(content)
            request.setResponseCode(200)
            request.finish()

        # convert the state integer to a string
        for key, value in AgentState._asdict().iteritems():
            if self.config["state"] == value:
                state = key.title()
                break
        else:
            raise KeyError("failed to find state")

        # data which will appear in the table
        table_data = [
            ("master", self.config["http-api"]),
            ("state", state),
            ("hostname", self.config["hostname"]),
            ("Agent ID", self.config.get("agent-id", "UNASSIGNED")),
            ("ip", self.config["ip"]),
            ("port", self.config["port"]),
            ("Reported CPUs", self.config["cpus"]),
            ("Reported RAM", self.config["ram"]),
            ("Total CPUs", cpu.NUM_CPUS),
            ("Total RAM", memory.TOTAL_RAM),
            ("Free RAM", memory.ram_free()),
            ("Total Swap", memory.TOTAL_SWAP),
            ("Free Swap", memory.swap_free())]

        # schedule a deferred so the reactor can get control back
        deferred = template.render(table_data=table_data)
        deferred.addCallback(cb)

        return NOT_DONE_YET


class StaticFiles(File):
    """
    More secure version of :class:`.File` that does not list
    directories.  In addition this will also sending along a
    response header asking clients to cache to data.
    """
    EXPIRES = 2630000

    def directoryListing(self):
        """override which ensures directories cannot be listed"""
        raise Error(FORBIDDEN, "directory listing is not allowed")

    def render(self, request):
        """overrides :meth:`.File.render` and sets the expires header"""
        request.setHeader("Cache-Control", "max-age=%s" % self.EXPIRES)
        return File.render(self, request)


def make_http_server(config):
    """
    make an http server and attach the endpoints to the service can use them
    """
    root = Resource(config)

    # TODO: /assign endpoint
    # TODO: /stop/<jobid>/<task> endpoint
    root.putChild(
        "", Index(config))
    root.putChild(
        "favicon.ico", StaticFiles(config["static-files"] + "/favicon.ico"))
    root.putChild(
        "static", StaticFiles(config["static-files"]))

    return TCPServer(config["port"], Site(root, config))

