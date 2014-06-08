# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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

import time
from datetime import timedelta

try:
    from httplib import ACCEPTED, OK
except ImportError:
    from http.client import ACCEPTED, OK

from twisted.web.server import NOT_DONE_YET

from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.sysinfo import memory
from pyfarm.agent.utility import dumps

logger = getLogger("agent.state")


class Stop(APIResource):
    isLeaf = False  # this is not really a collection of things

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]
        agent = config["agent"]
        stopping = agent.stop()

        # TODO: need to wire this up to the real deferred object in stop()
        if data.get("wait"):
            def finished(*_):
                request.finish()
            if stopping is not None:
                stopping.addCallback(request)
            else:
                logger.warning("NOT IMPLEMENTED: wait for stop")
                request.setResponseCode(OK)
                request.finish()
        else:
            request.setResponseCode(ACCEPTED)
            request.finish()

        return NOT_DONE_YET


class Status(APIResource):
    isLeaf = False  # this is not really a collection of things

    def get(self, **kwargs):
        return dumps(
            {"state": config["state"],
             "hostname": config["hostname"],
             "free_ram": int(memory.ram_free()),
             "agent_ram": int(memory.process_memory()),
             "pids": config["pids"],
             "id": config["agent-id"],
             "pidfile": config["pidfile"],
             "uptime": timedelta(
                 seconds=time.time() - config["start"]).total_seconds(),
             "jobs": list(config["jobtypes"].keys())})