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
import os
import atexit
from datetime import timedelta, datetime
from errno import ENOENT, EEXIST
from os.path import isfile, dirname

try:
    from httplib import ACCEPTED, OK, BAD_REQUEST
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, OK, BAD_REQUEST
try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

import psutil
from twisted.internet.defer import Deferred
from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Optional

from pyfarm.agent.config import config
from pyfarm.agent.http.api.base import APIResource
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import memory
from pyfarm.agent.utility import dumps, total_seconds

logger = getLogger("agent.http.state")


class Stop(APIResource):
    isLeaf = False  # this is not really a collection of things
    SCHEMAS = {
        "POST": Schema({
            Optional("wait"): bool})}

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]
        agent = config["agent"]

        try:
            os.remove(config["run_control_file"])
        except (WindowsError, OSError, IOError) as e:
            if e.errno != ENOENT:
                logger.error("Could not delete run control file %s: %s: %s",
                             config["run_control_file"],
                             type(e).__name__, e)

                def remove_run_control_file():
                    try:
                        os.remove(config["run_control_file"])
                        logger.debug("Removed run control file %r",
                                     config["run_control_file"])
                    except (OSError, IOError, WindowsError) as e:
                        logger.error("Failed to remove run control file %s: "
                                     "%s: %s",
                                    config["run_control_file"],
                                    type(e).__name__, e)

                atexit.register(remove_run_control_file)

        stopping = agent.stop()

        # TODO: need to wire this up to the real deferred object in stop()
        if data.get("wait"):
            def finished(_, finish_request):
                finish_request.setResponseCode(OK)
                finish_request.finish()

            if isinstance(stopping, Deferred):
                stopping.addCallback(finished, request)
            else:  # pragma: no cover
                request.setResponseCode(OK)
                request.finish()

        else:
            request.setResponseCode(ACCEPTED)
            request.finish()

        return NOT_DONE_YET


class Restart(APIResource):
    isLeaf = False

    def post(self, **kwargs):
        request = kwargs["request"]
        data = kwargs["data"]
        agent = config["agent"]

        # Ensure the run control file exists
        if not isfile(config["run_control_file"]):
            directory = dirname(config["run_control_file"])
            try:
                os.makedirs(directory)
            except (OSError, IOError) as e:  # pragma: no cover
                if e.errno != EEXIST:
                    logger.error(
                        "Failed to create parent directory for %s: %s: %s",
                        config["run_control_file"], type(e).__name__, e)
                    raise
            else:
                logger.debug("Created directory %s", directory)
            try:
                with open(config["run_control_file"], "a"):
                    pass
            except (OSError, IOError) as e:
                logger.error("Failed to create run control file %s: %s: %s",
                             config["run_control_file"], type(e).__name__, e)
            else:
                logger.info("Created run control file %s",
                            config["run_control_file"])

        if not config["current_assignments"] or data.get("immediately", False):
            logger.info("The agent will restart immediately.")
            agent.stop()
        else:
            logger.info("The agent will restart after the current assignment "
                        "is finished.")
            config["restart_requested"] = True

        request.setResponseCode(ACCEPTED)
        request.finish()

        return NOT_DONE_YET


class Status(APIResource):
    isLeaf = False  # this is not really a collection of things

    def get(self, **_):
        # Get counts for child processes and grandchild processes
        process = psutil.Process()
        direct_child_processes = len(process.children(recursive=False))
        all_child_processes = len(process.children(recursive=True))
        grandchild_processes = all_child_processes - direct_child_processes

        # Determine the last time we talked to the master (if ever)
        contacted = config.master_contacted(update=False)
        if isinstance(contacted, datetime):  # pragma: no cover
            contacted = datetime.utcnow() - contacted

        # Determine the last time we announced ourselves to the
        # master (if ever)
        last_announce = config.get("last_announce", None)
        if isinstance(last_announce, datetime):  # pragma: no cover
            last_announce = datetime.utcnow() - last_announce

        return dumps(
            {"state": config["state"],
             "agent_hostname": config["agent_hostname"],
             "free_ram": memory.free_ram(),
             "agent_process_ram": memory.process_memory(),
             "consumed_ram": memory.total_consumption(),
             "child_processes": direct_child_processes,
             "grandchild_processes": grandchild_processes,
             "pids": config["pids"],
             "agent_id": config["agent_id"],
             "last_master_contact": contacted,
             "last_announce": last_announce,
             "agent_lock_file": config["agent_lock_file"],
             "uptime": total_seconds(
                 timedelta(seconds=time.time() - config["start"])),
             "jobs": list(config["jobtypes"].keys())})
