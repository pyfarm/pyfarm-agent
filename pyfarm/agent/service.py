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
Manager Service
---------------

Sends and receives information from the master and performs systems level tasks
such as log reading, system information gathering, and management of processes.
"""

import json
import time
import logging
from random import random
from datetime import datetime
from functools import partial

import ntplib
import requests
from twisted.internet import reactor
from twisted.application.service import MultiService
from twisted.internet.error import ConnectionRefusedError
from twisted.python import log

from pyfarm.core.enums import AgentState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo import memory
from pyfarm.agent.http.client import post
from pyfarm.agent.tasks import ScheduledTaskManager, memory_utilization
from pyfarm.agent.process.manager import ProcessManager
from pyfarm.agent.config import config

ntplog = getLogger("agent.ntp")
svclog = getLogger("agent.svc")


class ManagerService(MultiService):
    """the service object itself"""
    ntp_client = ntplib.NTPClient()

    def __init__(self):
        self.scheduled_tasks = ScheduledTaskManager()
        self.log = partial(log.msg, system=self.__class__.__name__)
        self.info = partial(self.log, level=logging.INFO)
        self.error = partial(self.log, level=logging.ERROR)
        self.exception = partial(log.err, system=self.__class__.__name__)

        # register any scheduled tasks
        self.scheduled_tasks.register(
            memory_utilization, config["memory-check-interval"],
            func_args=(config, ))

        # finally, setup the base class
        MultiService.__init__(self)

    def _startServiceCallback(self, response):
        """internal callback used to start the service itself"""
        config["manager"] = ProcessManager(config)
        config["agent-id"] = response["id"]
        config["agent-url"] = \
            config["http-api"] + "/agents/%s" % response["id"]
        self.log("agent id is %s, starting service" % config["agent-id"])
        self.scheduled_tasks.start()

    def _failureCallback(self, response):
        """internal callback which is run when the service fails to start"""
        self.exception(response)

    def stopService(self):
        self.scheduled_tasks.stop()

        if "agent-id" not in config:
            self.info(
                "agent id was never set, cannot update master of state change")
            return

        self.info("informing master of agent shutdown")

        try:
            # Send the final update synchronously because the
            # reactor is shutting down.  Sending out this update using
            # the normal methods could very be missed as the reactor is
            # cleaning up.  Attempting to send the request any other way
            # at this point will just cause the deferred object to disappear
            # before being fired.
            response = requests.post(
                config["agent-url"],
                headers={"Content-Type": "application/json"},
                data=json.dumps(
                    {"id": config["agent-id"],
                     "state": AgentState.OFFLINE}))

        except requests.RequestException, e:
            self.exception(e)

        else:
            if response.ok:
                self.info(
                    "agent %s state is now OFFLINE" % config["agent-id"])
            else:
                self.error("ERROR SETTING AGENT STATE TO OFFLINE")
                self.error("      code: %s" % response.status_code)
                self.error("      text: %s" % response.text)


class Agent(object):
    scheduled_tasks = ScheduledTaskManager()
    ntp_client = ntplib.NTPClient()

    def __init__(self):
        self.first_post_attempts = 1

    @classmethod
    def http_retry_delay(cls):
        # TODO: provide command line flags for jitter
        delay = config["http-retry-delay"]
        delay += random()
        return delay

    @classmethod
    def get_system_data(cls):
        """
        Returns a dictionary of data containing information about the
        agent.  This is the information that is also passed along to
        the master.
        """
        ntplog.debug(
            "querying ntp server %r for current time", config["ntp-server"])
        try:
            pool_time = cls.ntp_client.request(config["ntp-server"])

        except Exception, e:
            time_offset_int = 0
            ntplog.warning("failed to determine network time: %s", e)

        else:
            time_offset_int = int(pool_time.tx_time - time.time())

            # format the offset for logging purposes
            utcoffset = datetime.utcfromtimestamp(pool_time.tx_time)
            iso_timestamp = utcoffset.isoformat()
            ntplog.debug(
                "network time: %s (local offset: %r)",
                iso_timestamp, time_offset_int)

            if time_offset_int:
                ntplog.warning(
                    "agent is %r second(s) off from ntp server at %r",
                    time_offset_int, config["ntp-server"])

        data = {
            "hostname": config["hostname"],
            "ip": config["ip"],
            "use_address": config["use-address"],
            "ram": config["ram"],
            "cpus": config["cpus"],
            "port": config["port"],
            "free_ram": memory.ram_free(),
            "time_offset": time_offset_int,
            "state": AgentState.ONLINE}

        if config.get("remote-ip"):
            data.update(remote_ip=config["remote-ip"])

        if config.get("projects"):
           data.update(projects=config["projects"])

        return data

    def run(self):
        system_data = self.get_system_data()
        url = config["master-api"] + "/agents/"

        def callback(response):
            print response

        def errback(failure):
            # TODO: max number of first post attempts may not be the same
            # as --http-max-retries
            if self.first_post_attempts > config["http-max-retries"]:
                svclog.critical(
                    "Reached maximum number of attempts to announce "
                    "the agent's presence to %r", url)
                reactor.stop()

            self.first_post_attempts += 1
            delay = self.http_retry_delay()
            do_post = lambda: post(
                config["master-api"] + "/agents/",
                data=system_data, callback=callback, errback=errback)

            if failure.type is ConnectionRefusedError:
                svclog.warning(
                    "Connection refused to %s, retrying in %s seconds",
                    url, delay)
                reactor.callLater(delay, do_post)
            else:
                svclog.critical("Unhandled exception, stopping reactor")
                svclog.exception(failure)
                reactor.stop()

        # fire off the first POST
        post(
            config["master-api"] + "/agents/",
            data=system_data, callback=callback, errback=errback)

