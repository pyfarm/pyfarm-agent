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
from datetime import datetime
from functools import partial

import ntplib
import requests
from twisted.application.service import MultiService

from pyfarm.core.enums import AgentState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo import memory
from pyfarm.agent.http.client import post as http_post
from pyfarm.agent.utility.retry import RetryDeferred
from pyfarm.agent.tasks import ScheduledTaskManager, memory_utilization
from pyfarm.agent.process.manager import ProcessManager
from pyfarm.agent.config import config

ntplog = getLogger("agent.ntp")


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
        config["agent-id"] = response["id"]
        config["agent-url"] = \
            config["http-api"] + "/agents/%s" % response["id"]
        self.log("agent id is %s, starting service" % config["agent-id"])
        self.scheduled_tasks.start()

    def _failureCallback(self, response):
        """internal callback which is run when the service fails to start"""
        self.exception(response)

    def startService(self):
        self.info("informing master of agent startup")

        def get_agent_data():
            ntp_client = ntplib.NTPClient()
            try:
                pool_time = ntp_client.request(config["ntp-server"])
                time_offset = int(pool_time.tx_time - time.time())

            except Exception, e:
                time_offset = 0
                self.log("failed to determine network time: %s" % e,
                         level=logging.WARNING)
            else:
                if time_offset:
                    self.log("agent time offset is %s" % time_offset,
                             level=logging.WARNING)

            data = {
                "hostname": config["hostname"],
                "ip": config["ip"],
                "use_address": config["use-address"],
                "ram": config["ram"],
                "cpus": config["cpus"],
                "port": config["port"],
                "free_ram": memory.ram_free(),
                "time_offset": time_offset,
                "state": AgentState.ONLINE}

            if config.get("remote-ip"):
                data.update(remote_ip=config["remote-ip"])

            if config.get("projects"):
               data.update(projects=config["projects"])

            return data

        # prepare a retry request to POST to the master
        retry_post_agent = RetryDeferred(
            http_post, self._startServiceCallback, self._failureCallback,
            headers={"Content-Type": "application/json"},
            max_retries=config["http-max-retries"],
            timeout=None,
            retry_delay=config["http-retry-delay"])

        retry_post_agent(
            config["http-api"] + "/agents",
            data=json.dumps(get_agent_data()))

        config["manager"] = ProcessManager(config)

        return retry_post_agent

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


def get_agent_data():
    """
    Returns a dictionary of data containing information about the
    agent.  This is the information that is also passed along to
    the master.
    """
    ntp_client = ntplib.NTPClient()

    ntplog.debug(
        "querying ntp server %r for current time", config["ntp-server"])
    try:
        pool_time = ntp_client.request(config["ntp-server"])

    except Exception, e:
        time_offset = 0
        ntplog.warning("failed to determine network time: %s", e)

    else:
        time_offset = int(pool_time.tx_time - time.time())

        # format the offset for logging purposes
        dtime = datetime.fromtimestamp(pool_time.tx_time)
        ntplog.debug(
            "network time: %s (local offset: %r)",
            dtime.isoformat(), time_offset)

        if time_offset:
            ntplog.warning(
                "agent is %r second(s) off from ntp server at %r",
                time_offset, config["ntp-server"])

    data = {
        "hostname": config["hostname"],
        "ip": config["ip"],
        "use_address": config["use-address"],
        "ram": config["ram"],
        "cpus": config["cpus"],
        "port": config["port"],
        "free_ram": memory.ram_free(),
        "time_offset": time_offset,
        "state": AgentState.ONLINE}

    if config.get("remote-ip"):
        data.update(remote_ip=config["remote-ip"])

    if config.get("projects"):
       data.update(projects=config["projects"])

    return data


def agent():
    def initial_post_success(response):
        """internal callback used to start the service itself"""
        config["agent-id"] = response["id"]
        config["agent-url"] = \
            config["http-api"] + "/agents/%s/" % response["id"]

        print "agent id is %s, starting service" % config["agent-id"]
        # self.log("agent id is %s, starting service" % config["agent-id"])
        # self.scheduled_tasks.start()

    def initial_post_failure(response):
        log.err(response)
        # TODO: try again or write out some info for debugging

    # post the agent's status to the master before
    # we do anything else
    retry_post_agent = RetryDeferred(
        http_post, initial_post_success, initial_post_failure,
        headers={"Content-Type": "application/json"},
        max_retries=config["http-max-retries"],
        timeout=None,
        retry_delay=config["http-retry-delay"])

    retry_post_agent(
        config["master-api"] + "/agents/",
        data=get_agent_data())


