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
from random import random
from datetime import datetime
from httplib import NOT_FOUND, BAD_REQUEST, OK

import ntplib
import requests
from twisted.internet import reactor
from twisted.application.service import MultiService
from twisted.internet.error import ConnectionRefusedError

from pyfarm.core.enums import AgentState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo import memory
from pyfarm.agent.http.client import post, get
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
    TIME_OFFSET = config["time-offset"]
    scheduled_tasks = ScheduledTaskManager()
    ntp_client = ntplib.NTPClient()

    def __init__(self):
        self.first_post_attempts = 1

    @classmethod
    def agents_api(cls):
        """Return the top level API url for all agents"""
        return config["master-api"] + "/agents/"

    @classmethod
    def agent_api(cls):
        """
        Return the API url for this agent or None if `agent-id` has not
        been set
        """
        try:
            return cls.agents_api() + str(config["agent-id"])
        except KeyError:
            svclog.error(
                "The `agent-id` configuration value has not been set yet")
            return None

    @classmethod
    def http_retry_delay(cls):
        # TODO: provide command line flags for jitter
        delay = config["http-retry-delay"]
        delay += random()
        return delay

    @classmethod
    def system_data(cls, requery_timeoffset=False):
        """
        Returns a dictionary of data containing information about the
        agent.  This is the information that is also passed along to
        the master.
        """
        # query the time offset and then cache it since
        # this is typically a blocking operation
        if requery_timeoffset or cls.TIME_OFFSET is None:
            ntplog.info(
                "Querying ntp server %r for current time", config["ntp-server"])

            try:
                pool_time = cls.ntp_client.request(config["ntp-server"])

            except Exception as e:
                ntplog.warning("Failed to determine network time: %s", e)

            else:
                cls.TIME_OFFSET = int(pool_time.tx_time - time.time())

                # format the offset for logging purposes
                utcoffset = datetime.utcfromtimestamp(pool_time.tx_time)
                iso_timestamp = utcoffset.isoformat()
                ntplog.debug(
                    "network time: %s (local offset: %r)",
                    iso_timestamp, cls.TIME_OFFSET)

                if cls.TIME_OFFSET:
                    ntplog.warning(
                        "Agent is %r second(s) off from ntp server at %r",
                        cls.TIME_OFFSET, config["ntp-server"])

        data = {
            "hostname": config["hostname"],
            "ip": config["ip"],
            "use_address": config["use-address"],
            "ram": int(config["ram"]),
            "cpus": config["cpus"],
            "port": config["port"],
            "free_ram": int(memory.ram_free()),
            "time_offset": cls.TIME_OFFSET or 0,
            "state": AgentState.ONLINE}

        if config.get("remote-ip"):
            data.update(remote_ip=config["remote-ip"])

        if config.get("projects"):
           data.update(projects=config["projects"])

        return data

    def search_for_agent(self, run=True):
        """
        Produces a callable object which will initiate the process
        necessary to search for this agent.  This is a method on the class
        itself so we can repeat the search from any location.
        """
        def search():
            system_data = self.system_data()
            return get(
                self.agents_api(),
                callback=self.callback_search_for_agent,
                errback=self.errback_search_for_agent,
                params={
                    "hostname": system_data["hostname"],
                    "ip": system_data["ip"]})

        return search() if run else search

    def create_agent(self, run=True):
        """Creates a new agent on the master"""
        def create():
            svclog.info("Registering this agent with the master")
            return post(
                self.agents_api(),
                callback=self.callback_create_agent,
                errback=self.errback_create_agent,
                data=self.system_data())

        return create() if run else create

    def callback_create_agent(self, response):
        print "===============", response

    def errback_create_agent(self, failure):
        delay = self.http_retry_delay()
        svclog.warning(
            "There was a problem creating the agent: %s.  Retrying "
            "in %r seconds", failure, delay)
        reactor.callLater(delay, self.create_agent(run=False))

    def callback_search_for_agent(self, response):
        delay = self.http_retry_delay()

        if response.code == OK:
            system_data = self.system_data()
            svclog.debug(
                "This agent may already be registered with the master")

            # see if one of the agents found is in fact us
            agents = response.json()
            similiar_agents = []
            if isinstance(agents, list):
                for agent in agents:
                    match_ip = \
                        agent["ip"] == system_data["ip"]
                    match_hostname = \
                        agent["hostname"] == system_data["hostname"]
                    match_port = agent["port"] == config["port"]

                    # this agent matches exactly, setup the configuration
                    if match_ip and match_hostname and match_port:
                        svclog.info(
                            "This agent is registered with the master, its "
                            "id is %r.", agent["id"])
                        config["agent-id"] = agent["id"]
                        break

                    # close but it's the wrong port
                    elif match_ip and match_hostname:
                        similiar_agents.append(agent)

                else:
                    if similiar_agents:
                        svclog.info(
                            "There are similar agents registered with the "
                            "master but this agent is not.")

        elif response.code == NOT_FOUND:
            svclog.debug(
                "This agent is not currently registered with the master.")
            self.create_agent()

        elif config >= BAD_REQUEST:
            svclog.warning(
                "Something was either wrong with our request or the "
                "server cannot handle it at this time: %r.  Retrying in "
                "%s seconds", response.data(), delay)
            reactor.callLater(delay, lambda: response.request.retry())

        # Retry anyway otherwise we could end up with the agent doing
        # nothing.
        else:
            svclog.error(
                "Unhandled case while attempting to find registered "
                "agent.  Retrying in %s seconds", delay)
            reactor.callLater(delay, lambda: response.request.retry())

    def errback_search_for_agent(self, failure):
        delay = self.http_retry_delay()
        agents_api = self.agents_api()

        if failure.type is ConnectionRefusedError:
            svclog.warning(
                "Connection refused to %s, retrying in %s seconds",
                agents_api, delay)

        else:
            # TODO: need a better way of making these errors visible
            svclog.critical(
                "Unhandled exception, retrying in %s seconds", delay)
            svclog.exception(failure)

        reactor.callLater(delay, self.search_for_agent(run=False))

    def run(self):
        """
        Internal code which starts the agent, registers it with the master,
        and performs the other steps necessary to get things running.
        """
        self.search_for_agent()
