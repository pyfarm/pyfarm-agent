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

import time
from datetime import datetime
from random import random
from httplib import BAD_REQUEST, OK

from ntplib import NTPClient
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.error import ConnectionRefusedError

from pyfarm.core.enums import AgentState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo import memory
from pyfarm.agent.http.client import post, get
from pyfarm.agent.tasks import ScheduledTaskManager
from pyfarm.agent.config import config

ntplog = getLogger("agent.ntp")
svclog = getLogger("agent.svc")


class Agent(object):
    TIME_OFFSET = config["time-offset"]

    def __init__(self):
        self.before_shutdown = None
        self.ntp_client = NTPClient()
        self.scheduled_tasks = ScheduledTaskManager()

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
                callback=self.callback_agent_created,
                errback=self.errback_agent_created,
                data=self.system_data())

        return create() if run else create

    def callback_agent_created(self, response):
        print "===============", response
        # TODO: handle response to agent creation

    def errback_agent_created(self, failure):
        delay = self.http_retry_delay()
        svclog.warning(
            "There was a problem creating the agent: %s.  Retrying "
            "in %r seconds", failure, delay)
        reactor.callLater(delay, self.create_agent(run=False))

    def callback_search_for_agent(self, response):
        delay = self.http_retry_delay()

        if response.code == OK:
            system_data = self.system_data()
            agents_found = response.json()

            if agents_found:
                svclog.debug(
                    "This agent may already be registered with the master")

                # see if one of the agents found is in fact us
                similiar_agents = []
                if isinstance(agents_found, list):
                    for agent in agents_found:
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
                            self.create_agent()
            else:
                svclog.debug(
                    "This agent is not currently registered with the master.")
                self.create_agent()

        elif config >= BAD_REQUEST:
            svclog.warning(
                "Something was either wrong with our request or the "
                "server cannot handle it at this time: %s.  Retrying in "
                "%s seconds.", response.data(), delay)
            reactor.callLater(delay, lambda: response.request.retry())

        # Retry anyway otherwise we could end up with the agent doing
        # nothing.
        else:
            svclog.error(
                "Unhandled case while attempting to find registered "
                "agent: %s (code: %s).  Retrying in %s seconds.",
                response.data(), response.code, delay)
            reactor.callLater(delay, lambda: response.request.retry())

    def errback_search_for_agent(self, failure):
        delay = self.http_retry_delay()
        agents_api = self.agents_api()

        if failure.type is ConnectionRefusedError:
            svclog.warning(
                "Connection refused to %s: %s. Retrying in %s seconds.",
                agents_api, failure, delay)

        else:
            # TODO: need a better way of making these errors visible
            svclog.critical(
                "Unhandled exception: %s.  Retrying in %s seconds.",
                failure, delay)
            svclog.exception(failure)

        reactor.callLater(delay, self.search_for_agent(run=False))

    def run(self):
        """
        Internal code which starts the agent, registers it with the master,
        and performs the other steps necessary to get things running.
        """
        self.search_for_agent()
        # TODO: add callbacks for config changes for values which need to
        # be updated in the DB too
        config.register_callback(
            "agent-id", self.callback_agent_id_set)

    def callback_agent_id_set(self, change_type, key, value):
        """
        When `agent-id` is created we need to:

            * Register a shutdown event so that when the agent is told to
              shutdown it will notify the master of a state change.
            * Star the scheduled task manager
        """
        if key == "agent-id" and change_type == config.CREATED \
                and self.before_shutdown is None:
            self.before_shutdown = reactor.addSystemEventTrigger(
                "before", "shutdown", self.shutdown)
            svclog.debug(
                "`%s` was %s, adding system event trigger for shutdown",
                key, change_type)
            self.scheduled_tasks.start()

    def shutdown(self):
        svclog.info("Agent is shutting down")

        # TODO: This is here because because you could potentially block
        # forever while the reactor is trying to shutdown.  It would be better
        # to find a way to catch the behavior causing the loop but if not
        # we should probably have a timeout in some specific cases
        # reactor.callLater(xxx, reactor.crash)

        results_trigger = Deferred()

        def success_callback(successful):
            if successful:
                svclog.info("The agent has shutdown successfully")
            else:
                svclog.warning("The agent has not shutdown successfully")

        results_trigger.addCallback(success_callback)

        def callback_post_offline(response):
            # there's only one response that should be considered valid
            if response.code != OK:
                delay = random() + random()
                svclog.warning(
                    "State update failed: %s.  Retrying in %s seconds",
                    response.data(), delay)
                reactor.callLater(delay, response.request.retry)

            else:
                # Trigger the final deferred object so the reactor can exit
                results_trigger.callback(True)

        def errback_post_offline(failure):
            # TODO: warning then retry on failure
            raise NotImplementedError

        post_update = post(self.agent_api(),
             data={"state": AgentState.OFFLINE},
             callback=callback_post_offline,
             errback=errback_post_offline)

        # Wait on all deferred objects to run their callbacks.  The workflow
        # here is for `post_update` to do the work it needs to perform which
        # will then call the callback on `results_trigger` which should
        # allow the reactor to exit cleanly.
        return DeferredList([post_update, results_trigger])