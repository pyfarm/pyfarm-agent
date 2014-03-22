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
from httplib import (
    responses, BAD_REQUEST, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR)

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
    """
    Main class associated with getting getting the internals
    of the internals of the agent's operations up and running including
    adding or updating itself with the master, starting the periodic
    task manager, and handling shutdown conditions.
    """
    def __init__(self):
        self.shutdown_registered = False
        self.scheduled_tasks = ScheduledTaskManager()

    @classmethod
    def agent_api(cls):
        """
        Return the API url for this agent or None if `agent-id` has not
        been set
        """
        try:
            return "%(master-api)s/agents/%(agent-id)s" % config
        except KeyError:
            svclog.error(
                "The `agent-id` configuration value has not been set yet")
            return None

    @classmethod
    def http_retry_delay(cls, uniform=False, get_delay=random):
        """
        Returns a floating point value that can be used to delay
        an http request.  The main purpose of this is to ensure that not all
        requests are run with the same interval between then.  This helps to
        ensure that if the same request, such as agents coming online, is being
        run on multiple systems they should be staggered a little more than
        they would be without the non-uniform delay.
        """
        # TODO: provide command line flags for jitter
        delay = config["http-retry-delay"]
        if not uniform:
            delay += get_delay()
        return delay

    def system_data(self, requery_timeoffset=False):
        """
        Returns a dictionary of data containing information about the
        agent.  This is the information that is also passed along to
        the master.
        """
        # query the time offset and then cache it since
        # this is typically a blocking operation
        if requery_timeoffset or config["time-offset"] is None:
            ntplog.info(
                "Querying ntp server %r for current time", config["ntp-server"])

            ntp_client = NTPClient()
            try:
                pool_time = ntp_client.request(config["ntp-server"])

            except Exception as e:
                ntplog.warning("Failed to determine network time: %s", e)

            else:
                config["time-offset"] = int(pool_time.tx_time - time.time())

                # format the offset for logging purposes
                utcoffset = datetime.utcfromtimestamp(pool_time.tx_time)
                iso_timestamp = utcoffset.isoformat()
                ntplog.debug(
                    "network time: %s (local offset: %r)",
                    iso_timestamp, config["time-offset"])

                if config["time-offset"] != 0:
                    ntplog.warning(
                        "Agent is %r second(s) off from ntp server at %r",
                        config["time-offset"], config["ntp-server"])

        data = {
            "hostname": config["hostname"],
            "ip": config["ip"],
            "use_address": config["use-address"],
            "ram": int(config["ram"]),
            "cpus": config["cpus"],
            "port": config["port"],
            "free_ram": int(memory.ram_free()),
            "time_offset": config["time-offset"] or 0,
            "state": config["state"]}

        if "remote-ip" in config:
            data.update(remote_ip=config["remote-ip"])

        if "projects" in config:
           data.update(projects=config["projects"])

        return data

    def run(self):
        """
        Internal code which starts the agent, registers it with the master,
        and performs the other steps necessary to get things running.
        """
        # TODO: start the internal http server here
        self.start_search_for_agent()
        config.register_callback(
            "agent-id", self.callback_agent_id_set)

    def shutdown_tasks(self):
        """
        This method is called before the reactor shuts and stops
        any running tasks.
        """
        svclog.info("Stopping tasks")
        self.scheduled_tasks.stop()
        # TODO: stop tasks

    def shutdown_post_agent_state(self):
        """
        This method is called before the reactor shuts down and lets the
        master know that the agent's state is now ``offline``
        """
        svclog.info("Agent is shutting down")

        # This deferred is fired when we've either been successful
        # or failed to letting the master know we're shutting
        # down.  It is the last object to fire so if you have to do
        # other things, like stop the process, that should happen before
        # the callback for this one is run.
        results_trigger = Deferred()

        def post_update(run=True):
            def perform():
                return post(
                    self.agent_api(),
                    data={
                        "state": AgentState.OFFLINE,
                        "free_ram": int(memory.ram_free())},
                    callback=callback_post_offline,
                    errback=errback_post_offline)

            return perform() if run else perform

        def success_callback(successful):
            if successful:
                svclog.info(
                    "Agent %r has shutdown successfully", config["agent-id"])
            elif successful is None:
                svclog.warning(
                    "Agent %r may not have shutdown successfully.",
                    config["agent-id"])
            else:
                svclog.error(
                    "Agent %r has not shutdown successfully",
                    config["agent-id"])

        def callback_post_offline(response):
            # there's only one response that should be considered valid
            if response.code == NOT_FOUND:
                svclog.warning(
                    "Agent %r no longer exists, cannot update state",
                    config["agent-id"])
                results_trigger.callback(None)

            elif response.code >= INTERNAL_SERVER_ERROR:
                delay = random() + random()
                svclog.warning(
                    "State update failed due to server error: %s.  "
                    "Retrying in %s seconds",
                    response.data(), delay)
                reactor.callLater(delay, response.request.retry)

            elif response.code == OK:
                # Trigger the final deferred object so the reactor can exit
                results_trigger.callback(True)

            else:
                delay = random() + random()
                svclog.warning(
                    "State update failed due to unhandled error: %s.  "
                    "Retrying in %s seconds",
                    response.data(), delay)
                reactor.callLater(delay, response.request.retry)

        def errback_post_offline(failure):
            delay = self.http_retry_delay()
            svclog.warning(
                "State update failed due to unhandled error: %s.  "
                "Retrying in %s seconds",
                failure, delay)
            reactor.callLater(delay, post_update(run=False))

        results_trigger.addCallback(success_callback)

        # Post our current state to the master.  We're only posting ram_free
        # and state here because all other fields would be updated the next
        # time the agent starts up.  ram_free would be too but having it
        # here is beneficial in cases where the agent terminated abnormally.
        post_update_deferred = post_update()

        # Wait on all deferred objects to run their callbacks.  The workflow
        # here is for `post_update` to do the work it needs to perform which
        # will then call the callback on `results_trigger` which should
        # allow the reactor to exit cleanly.
        return DeferredList([post_update_deferred, results_trigger])

    def start_search_for_agent(self, run=True):
        """
        Produces a callable object which will initiate the process
        necessary to search for this agent.  This is a method on the class
        itself so we can repeat the search from any location.
        """
        def search():
            system_data = self.system_data()
            return get(
                "%(master-api)s/agents/" % config,
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
                "%(master-api)s/agents/" % config,
                callback=self.callback_agent_created,
                errback=self.errback_agent_created,
                data=self.system_data())

        return create() if run else create

    def callback_agent_created(self, response):
        """
        Callback run when we're able to create the agent on the master.  This
        method will retry the original request of the
        """
        if response.code == CREATED:
            data = response.json()
            config["agent-id"] = data["id"]
            svclog.info("Agent is now online (created on master)")
        else:
            delay = self.http_retry_delay()
            svclog.warning(
                "We expected to receive an CREATED response code but instead"
                "we got %s. Retrying in %s seconds.",
                responses[response.code], delay)
            reactor.callLater(delay, self.create_agent(run=False))

    def errback_agent_created(self, failure):
        """
        Error handler run whenever an error is raised while trying
        to create the agent on the master.  The failed request will be
        retried.
        """
        delay = self.http_retry_delay()
        svclog.warning(
            "There was a problem creating the agent: %s.  Retrying "
            "in %r seconds.", failure, delay)
        reactor.callLater(delay, self.create_agent(run=False))

    def post_to_existing_agent(self, run=True):
        """
        Either executes the code necessary to post system data to
        an existing agent or returns a callable to do so.
        """
        def run_post():
            return post(self.agent_api(),
                data=self.system_data(),
                callback=self.callback_post_existing_agent,
                errback=self.errback_post_existing_agent)
        return run_post() if run else run_post

    def callback_post_existing_agent(self, response):
        """
        Called when we got a response back while trying to post updated
        information to an existing agent.  This should happen as a result
        of other callbacks being run at startup.
        """
        # print config.keys()
        if response.code == OK:
            svclog.info("Agent is now online (updated on master)")
        else:
            delay = self.http_retry_delay()
            svclog.warning(
                "We expected to receive an OK response code but instead"
                "we got %s.  Retrying in %s.", responses[response.code], delay)
            reactor.callLater(delay, self.post_to_existing_agent(run=False))

    def errback_post_existing_agent(self, failure):
        """
        Error handler which is called if we fail to post an update
        to an existing agent for some reason.
        """
        delay = self.http_retry_delay()
        svclog.warning(
            "There was error updating an existing agent: %s.  Retrying "
            "in %r seconds", failure, delay)
        reactor.callLater(delay, self.post_to_existing_agent(run=False))

    def callback_search_for_agent(self, response):
        """
        Callback that gets called whenever we search for the agent.  This
        search occurs at startup and after this callback finishes we
        we'll either post updates to an existing agent or
        """
        delay = self.http_retry_delay()

        if response.code == OK:
            system_data = self.system_data()
            agents_found = response.json()

            if agents_found:
                svclog.debug(
                    "This agent may already be registered with the master")

                # see if there's an agent which is the exact same as
                # this agent
                similar_agents = []
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

                            # now that we've found the agent,
                            # post out date to the master
                            self.post_to_existing_agent()
                            break

                        # close but it's the wrong port
                        elif match_ip and match_hostname:
                            similar_agents.append(agent)

                    else:
                        if similar_agents:
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
        """
        Callback that gets called when we fail to search for the agent
        due to an exception (not a response code).
        """
        delay = self.http_retry_delay()
        if failure.type is ConnectionRefusedError:
            agents_api = "%(master-api)s/agents/" % config
            svclog.warning(
                "Connection refused to %s: %s. Retrying in %s seconds.",
                agents_api, failure, delay)

        else:
            # TODO: need a better way of making these errors visible
            svclog.critical(
                "Unhandled exception: %s.  Retrying in %s seconds.",
                failure, delay)
            svclog.exception(failure)

        reactor.callLater(delay, self.start_search_for_agent(run=False))

    def callback_agent_id_set(self, change_type, key, value):
        """
        When `agent-id` is created we need to:

            * Register a shutdown event so that when the agent is told to
              shutdown it will notify the master of a state change.
            * Star the scheduled task manager
        """
        if key == "agent-id" and change_type == config.CREATED \
                and not self.shutdown_registered:
            self.shutdown_registered = True
            reactor.addSystemEventTrigger(
                "before", "shutdown", self.shutdown_tasks)
            reactor.addSystemEventTrigger(
                "before", "shutdown", self.shutdown_post_agent_state)
            svclog.debug(
                "`%s` was %s, adding system event trigger for shutdown",
                key, change_type)
            self.scheduled_tasks.start()
