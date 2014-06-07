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

import os
import time
import signal
from datetime import datetime
from functools import partial
from httplib import (
    responses, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR)
from os.path import join
from random import random

from ntplib import NTPClient
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.error import ConnectionRefusedError

from pyfarm.core.enums import AgentState
from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.api.assign import Assign
from pyfarm.agent.http.api.base import APIRoot, Versions
from pyfarm.agent.http.api.config import Config
from pyfarm.agent.http.api.log import LogQuery
from pyfarm.agent.http.api.tasks import Tasks
from pyfarm.agent.http.core.client import post, http_retry_delay
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.http.core.server import Site, StaticPath
from pyfarm.agent.http.log import Logging
from pyfarm.agent.http.system import Index, Configuration
from pyfarm.agent.tasks import ScheduledTaskManager
from pyfarm.agent.sysinfo import memory

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
        # so parts of this instance are accessible elsewhere
        assert "agent" not in config
        config["agent"] = self
        self.http = None
        self.sigint_signal_count = 0
        self.register_shutdown_events = False
        self.scheduled_tasks = ScheduledTaskManager()
        self.last_free_ram_post = time.time()

    @classmethod
    def agent_api(cls):
        """
        Return the API url for this agent or None if `agent-id` has not
        been set
        """
        try:
            return cls.agents_endpoint() + str(config["agent-id"])
        except KeyError:
            svclog.error(
                "The `agent-id` configuration value has not been set yet")
            return None

    @classmethod
    def agents_endpoint(cls):
        """
        Returns the API endpoint for used for updating or creating
        agents on the master
        """
        return config["master-api"] + "/agents/"

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
            "systemid": config["systemid"],
            "hostname": config["hostname"],
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

    def build_http_resource(self):
        svclog.debug("Building HTTP Service")
        root = Resource()

        # static endpoints to redirect resources
        # to the right objects
        root.putChild(
            "favicon.ico",
            StaticPath(join(config["static-files"], "favicon.ico"),
                       defaultType="image/x-icon"))
        root.putChild(
            "static",
            StaticPath(config["static-files"]))

        # external endpoints
        root.putChild("", Index())
        root.putChild("configuration", Configuration())
        root.putChild("logging", Logging())

        # TODO: renable these once they are working again
        # resource.putChild("assign", Assign(config))
        # resource.putChild("processes", Processes(config))
        # resource.putChild("shutdown", Shutdown(config))

        # api endpoints
        api = root.putChild("api", APIRoot())
        api.putChild("versions", Versions())
        v1 = api.putChild("v1", APIRoot())
        v1.putChild("assign", Assign())
        v1.putChild("tasks", Tasks())
        v1.putChild("config", Config())
        v1.putChild("logging", LogQuery())

        return root

    def start(self, shutdown_events=True, http_server=True):
        """
        Internal code which starts the agent, registers it with the master,
        and performs the other steps necessary to get things running.

        :param bool shutdown_events:
            If True register all shutdown events so certain actions, such as
            information the master we're going offline, can take place.

        :param bool http_server:
            If True then construct and serve the externally facing http
            server.
        """
        # setup the internal http server so external entities can
        # interact with the service.
        if http_server:
            http_resource = self.build_http_resource()
            self.http = Site(http_resource)
            reactor.listenTCP(config["port"], self.http)

        # Update the configuration with this pid (which may be different
        # than the original pid).
        config["pids"].update(child=os.getpid())

        # get ready to 'publish' the agent
        config.register_callback(
            "agent-id",
            partial(
                self.callback_agent_id_set, shutdown_events=shutdown_events))
        config.register_callback("free_ram", self.callback_free_ram_changed)
        config.register_callback("cpus", self.callback_cpu_count_changed)
        return self.post_agent_to_master()

    def stop(self, *_):  # this is the SIGINT handler, hence the *_
        """
        Internal code which stops the agent.  This will terminate any running
        processes, inform the master of the terminated tasks, update the
        state of the agent on the master.
        """
        if self.sigint_signal_count:
            svclog.critical(
                "!!! Continuing to press Ctrl+C will forcefully terminate the "
                "agent.  This may not be a safe thing to do.")

        # If the agent is stuck in a loop, such as with posting itself
        # to the master, we may forcefully terminate the agent by hitting
        # Ctrl+C multiple times
        if self.sigint_signal_count > 2:
            reactor.stop()
            svclog.critical("Agent has been forcefully terminated")
            os._exit(1)

        self.sigint_signal_count += 1

        svclog.info("Stopping the agent")
        self.scheduled_tasks.stop()

        master_update_finished = self.shutdown_post_update()

        # Once all other deferreds have finished, stop the reactor
        finished = DeferredList([master_update_finished])
        finished.addCallback(lambda *_: reactor.stop())

    def shutdown_post_update(self):
        """
        This method is called before the reactor shuts down and lets the
        master know that the agent's state is now ``offline``
        """
        svclog.info("Informing master of shutdown")

        # This deferred is fired when we've either been successful
        # or failed to letting the master know we're shutting
        # down.  It is the last object to fire so if you have to do
        # other things, like stop the process, that should happen before
        # the callback for this one is run.
        finished = Deferred()

        def post_update(run=True):
            def perform():
                return post(
                    self.agent_api(),
                    data={
                        "state": AgentState.OFFLINE,
                        "free_ram": int(memory.ram_free())},
                    callback=results_from_post,
                    errback=error_while_posting)

            return perform() if run else perform

        def results_from_post(response):
            if response.code == NOT_FOUND:
                svclog.warning(
                    "Agent %r no longer exists, cannot update state",
                    config["agent-id"])
                finished.callback(NOT_FOUND)

            elif response.code == OK:
                svclog.info(
                    "Agent %r has shutdown successfully", config["agent-id"])
                finished.callback(OK)

            elif response.code >= INTERNAL_SERVER_ERROR:
                delay = random() + random()
                svclog.warning(
                    "State update failed due to server error: %s.  "
                    "Retrying in %s seconds",
                    response.data(), delay)
                reactor.callLater(delay, response.request.retry)

            else:
                delay = random() + random()
                svclog.warning(
                    "State update failed due to unhandled error: %s.  "
                    "Retrying in %s seconds",
                    response.data(), delay)
                reactor.callLater(delay, response.request.retry)

        def error_while_posting(failure):
            delay = http_retry_delay()
            svclog.warning(
                "State update failed due to unhandled error: %s.  "
                "Retrying in %s seconds",
                failure, delay)
            reactor.callLater(delay, post_update(run=False))

        # Post our current state to the master.  We're only posting ram_free
        # and state here because all other fields would be updated the next
        # time the agent starts up.  ram_free would be too but having it
        # here is beneficial in cases where the agent terminated abnormally.
        post_update()
        return finished

    def errback_post_agent_to_master(self, failure):
        """
        Called when there's a failure trying to post the agent to the
        master.  This is often because of some lower level issue but it
        may be recoverable to we retry the request.
        """
        delay = http_retry_delay()

        if failure.type is ConnectionRefusedError:
            svclog.error(
                "Failed to POST agent to master, the connection was refused. "
                "Retrying in %s seconds", delay)
        else:
            svclog.error(
                "Unhandled error when trying to POST the agent to the master. "
                "The error was %s.  Retrying in %s seconds.", failure, delay)

        return reactor.callLater(
            http_retry_delay(), self.post_agent_to_master)

    def callback_post_agent_to_master(self, response):
        """
        Called when we get a response after POSTing the agent to the
        master.
        """
        # Master might be down or some other internal problem
        # that might eventually be fixed.  Retry the request.
        if response.code >= 500:
            delay = http_retry_delay()
            svclog.warning(
                "Failed to post to master due to a server side error "
                "error %s, retrying in %s seconds", response.code, delay)
            reactor.callLater(delay, self.post_agent_to_master)

        # Master is up but is rejecting our request because there's something
        # wrong with it.  Do not retry the request.
        elif response.code >= 400:
            svclog.error(
                "%s accepted our POST request but responded with code %s "
                "which is a client side error.  The message the server "
                "responded with was %r.  Sorry, but we cannot retry this "
                "request as it's an issue with the agent's request.",
                self.agents_endpoint(), response.code, response.data())

        else:
            data = response.json()
            config["agent-id"] = data["id"]

            if response.code == OK:
                svclog.info(
                    "POST to %s was successful. Agent %s was updated.",
                    self.agents_endpoint(), config["agent-id"])

            elif response.code == CREATED:
                svclog.info(
                    "POST to %s was successful.  A new agent "
                    "with an id of %s was created.",
                    self.agents_endpoint(), config["agent-id"])

    def post_agent_to_master(self):
        """
        Runs the POST request to contact the master.  Running this method
        multiple times should be considered safe but is generally something
        that should be avoided.
        """
        return post(
            self.agents_endpoint(),
            callback=self.callback_post_agent_to_master,
            errback=self.errback_post_agent_to_master,
            data=self.system_data())

    def callback_post_free_ram(self, response):
        """
        Called when we get a response back from the master
        after POSTing a change for ``free_ram``
        """
        self.last_free_ram_post = time.time()
        if response.code == OK:
            svclog.info(
                "POST %s {'free_ram': %d}` succeeded",
                self.agent_api(), response.json()["free_ram"])

        # Because this happens with a fairly high frequency we don't
        # retry failed requests because we'll be running `post_free_ram`
        # soon again anyway
        else:
            svclog.warning(
                "Failed to post `free_ram` to %s: %s %s - %s",
                self.agent_api(), response.code, responses[response.code],
                response.json())

    def errback_post_free_ram(self, failure):
        """
        Error handler which is called if we fail to post a ram
        update to the master for some reason
        """
        svclog.error(
            "Failed to post update to `free_ram` to the master: %s", failure)

    def post_free_ram(self):
        """
        Posts the current nu
        """
        since_last_update = time.time() - self.last_free_ram_post
        left_till_update = config["ram-max-report-interval"] - since_last_update

        if left_till_update > 0:
            svclog.debug(
                "Skipping POST for `free_ram` change, %.2f seconds left "
                "in current interval.", left_till_update)
            deferred = Deferred()
            deferred.callback("delay")
            return deferred
        else:
            return post(self.agent_api(),
                data={"free_ram": config["free_ram"]},
                callback=self.callback_post_free_ram,
                errback=self.errback_post_free_ram)

    def callback_free_ram_changed(self, change_type, key, new_value, old_value):
        """
        Callback used to decide and act on changes to the
        ``config['ram']`` value.
        """
        if change_type == config.MODIFIED:
            if abs(new_value - old_value) < config["ram-report-delta"]:
                svclog.debug("Not enough change in free_ram to report")
            else:
                self.post_free_ram()

    def errback_post_cpu_count_change(self, failure):
        """
        Error handler which is called if we fail to post a cpu count update
        to an existing agent for some reason.
        """
        delay = http_retry_delay()
        svclog.warning(
            "There was error updating an existing agent: %s.  Retrying "
            "in %r seconds", failure, delay)
        reactor.callLater(delay, self.post_cpu_count(run=False))

    def callback_post_cpu_count_change(self, response):
        """
        Called when we received a response from the master after
        """
        if response.code == OK:
            svclog.info("CPU count change POSTed to %s", self.agent_api())
        else:
            delay = http_retry_delay()
            svclog.warning(
                "We expected to receive an OK response code but instead"
                "we got %s.  Retrying in %s.", responses[response.code], delay)
            reactor.callLater(delay, self.post_cpu_count(run=False))

    def post_cpu_count(self, run=True):
        """POSTs CPU count changes to the master"""
        def run_post():
            return post(self.agent_api(),
                data={"cpus": config["cpus"]},
                callback=self.callback_post_cpu_count_change,
                errback=self.errback_post_cpu_count_change)
        return run_post() if run else run_post

    def callback_cpu_count_changed(
            self, change_type, key, new_value, old_value):
        """
        Callback used to decide and act on changes to the
        ``config['cpus']`` value.
        """
        if change_type == config.MODIFIED and new_value != old_value:
            svclog.debug(
                "CPU count has been changed from %s to %s",
                old_value, new_value)
            self.post_cpu_count()

    def callback_agent_id_set(
            self, change_type, key, new_value, old_value, shutdown_events=True):
        """
        When `agent-id` is created we need to:

            * Register a shutdown event so that when the agent is told to
              shutdown it will notify the master of a state change.
            * Star the scheduled task manager
        """
        if key == "agent-id" and change_type == config.CREATED \
                and not self.register_shutdown_events:
            if shutdown_events:
                self.register_shutdown_events = True

            # set the initial free_ram
            config["free_ram"] = int(memory.ram_free())

            svclog.debug(
                "`%s` was %s, adding system event trigger for shutdown",
                key, change_type)
            self.scheduled_tasks.start()
