# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
# Copyright 2014 Ambient Entertainment GmbH & Co. KG
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
import sys
import time
import uuid
from datetime import datetime, timedelta
from functools import partial
from os.path import join
from platform import platform

try:
    from httplib import (
        responses, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR, BAD_REQUEST)
except ImportError:  # pragma: no cover
    from http.client import (
        responses, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR, BAD_REQUEST)

import treq
from ntplib import NTPClient
from twisted.internet import reactor
from twisted.internet.defer import (
    Deferred, inlineCallbacks, returnValue, DeferredLock)
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.task import deferLater

from pyfarm.core.enums import NUMERIC_TYPES, AgentState
from pyfarm.agent.config import config
from pyfarm.agent.http.api.assign import Assign
from pyfarm.agent.http.api.base import APIRoot, Versions
from pyfarm.agent.http.api.config import Config
from pyfarm.agent.http.api.state import Status, Stop, Restart
from pyfarm.agent.http.api.tasks import Tasks
from pyfarm.agent.http.api.tasklogs import TaskLogs
from pyfarm.agent.http.api.update import Update
from pyfarm.agent.http.core.client import post, http_retry_delay, post_direct
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.http.core.server import Site, StaticPath
from pyfarm.agent.http.system import Index, Configuration
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import memory, network, system, cpu, graphics
from pyfarm.agent import utility

svclog = getLogger("agent.service")
ntplog = getLogger("agent.service.ntp")


class Agent(object):
    """
    Main class associated with getting getting the internals of the
    agent's operations up and running including adding or updating
    itself with the master, starting the periodic task manager,
    and handling shutdown conditions.
    """
    def __init__(self):
        # so parts of this instance are accessible elsewhere
        assert "agent" not in config
        config["agent"] = self
        self.services = {}
        self.register_shutdown_events = False
        self.last_free_ram_post = time.time()
        self.repeating_call_counter = {}
        self.shutdown_timeout = None
        self.post_shutdown_lock = DeferredLock()
        self.stop_lock = DeferredLock()
        self.reannouce_lock = DeferredLock()
        self.stopped = False

        # Register a callback so self.shutdown_timeout is set when
        # "shutting_down" is set or modified.
        config.register_callback(
            "shutting_down", self.callback_shutting_down_changed)

    @classmethod
    def agent_api(cls):
        """
        Return the API url for this agent or None if `agent_id` has not
        been set
        """
        try:
            return cls.agents_endpoint() + str(config["agent_id"])
        except KeyError:
            svclog.error(
                "The `agent_id` configuration value has not been set yet")
            return None

    @classmethod
    def agents_endpoint(cls):
        """
        Returns the API endpoint for used for updating or creating
        agents on the master
        """
        return config["master_api"] + "/agents/"

    @property
    def shutting_down(self):
        return config.get("shutting_down", False)

    @shutting_down.setter
    def shutting_down(self, value):
        assert value in (True, False)
        config["shutting_down"] = value

    def repeating_call(
            self, delay, function, function_args=None, function_kwargs=None,
            now=True, repeat_max=None, function_id=None):
        """
        Causes ``function`` to be called repeatedly up until ``repeat_max``
        or until stopped.

        :param int delay:
            Number of seconds to delay between calls of ``function``.

            ..  note::

                ``delay`` is an approximate interval between when one call ends
                and the next one begins.  The exact time can vary due
                to how the Twisted reactor runs, how long it takes
                ``function`` to run and what else may be going on in the
                agent at the time.

        :param function:
            A callable function to run

        :type function_args: tuple, list
        :keyword function_args:
            Arguments to pass into ``function``

        :keyword dict function_kwargs:
            Keywords to pass into ``function``

        :keyword bool now:
            If True then run ``function`` right now in addition
            to scheduling it.

        :keyword int repeat_max:
            Repeat calling ``function`` this may times.  If not provided
            then we'll continue to repeat calling ``function`` until
            the agent shuts down.

        :keyword uuid.UUID function_id:
            Used internally to track a function's execution count.  This
            keyword exists so if you call :meth:`repeating_call` multiple
            times on the same function or method it will handle ``repeat_max``
            properly.
        """
        if self.shutting_down:
            svclog.debug(
                "Skipping task %s(*%r, **%r) [shutting down]",
                function.__name__, function_args, function_kwargs
            )
            return

        if function_args is None:
            function_args = ()

        if function_kwargs is None:
            function_kwargs = {}

        if function_id is None:
            function_id = uuid.uuid4()

        assert isinstance(delay, NUMERIC_TYPES[:-1])
        assert callable(function)
        assert isinstance(function_args, (list, tuple))
        assert isinstance(function_kwargs, dict)
        assert repeat_max is None or isinstance(repeat_max, int)
        repeat_count = self.repeating_call_counter.setdefault(function_id, 0)

        if repeat_max is None or repeat_count < repeat_max:
            svclog.debug(
                "Executing task %s(*%r, **%r).  Next execution in %s seconds.",
                function.__name__, function_args, function_kwargs, delay
            )

            # Run this function right now using another deferLater so
            # it's scheduled by the reactor and executed before we schedule
            # another.
            if now:
                deferLater(
                    reactor, 0, function, *function_args, **function_kwargs
                )
                self.repeating_call_counter[function_id] += 1
                repeat_count = self.repeating_call_counter[function_id]

            # Schedule the next call but only if we have not hit the max
            if repeat_max is None or repeat_count < repeat_max:
                deferLater(
                    reactor, delay, self.repeating_call, delay,
                    function, function_args=function_args,
                    function_kwargs=function_kwargs, now=True,
                    repeat_max=repeat_max, function_id=function_id
                )

    def should_reannounce(self):
        """Small method which acts as a trigger for :meth:`reannounce`"""
        if self.reannouce_lock.locked or self.shutting_down:
            return False

        contacted = config.master_contacted(update=False)
        remaining = (datetime.utcnow() - contacted).total_seconds()
        return remaining > config["agent_master_reannounce"]

    def reannounce(self, force=False):
        """
        Method which is used to periodically contact the master.  This
        method is generally called as part of a scheduled task.
        """
        yield self.reannouce_lock.acquire()
        if not self.should_reannounce() and not force:
            yield self.reannouce_lock.release()
            returnValue(None)

        svclog.debug("Announcing %s to master", config["agent_hostname"])
        data = None
        while True:  # for retries
            try:
                response = yield post_direct(
                    self.agent_api(),
                    data={
                        "state": config["state"],
                        "current_assignments": config.get(
                            "current_assignments", {}),  # may not be set yet
                        "free_ram": memory.free_ram()}
                )

            except Exception as error:
                # Don't retry because reannounce is called periodically
                svclog.error(
                    "Failed to announce self to the master: %s.  This "
                    "request will not be retried.", error)
                break

            else:
                data = yield treq.json_content(response)
                if response.code == OK:
                    config.master_contacted(announcement=True)
                    svclog.info("Announced self to the master server.")
                    break

                elif response.code >= INTERNAL_SERVER_ERROR:
                    if not self.shutting_down:
                        delay = http_retry_delay()
                        svclog.warning(
                            "Could not announce self to the master server, "
                            "internal server error: %s.  Retrying in %s "
                            "seconds.", data, delay)

                        deferred = Deferred()
                        reactor.callLater(delay, deferred.callback, None)
                        yield deferred
                    else:
                        svclog.warning(
                            "Could not announce to master. Not retrying "
                            "because of pending shutdown.")
                        break

                elif response.code == NOT_FOUND:
                    svclog.warning("The master says it does not know about our "
                                   "agent id. Posting as a new agent.")
                    yield self.post_agent_to_master()
                    break

                # If this is a client problem retrying the request
                # is unlikely to fix the issue so we stop here
                elif response.code >= BAD_REQUEST:
                    svclog.error(
                        "Failed to announce self to the master, bad "
                        "request: %s.  This request will not be retried.",
                        data)
                    break

                else:
                    svclog.error(
                        "Unhandled error when posting self to the "
                        "master: %s (code: %s).  This request will not be "
                        "retried.", data, response.code)
                    break

        yield self.reannouce_lock.release()
        returnValue(data)

    def system_data(self, requery_timeoffset=False):
        """
        Returns a dictionary of data containing information about the
        agent.  This is the information that is also passed along to
        the master.
        """
        # query the time offset and then cache it since
        # this is typically a blocking operation
        if config["agent_time_offset"] == "auto":
            config["agent_time_offset"] = None

        if requery_timeoffset or config["agent_time_offset"] is None:
            ntplog.info(
                "Querying ntp server %r for current time",
                config["agent_ntp_server"])

            ntp_client = NTPClient()
            try:
                pool_time = ntp_client.request(
                    config["agent_ntp_server"],
                    version=config["agent_ntp_server_version"])

            except Exception as e:
                ntplog.warning("Failed to determine network time: %s", e)

            else:
                config["agent_time_offset"] = \
                    int(pool_time.tx_time - time.time())

                # format the offset for logging purposes
                utcoffset = datetime.utcfromtimestamp(pool_time.tx_time)
                iso_timestamp = utcoffset.isoformat()
                ntplog.debug(
                    "network time: %s (local offset: %r)",
                    iso_timestamp, config["agent_time_offset"])

                if config["agent_time_offset"] != 0:
                    ntplog.warning(
                        "Agent is %r second(s) off from ntp server at %r",
                        config["agent_time_offset"],
                        config["agent_ntp_server"])

        data = {
            "id": config["agent_id"],
            "hostname": config["agent_hostname"],
            "version": config.version,
            "os_class": system.operating_system(),
            "os_fullname": platform(),
            "ram": int(config["agent_ram"]),
            "cpus": config["agent_cpus"],
            "cpu_name": cpu.cpu_name(),
            "port": config["agent_api_port"],
            "free_ram": memory.free_ram(),
            "time_offset": config["agent_time_offset"] or 0,
            "state": config["state"],
            "mac_addresses": list(network.mac_addresses()),
            "current_assignments": config.get(
                "current_assignments", {})}  # may not be set yet

        try:
            gpu_names = graphics.graphics_cards()
            data["gpus"] = gpu_names
        except graphics.GPULookupError:
            pass

        if "remote_ip" in config:
            data.update(remote_ip=config["remote_ip"])

        return data

    def build_http_resource(self):
        svclog.debug("Building HTTP Service")
        root = Resource()

        # static endpoints to redirect resources
        # to the right objects
        root.putChild(
            "favicon.ico",
            StaticPath(join(config["agent_static_root"], "favicon.ico"),
                       defaultType="image/x-icon"))
        root.putChild(
            "static",
            StaticPath(config["agent_static_root"]))

        # external endpoints
        root.putChild("", Index())
        root.putChild("configuration", Configuration())

        # api endpoints
        api = root.putChild("api", APIRoot())
        api.putChild("versions", Versions())
        v1 = api.putChild("v1", APIRoot())

        # Top level api endpoints
        v1.putChild("assign", Assign(self))
        v1.putChild("tasks", Tasks())
        v1.putChild("config", Config())
        v1.putChild("task_logs", TaskLogs())

        # Endpoints which are generally used for status
        # and operations.
        v1.putChild("status", Status())
        v1.putChild("stop", Stop())
        v1.putChild("restart", Restart())
        v1.putChild("update", Update())

        return root

    def _start_manhole(self, port, username, password):
        """
        Starts the manhole server so we can connect to the agent
        over telnet
        """
        if "manhole" in self.services:
            svclog.warning(
                "Telnet manhole service is already running on port %s",
                self.services["manhole"].port)
            return

        svclog.info("Starting telnet manhole on port %s", port)

        # Since we don't always need this module we import
        # it here to save on memory and other resources
        from pyfarm.agent.manhole import manhole_factory

        # Contains the things which will be in the top level
        # namespace of the Python interpreter.
        namespace = {
            "config": config, "agent": self,
            "jobtypes": config["jobtypes"],
            "current_assignments": config["current_assignments"]}

        factory = manhole_factory(namespace, username, password)
        self.services["manhole"] = reactor.listenTCP(port, factory)

    def _start_http_api(self, port):
        """
        Starts the HTTP api so the master can communicate
        with the agent.
        """
        if "api" in self.services:
            svclog.warning(
                "HTTP API service already running on port %s",
                self.services["api"].port)
            return

        http_resource = self.build_http_resource()
        self.services["api"] = reactor.listenTCP(port, Site(http_resource))

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
        if config["agent_manhole"]:
            self._start_manhole(config["agent_manhole_port"],
                                config["agent_manhole_username"],
                                config["agent_manhole_password"])

        # setup the internal http server so external entities can
        # interact with the service.
        if http_server:
            self._start_http_api(config["agent_api_port"])

        # Update the configuration with this pid (which may be different
        # than the original pid).
        config["pids"].update(child=os.getpid())

        # get ready to 'publish' the agent
        config.register_callback(
            "agent_id",
            partial(
                self.callback_agent_id_set, shutdown_events=shutdown_events))
        return self.post_agent_to_master()

    @inlineCallbacks
    def stop(self):
        """
        Internal code which stops the agent.  This will terminate any running
        processes, inform the master of the terminated tasks, update the
        state of the agent on the master.
        """
        yield self.stop_lock.acquire()
        if self.stopped:
            yield self.stop_lock.release()
            svclog.warning("Agent is already stopped")
            returnValue(None)

        svclog.info("Stopping the agent")

        self.shutting_down = True
        self.shutdown_timeout = (
            datetime.utcnow() + timedelta(
                seconds=config["agent_shutdown_timeout"]))

        utility.remove_file(
            config["agent_lock_file"], retry_on_exit=True, raise_=False)

        svclog.debug("Stopping execution of jobtypes")
        for jobtype_id, jobtype in config["jobtypes"].items():
            try:
                jobtype.stop()
            except Exception as error:  # pragma: no cover
                svclog.warning(
                    "Error while calling stop() on %s (id: %s): %s",
                    jobtype, jobtype_id, error
                )
                config["jobtypes"].pop(jobtype_id)

        svclog.info(
            "Waiting on %s job types to terminate", len(config["jobtypes"]))

        while config["jobtypes"] and datetime.utcnow() < self.shutdown_timeout:
            for jobtype_id, jobtype in config["jobtypes"].copy().items():
                if not jobtype._has_running_processes():
                    svclog.warning(
                        "%r has not removed itself, forcing removal",
                        jobtype)
                    config["jobtypes"].pop(jobtype_id)

            # Brief delay so we don't tie up the cpu
            delay = Deferred()
            reactor.callLater(1, delay.callback, None)
            yield delay

        if self.agent_api() is not None:
            try:
                yield self.post_shutdown_to_master()
            except Exception as error:  # pragma: no cover
                svclog.warning(
                    "Error while calling post_shutdown_to_master()", error)
        else:
            svclog.warning("Cannot post shutdown, agent_api() returned None")

        self.stopped = True
        yield self.stop_lock.release()
        returnValue(None)

    def sigint_handler(self, *_):
        utility.remove_file(
            config["run_control_file"], retry_on_exit=True, raise_=False)

        def errback(failure):
            svclog.error(
                "Error while attempting to shutdown the agent: %s", failure)

            # Stop the reactor but handle the exit code ourselves otherwise
            # Twisted will just exit with 0.
            reactor.stop()
            sys.exit(1)

        # Call stop() and wait for it to finish before we stop
        # the reactor.
        # NOTE: We're not using inlineCallbacks here because reactor.stop()
        # would be called in the middle of the generator unwinding
        deferred = self.stop()
        deferred.addCallbacks(lambda _: reactor.stop(), errback)

    @inlineCallbacks
    def post_shutdown_to_master(self, stop_reactor=True):
        """
        This method is called before the reactor shuts down and lets the
        master know that the agent's state is now ``offline``
        """
        # We're under the assumption that something's wrong with
        # our code if we try to call this method before self.shutting_down
        # is set.
        assert self.shutting_down
        yield self.post_shutdown_lock.acquire()

        svclog.info("Informing master of shutdown")

        # Because post_shutdown_to_master is blocking and needs to
        # stop the reactor from finishing we perform the retry in-line
        data = None
        while True:
            try:
                response = yield post_direct(
                    self.agent_api(),
                    data={
                        "state": AgentState.OFFLINE,
                        "free_ram": memory.free_ram(),
                        "current_assignments": config["current_assignments"]})

            # When we get a hard failure it could be an issue with the
            # server, although it's unlikely, so we retry.  Only retry
            # for a set period of time though since the shutdown as a timeout
            except Exception as failure:
                if self.shutdown_timeout > datetime.utcnow():
                    delay = http_retry_delay()
                    svclog.warning(
                        "State update failed due to unhandled error: %s.  "
                        "Retrying in %s seconds",
                        failure, delay)

                    # Wait for 'pause' to fire, introducing a delay
                    pause = Deferred()
                    reactor.callLater(delay, pause.callback, None)
                    yield pause

                else:
                    svclog.warning(
                        "State update failed due to unhandled error: %s.  "
                        "Shutdown timeout reached, not retrying.",
                        failure)
                    break

            else:
                data = yield treq.json_content(response)
                if response.code == NOT_FOUND:
                    svclog.warning(
                        "Agent %r no longer exists, cannot update state.",
                        config["agent_id"])
                    break

                elif response.code == OK:
                    svclog.info(
                        "Agent %r has POSTed shutdown state change "
                        "successfully.",
                        config["agent_id"])
                    break

                elif response.code >= INTERNAL_SERVER_ERROR:
                    if self.shutdown_timeout > datetime.utcnow():
                        delay = http_retry_delay()
                        svclog.warning(
                            "State update failed due to server error: %s.  "
                            "Retrying in %s seconds.",
                            data, delay)

                        # Wait for 'pause' to fire, introducing a delay
                        pause = Deferred()
                        reactor.callLater(delay, pause.callback, None)
                        yield pause
                    else:
                        svclog.warning(
                            "State update failed due to server error: %s.  "
                            "Shutdown timeout reached, not retrying.",
                            data)
                        break

        yield self.post_shutdown_lock.release()
        returnValue(data)

    @inlineCallbacks
    def post_agent_to_master(self):
        """
        Runs the POST request to contact the master.  Running this method
        multiple times should be considered safe but is generally something
        that should be avoided.
        """
        url = self.agents_endpoint()
        data = self.system_data()

        try:
            response = yield post_direct(url, data=data)
        except Exception as failure:
            delay = http_retry_delay()
            if isinstance(failure, ConnectionRefusedError):
                svclog.error(
                    "Failed to POST agent to master, the connection was "
                    "refused. Retrying in %s seconds", delay)
            else:  # pragma: no cover
                svclog.error(
                    "Unhandled error when trying to POST the agent to the "
                    "master. The error was %s.", failure)

            if not self.shutting_down:
                svclog.info(
                    "Retrying failed POST to master in %s seconds.", delay)
                yield deferLater(reactor, delay, self.post_agent_to_master)
            else:
                svclog.warning("Not retrying POST to master, shutting down.")

        else:
            # Master might be down or have some other internal problems
            # that might eventually be fixed.  Retry the request.
            if response.code >= INTERNAL_SERVER_ERROR:
                if not self.shutting_down:
                    delay = http_retry_delay()
                    svclog.warning(
                        "Failed to post to master due to a server side error "
                        "error %s, retrying in %s seconds",
                        response.code, delay)
                    yield deferLater(reactor, delay, self.post_agent_to_master)
                else:
                    svclog.warning(
                        "Failed to post to master due to a server side error "
                        "error %s. Not retrying, because the agent is "
                        "shutting down", response.code)

            # Master is up but is rejecting our request because there's
            # something wrong with it.  Do not retry the request.
            elif response.code >= BAD_REQUEST:
                text = yield response.text()
                svclog.error(
                    "%s accepted our POST request but responded with code %s "
                    "which is a client side error.  The message the server "
                    "responded with was %r.  Sorry, but we cannot retry this "
                    "request as it's an issue with the agent's request.",
                    url, response.code, text)

            else:
                data = yield treq.json_content(response)
                config["agent_id"] = data["id"]
                config.master_contacted()

                if response.code == OK:
                    svclog.info(
                        "POST to %s was successful. Agent %s was updated.",
                        url, config["agent_id"])

                elif response.code == CREATED:
                    svclog.info(
                        "POST to %s was successful.  A new agent "
                        "with an id of %s was created.",
                        url, config["agent_id"])

                returnValue(data)

    def callback_agent_id_set(
            self, change_type, key, new_value, old_value, shutdown_events=True):
        """
        When `agent_id` is created we need to:

            * Register a shutdown event so that when the agent is told to
              shutdown it will notify the master of a state change.
            * Star the scheduled task manager
        """
        if key == "agent_id" and change_type == config.CREATED \
                and not self.register_shutdown_events:
            if shutdown_events:
                self.register_shutdown_events = True

            # set the initial free_ram
            config["free_ram"] = memory.free_ram()

            config.master_contacted()
            svclog.debug(
                "`%s` was %s, adding system event trigger for shutdown",
                key, change_type)

            self.repeating_call(
                config["agent_master_reannounce"], self.reannounce)

    def callback_shutting_down_changed(
            self, change_type, key, new_value, old_value):
        """
        When `shutting_down` is changed in the configuration, set or
        reset self.shutdown_timeout
        """
        if change_type not in (config.MODIFIED, config.CREATED):
            return

        if new_value is not True:
            self.shutdown_timeout = None
            return

        self.shutdown_timeout = timedelta(
            seconds=config["agent_shutdown_timeout"]) + datetime.utcnow()
        svclog.debug("New shutdown_timeout is %s", self.shutdown_timeout)
