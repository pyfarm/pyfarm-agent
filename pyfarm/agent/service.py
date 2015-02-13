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

import atexit
import os
import time
import uuid
from datetime import datetime, timedelta
from errno import ENOENT
from functools import partial
from os.path import join, isfile
from platform import platform

try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

try:
    from httplib import (
        responses, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR, BAD_REQUEST)
except ImportError:  # pragma: no cover
    from http.client import (
        responses, OK, CREATED, NOT_FOUND, INTERNAL_SERVER_ERROR, BAD_REQUEST)

import treq
from ntplib import NTPClient
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.task import deferLater
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

        # reannounce_client is set when the agent id is
        # first set. reannounce_client_instance ensures
        # that once we start the announcement process we
        # won't try another until we're finished
        self.reannounce_client_request = None

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
        if self.reannounce_client_request is not None:
            svclog.debug("Agent is already trying to announce itself.")
            return

        if self.shutting_down:
            return False

        contacted = config.master_contacted(update=False)
        remaining = (datetime.utcnow() - contacted).total_seconds()
        return remaining > config["agent_master_reannounce"]

    def reannounce(self):
        """
        Method which is used to periodically contact the master.  This
        method is generally called as part of a scheduled task.
        """
        if not self.should_reannounce():
            return

        svclog.debug("Announcing %s to master", config["agent_hostname"])

        def callback(response):
            if response.code == OK:
                self.reannounce_client_request = None
                config.master_contacted(announcement=True)
                svclog.info("Announced self to the master server.")

            elif response.code >= INTERNAL_SERVER_ERROR:
                if not self.shutting_down:
                    delay = http_retry_delay()
                    svclog.warning(
                        "Could not announce self to the master server, "
                        "internal server error: %s.  Retrying in %s seconds.",
                        response.data(), delay)
                    reactor.callLater(delay, response.request.retry)
                else:
                    svclog.warning("Could not announce to master. Not retrying "
                                   "because of pending shutdown")

            elif response.code == NOT_FOUND:
                self.reannounce_client_request = None
                svclog.warning("The master says it does not know about our "
                               "agent id. Posting as a new agent.")
                self.post_agent_to_master()

            # If this is a client problem retrying the request
            # is unlikely to fix the issue so we stop here
            elif response.code >= BAD_REQUEST:
                self.reannounce_client_request = None
                svclog.error(
                    "Failed to announce self to the master, bad "
                    "request: %s.  This request will not be retried.",
                    response.data())

            else:
                self.reannounce_client_request = None
                svclog.error(
                    "Unhandled error when posting self to the "
                    "master: %s (code: %s).  This request will not be "
                    "retried.", response.data(), response.code)

        # In the event of a hard failure, do not retry because we'll
        # be rerunning this code soon anyway.
        def errback(failure):
            self.reannounce_client_request = None
            svclog.error(
                "Failed to announce self to the master: %s.  This "
                "request will not be retried.", failure)

        self.reannounce_client_request = post(
            self.agent_api(),
            callback=callback, errback=errback,
            data={
                "state": config["state"],
                "current_assignments": config.get(
                    "current_assignments", {}),  # may not be set yet
                "free_ram": memory.free_ram()})

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
        config.register_callback("free_ram", self.callback_free_ram_changed)
        config.register_callback("cpus", self.callback_cpu_count_changed)
        return self.post_agent_to_master()

    def stop(self):
        """
        Internal code which stops the agent.  This will terminate any running
        processes, inform the master of the terminated tasks, update the
        state of the agent on the master.
        """
        svclog.info("Stopping the agent")

        # If this is the first time we're calling stop() then
        # setup a function call to remove the pidfile when the
        # process exits.
        if not self.shutting_down:
            self.shutting_down = True
            self.shutdown_timeout = (
                datetime.utcnow() + timedelta(
                    seconds=config["agent_shutdown_timeout"]))

            def remove_pidfile():
                if not isfile(config["agent_lock_file"]):
                    svclog.warning(
                        "%s does not exist", config["agent_lock_file"])
                    return

                try:
                    os.remove(config["agent_lock_file"])
                    svclog.debug(
                        "Removed pidfile %r", config["agent_lock_file"])
                except (OSError, IOError) as e:
                    svclog.error(
                        "Failed to remove lock file %r: %s",
                        config["agent_lock_file"], e)

            atexit.register(remove_pidfile)

            svclog.debug("Stopping execution of jobtypes")
            for uuid, jobtype in config["jobtypes"].items():
                jobtype.stop()

            def wait_on_stopped():
                svclog.info("Waiting on %s job types to terminate",
                            len(config["jobtypes"]))

                for jobtype_id, jobtype in config["jobtypes"].copy().items():
                    if not jobtype._has_running_processes():
                        svclog.error(
                            "%r has not removed itself, forcing removal",
                            jobtype)
                        config["jobtypes"].pop(jobtype_id)

                if config["jobtypes"]:
                    reactor.callLater(1, wait_on_stopped)
                elif self.agent_api() is not None:
                    self.post_shutdown_to_master()
                else:
                    reactor.stop()

            wait_on_stopped()

    def sigint_handler(self, *_):
        try:
            os.remove(config["run_control_file"])
        except (WindowsError, OSError, IOError) as e:
            if e.errno != ENOENT:
                svclog.error("Could not delete run control file %s: %s: %s",
                             config["run_control_file"],
                             type(e).__name__, e)

                def remove_run_control_file():
                    try:
                        os.remove(config["run_control_file"])
                        svclog.debug("Removed run control file %r",
                                     config["run_control_file"])
                    except (OSError, IOError, WindowsError) as e:
                        svclog.error("Failed to remove run control file %s: "
                                     "%s: %s",
                                    config["run_control_file"],
                                    type(e).__name__, e)

                atexit.register(remove_run_control_file)

        self.stop()

    def post_shutdown_to_master(self, stop_reactor=True):
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
            # NOTE: current_assignments *should* be empty now because the tasks
            # should shutdown first.  If not then we'll let the master know.
            def perform():
                return post(
                    self.agent_api(),
                    data={
                        "state": AgentState.OFFLINE,
                        "free_ram": memory.free_ram(),
                        "current_assignments": config["current_assignments"]},
                    callback=results_from_post,
                    errback=error_while_posting)

            return perform() if run else perform

        def results_from_post(response):
            if response.code == NOT_FOUND:
                svclog.warning(
                    "Agent %r no longer exists, cannot update state",
                    config["agent_id"])
                finished.callback(NOT_FOUND)

            elif response.code == OK:
                svclog.info(
                    "Agent %r has shutdown successfully", config["agent_id"])
                finished.callback(OK)

            elif response.code >= INTERNAL_SERVER_ERROR:
                if self.shutdown_timeout > datetime.utcnow():
                    delay = http_retry_delay()
                    svclog.warning(
                        "State update failed due to server error: %s.  "
                        "Retrying in %s seconds.",
                        response.data(), delay)
                    reactor.callLater(delay, response.request.retry)
                else:
                    svclog.warning(
                        "State update failed due to server error: %s.  "
                        "Shutdown timeout reached, not retrying.",
                        response.data())
                    finished.errback(INTERNAL_SERVER_ERROR)

            else:
                if self.shutdown_timeout > datetime.utcnow():
                    delay = http_retry_delay()
                    svclog.warning(
                        "State update failed due to unhandled error: %s.  "
                        "Retrying in %s seconds.",
                        response.data(), delay)
                    reactor.callLater(delay, response.request.retry)
                else:
                    svclog.warning(
                        "State update failed due to unhandled error: %s.  "
                        "Shutdown timeout reached, not retrying.",
                        response.data())
                    finished.errback(response.code)

        def error_while_posting(failure):
            if self.shutdown_timeout > datetime.utcnow():
                delay = http_retry_delay()
                svclog.warning(
                    "State update failed due to unhandled error: %s.  "
                    "Retrying in %s seconds",
                    failure, delay)
                reactor.callLater(delay, post_update(run=False))
            else:
                svclog.warning(
                    "State update failed due to unhandled error: %s.  "
                    "Shutdown timeout reached, not retrying.",
                    failure)
                finished.errback(failure)

        # Post our current state to the master.  We're only posting free_ram
        # and state here because all other fields would be updated the next
        # time the agent starts up.  free_ram would be too but having it
        # here is beneficial in cases where the agent terminated abnormally.
        post_update()

        if stop_reactor:
            finished.addBoth(lambda *_: reactor.stop())

        return finished

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
            if isinstance(failure, ConnectionRefusedError):
                svclog.error(
                    "Failed to POST agent to master, the connection was "
                    "refused. Retrying in %s seconds")
            else:  # pragma: no cover
                svclog.error(
                    "Unhandled error when trying to POST the agent to the "
                    "master. The error was %s.", failure)

            if not self.shutting_down:
                delay = http_retry_delay()
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
        left_till_update = \
            config["agent_ram_max_report_frequency"] - since_last_update

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
            if abs(new_value - old_value) < config["agent_ram_report_delta"]:
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
                data={"cpus": config["agent_cpus"]},
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
