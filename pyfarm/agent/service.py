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
import logging
from functools import partial
from os.path import join, abspath, dirname

import ntplib
import requests
from twisted.python import log, usage
from twisted.application.service import MultiService

try:
    import json
except ImportError:
    import simplejson as json


from pyfarm.core.enums import AgentState
from pyfarm.core.utility import convert
from pyfarm.core.sysinfo import memory
from pyfarm.agent.http.client import post as http_post
from pyfarm.agent.utility.retry import RetryDeferred
from pyfarm.agent.tasks import ScheduledTaskManager, memory_utilization
from pyfarm.agent.process.manager import ProcessManager
from pyfarm.agent.config import config

# determine template location
import pyfarm.agent
TEMPLATE_ROOT = abspath(join(dirname(pyfarm.agent.__file__), "templates"))
STATIC_ROOT = abspath(join(dirname(pyfarm.agent.__file__), "static"))


def convert_option_ston(key, value, types=None):
    # special "infinite" value reserved for some flags
    if (isinstance(value, basestring) and key == "http-max-retries"
            and value.lower() in ("inf", "infinite", "unlimited")):
        return float("inf")

    try:
        # NOTE: some values are not covered by tests
        # because they are covered in a base module
        try:  # pragma: no cover
            if types is None:
                value = convert.ston(value)
            else:
                value = convert.ston(value, types=types)

        except SyntaxError, e:
            raise ValueError(e)

        if key == "ram":
            value = int(value)

    except ValueError:
        raise usage.UsageError(
            "--%s requires a number but got %s" % (key, repr(value)))

    return value

convert_option_stoi = partial(convert_option_ston, types=int)


def convert_option_projects(_, value):
    return filter(bool, map(str.strip, value.split(",")))


class Options(usage.Options):  # pragma: no cover
    optParameters = [
        # http retries/detailed configuration
        ("http-max-retries", "", "unlimited",
         "The max number of times to retry a request back to the master"),
        ("http-retry-delay", "", 3,
         "If a http request back to the master has failed, wait this amount of "
         "time before trying again"),

        # local agent settings which control some resources
        # and restrictions
        ("memory-check-interval", "", 10,
         "how often swap and ram resources should be checked and sent to the "
         "master"),
        ("ram-report-delta", "", 75,
         "If the amount of ram in use changes by this amount in megabytes "
         "the the change will be reported to the master"),
        ("swap-report-delta", "", 25,
         "If the amount of swap in use changes by this amount in megabytes "
         "the change will be reported to the master"),
        ("ram-record-delta", "", 25,
         "If the amount of ram in use changes by this amount in megabytes "
         "the the change will recorded to the local datastore"),
        ("swap-record-delta", "", 10,
         "If the amount of swap in use changes by this amount in megabytes "
         "the change will recorded to the local datastore"),
        ("projects", "", "",
         "A comma separated list of projects this agent is allowed to do work "
         "for.  Note however that this only updates the master at the time "
         "the agent is run.  Once the agent has been launched all further "
         "'membership' is present in the database and acted on by the queue.  "
         "By default, an agent can execute work for any project."),
        ("ntp-server", "", "pool.ntp.org",
         "The default network time server this agent should query to "
         "retrieve the real time.  This will be used to help determine the "
         "agent's click skew (if any)."),
        ("ntp-server-version", "", 2,
         "The version of the NTP server in case it's running an older or "
         "newer version.  The default value should generally be used."),
        ("html-templates", "", TEMPLATE_ROOT,
         "The default location where the local web service should serve "
         "html templates from."),
        ("static-files", "", STATIC_ROOT,
        "The default location where the agent should find static files"
        "for the local web service.")]

    optFlags = [
        ("pretty-json", "",
         "If provided then all json output is human readable")]

    # special variable used for inferring type in makeService
    optConverters = {
        "memory-check-interval": convert_option_ston,
        "http-max-retries": convert_option_stoi,
        "http-retry-delay": convert_option_ston,
        "http-api-port": convert_option_stoi,
        "projects": convert_option_projects,
        "ntp-server-version": convert_option_stoi,
        "ram-report-delta": convert_option_stoi,
        "swap-report-delta": convert_option_stoi,
        "ram-record-delta": convert_option_stoi,
        "swap-record-delta": convert_option_stoi}


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

    try:
        pool_time = ntp_client.request(config["ntp-server"])
        time_offset = int(pool_time.tx_time - time.time())

    except Exception, e:
        time_offset = 0
        log.msg(
            "failed to determine network time: %s" % e,
            level=logging.WARNING)
    else:
        if time_offset:
            log.msg(
                "agent time offset is %s" % time_offset,
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


def agent():
    def initial_post_success(response):
        """internal callback used to start the service itself"""
        config["agent-id"] = response["id"]
        config["agent-url"] = \
            config["http-api"] + "/agents/%s" % response["id"]

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

    agent_data = get_agent_data()

