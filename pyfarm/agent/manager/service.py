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
===============

Sends and receives information from the master and performs systems level tasks
such as log reading, system information gathering, and management of processes.
"""

import os
import time
import logging
import socket
from pprint import pformat
from functools import partial

import ntplib
from zope.interface import implementer
from twisted.python import log, usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker, MultiService

try:
    import json
except ImportError:
    import simplejson as json

from pyfarm.core.enums import UseAgentAddress
from pyfarm.core.utility import convert
from pyfarm.core.sysinfo import network, memory, cpu
from pyfarm.agent.http.client import post as http_post
from pyfarm.agent.utility.retry import RetryDeferred
from pyfarm.agent.manager.tasks import memory_utilization
from pyfarm.agent.utility.tasks import ScheduledTaskManager


def check_address(value):
    # is this a valid ip address?
    try:
        socket.inet_aton(value)
        return value

    except socket.error:
        pass

    # could we map the hostname to an address?
    try:
        socket.gethostbyname(value)
        return value

    except socket.gaierror:
        raise usage.UsageError("failed to resolve %s to an address" % value)


def convert_option_ston(key, value, types=None):
    # special "infinite" value reserved for some flags
    if (isinstance(value, basestring) and key == "http-max-retries"
            and value.lower() in ("inf", "infinite", "unlimited")):
        return float("inf")

    try:
        if types is None:
            value = convert.ston(value)
        else:
            value = convert.ston(value, types=types)

        if key == "ram":
            value = int(value)

    except ValueError:
        raise usage.UsageError(
            "--%s requires a number but got %s" % (key, repr(value)))

    return value

convert_option_stoi = partial(convert_option_ston, types=int)


def convert_option_projects(key, value):
    return filter(bool, map(str.strip, value.split(",")))


def convert_option_contact_addr(key, value):
    value = value.lower()
    mappings = {
        "hostname": UseAgentAddress.HOSTNAME,
        "ip": UseAgentAddress.LOCAL,
        "remote-ip": UseAgentAddress.REMOTE}

    if value not in mappings:
        usage.UsageError(
            "invalid value for --%s, valid values are %s" % (
                key, mappings.keys()))
    else:
        return mappings[value]


class Options(usage.Options):
    optParameters = [
        # local 'server' settings
        ("port", "p", 50000,
         "The port which the master should use to talk back to the agent."),

        # agent -> master communications
        ("master", "", None,
         "The master server's hostname or address.  If no other options are "
         "provided to describe the location of specific services then the "
         "resource urls will be built off of this address."),
        ("http-auth-user", "", None,
         "the user to use for connecting to the master's REST api.  The "
         "default is communication without authentication."),
        ("http-auth-password", "", None,
         "The password to use to connect to the master's REST api.  The "
         "default is communication without authentication."),
        ("http-api-port", "", 5000,
         "Default port the restfull HTTP api runs on.  By default this value "
         "is combined with --master but could also be combined with the "
         "alternate --http-api-server"),
        ("http-api-scheme", "", "http",
         "The scheme to use to communicate over http.  Valid values are "
         "'http' or 'https'"),
        ("http-api-prefix", "", "/api/v1",
         "The prefix for accessing the http api on the master, this does not"
         "include the server, scheme, port, etc"),

        # statsd setup
        ("statsd-port", "", 8125,
         "Default port that statsd runs on.  By default this value is combined "
         "with --master but could also be combined with the alternate "
         "--statsd-server"),
        ("http-api-server", "", None,
         "Alternate server which is running the restfull HTTP server.  This "
         "will replace the value provided by the --master flag."),
        ("statsd-server", "", None,
         "Alternate server which is running the statsd service.  This will "
         "replace the value provided by the --master flag."),

        # http retries/detailed configuration
        ("http-max-retries", "", "unlimited",
         "The max number of times to retry a request back to the master"),
        ("http-retry-delay", "", 3,
         "If a http request back to the master has failed, wait this amount of "
         "time before trying again"),

        # information about this agent which we send to the master
        # before starting the main service code
        ("hostname", "", network.hostname(),
         "The agent's hostname to send to the master"),
        ("ip", "", network.ip(),
         "The agent's local ip address to send to the master"),
        ("remote-ip", "", "",
         "The remote ip address to report, this may be different than"
         "--ip"),
        ("contact-address", "", "hostname",
         "Which address the master should use when talking back to an agent.  "
         "Valid options are 'hostname', 'ip', and 'remote-ip'"),
        ("ram", "", memory.TOTAL_RAM,
         "The total amount of ram installed on the agent which will be"
         "sent to the master"),
        ("cpus", "", cpu.NUM_CPUS,
         "The number of cpus this agent has which will be sent to the master."),

        # TODO: add *_allocation columns

        # local agent settings which control some resources
        # and restrictions
        ("memory-check-interval", "", 10,
         "how often swap and ram resources should be checked and sent to the "
         "master"),
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
         "newer version.  The default value should generally be used.")]

    # special variable used for inferring type in makeService
    optConverters = {
        "port": convert_option_stoi,
        "memory-check-interval": convert_option_ston,
        "http-max-retries": convert_option_stoi,
        "http-retry-delay": convert_option_ston,
        "http-api-port": convert_option_stoi,
        "statsd-port": convert_option_stoi,
        "projects": convert_option_projects,
        "contact-address": convert_option_contact_addr,
        "ram": convert_option_ston,
        "cpus": convert_option_stoi,
        "ntp-server-version": convert_option_stoi}
#
#class IPCReceieverFactory(Factory):
#    """
#    Receives incoming connections and hands them off to the protocol
#    object.  In addition this class will also keep a list of all hosts
#    which have connected so they can be notified upon shutdown.
#    """
#    protocol = IPCProtocol
#    known_hosts = set()
#
#    def stopFactory(self):  # TODO: notify all connected hosts we are stopping
#        if self.known_hosts:
#            log.msg("notifying all known hosts of termination")


class ManagerService(MultiService):
    """the service object itself"""
    config = {}
    ntp_client = ntplib.NTPClient()

    def __init__(self, config):
        self.config.update(config)
        self.scheduled_tasks = ScheduledTaskManager()
        self.log = partial(log.msg, system=self.__class__.__name__)
        self.err = partial(log.err, system=self.__class__.__name__)

        # register any scheduled tasks
        self.scheduled_tasks.register(
            memory_utilization, self.config["memory-check-interval"])

        # finally, setup the base class
        MultiService.__init__(self)

    def _startServiceCallback(self, response):
        """internal callback used to start the service itself"""
        self.log("starting service")
        self.log("agent database entry: %s%s" % (os.linesep, pformat(response)))
        self.scheduled_tasks.start()

    def _failureCallback(self, response):
        """internal callback which is run when the service fails to start"""
        self.err(response)

    def startService(self):
        self.log("informing master of agent startup", level=logging.INFO)
        self.log("%s" % pformat(self.config), level=logging.DEBUG,
                 system="%s.config" % self.__class__.__name__)

        def get_agent_data():
            ntp_client = ntplib.NTPClient()
            try:
                pool_time = ntp_client.request(self.config["ntp-server"])
                time_offset = int(pool_time.tx_time - time.time())

            except Exception, e:
                time_offset = 0
                self.log("failed to determine network time: %s" % e,
                         level=logging.WARNING)
            else:
                if time_offset:
                    self.log("agent time offset is %s" % time_offset,
                             level=logging.INFO)

            data = {
                "hostname": self.config["hostname"],
                "ip": self.config["ip"],
                "use_address": self.config["contact-address"],
                "ram": self.config["ram"],
                "cpus": self.config["cpus"],
                "port": self.config["port"],
                "free_ram": memory.ram_free(),
                "time_offset": time_offset}

            if self.config["remote-ip"]:
                data.update(remote_ip=self.config["remote-ip"])

            if self.config["projects"]:
               data.update(projects=self.config["projects"])

            return data

        # prepare a retry request to POST to the master
        retry_post_agent = RetryDeferred(
            http_post, self._startServiceCallback, self._failureCallback,
            headers={"Content-Type": "application/json"},
            max_retries=self.config["http-max-retries"],
            timeout=None,
            retry_delay=self.config["http-retry-delay"],)

        retry_post_agent(
            self.config["http-api"] + "/agents",
            data=json.dumps(get_agent_data()))

    def stopService(self):
        self.scheduled_tasks.stop()
        self.log("informing master of agent shutdown", level=logging.INFO)


@implementer(IServiceMaker, IPlugin)
class ManagerServiceMaker(object):
    """
    Main service which which serves runs PyFarm's agent which consists
    of and HTTP REST api and process management/monitoring.
    """
    tapname = "pyfarm.agent"
    description = __doc__
    options = Options

    def makeService(self, options):
        config = {}

        # convert all incoming options to values we can use
        for key, value in options.items():
            if value is not None and key in options.optConverters:
                value = options.optConverters[key](key, value)

            config[key] = value

        # set or raise error about missing http api server
        http_server = config.get("http-api-server") or config.get("master")
        if http_server is None:
            raise usage.UsageError(
                "--master or --http-api-server must be provided")
        else:
            # make sure the http scheme is set properly
            if config["http-api-scheme"] not in ("http", "https"):
                raise usage.UsageError(
                    "valid schemes for --http-api-scheme are 'http' or 'https'")

            check_address(http_server)

        config["http-api"] = "%(scheme)s://%(server)s:%(port)s%(prefix)s" % {
            "scheme": config["http-api-scheme"],
            "server": http_server,
            "port": str(config["http-api-port"]),
            "prefix": config["http-api-prefix"]}


        # set or raise error about missing statstd server
        statsd_server = config.get("statsd-server") or config.get("master")
        if statsd_server is None:
            raise usage.UsageError(
                "--master or --statsd-server must be provided")
        else:
            check_address(statsd_server)
            config["statsd"] = ":".join(
                [statsd_server, str(config["statsd-port"])])

        service = ManagerService(config)

        # ipc service setup
        #ipc_factory = IPCReceieverFactory()
        #ipc_server = internet.TCPServer(service.config.get("ipc-port"),
        #                                ipc_factory)
        #ipc_server.setServiceParent(service)

        return service
