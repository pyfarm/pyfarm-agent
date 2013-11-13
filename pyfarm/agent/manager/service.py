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

import logging
import socket
from functools import partial

from zope.interface import implementer
from twisted.python import log, usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker, MultiService

try:
    import json
except ImportError:
    import simplejson as json

from pyfarm.core.utility import convert
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


def convert_option_ston(key, value):
    # special "infinite" value reserved for some flags
    if (isinstance(value, basestring) and key == "http-max-retries"
            and value.lower() in ("inf", "infinite", "unlimited")):
        return float("inf")

    try:
        return convert.ston(value)

    except ValueError:
        raise usage.UsageError(
            "--%s requires a number but got %s" % (key, repr(value)))


def convert_option_projects(key, value):
    return filter(bool, map(str.strip, value.split(",")))


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
         "the password to use to connect to the master's REST api.  The "
         "default is communication without authentication."),
        ("http-api-port", "", 5000,
         "Default port the restfull HTTP api runs on.  By default this value "
         "is combined with --master but could also be combined with the "
         "alternate --http-api-server"),
        ("http-api-scheme", "", "http",
         "The scheme to use to communicate over http.  Valid values are "
         "'http' or 'https'"),

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
         "the max number of times to retry a request back to the master"),
        ("http-retry-delay", "", 3,
         "if a http request back to the master has failed, wait this amount of "
         "time before trying again"),

        # local agent settings which control some resources
        # and restrictions
        ("memory-check-interval", "", 10,
         "how often swap and ram resources should be checked and sent to the "
         "master"),
        ("projects", "", "*",
         "A comma separated list of projects this agent is allowed to do work "
         "for.  Note however that this only updates the master at the time "
         "the agent is run.  Once the agent has been launched all further "
         "'membership' is present in the database and acted on by the queue.")]

    # special variable used for inferring type in makeService
    optConverters = {
        "port": convert_option_ston,
        "memory-check-interval": convert_option_ston,
        "http-max-retries": convert_option_ston,
        "http-retry-delay": convert_option_ston,
        "http-api-port": convert_option_ston,
        "statsd-port": convert_option_ston,
        "projects": convert_option_projects}
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

    def __init__(self, config):
        self.config.update(config)
        self.scheduled_tasks = ScheduledTaskManager()
        self.log = partial(log.msg, system=self.__class__.__name__)

        # register any scheduled tasks
        self.scheduled_tasks.register(
            memory_utilization, self.config["memory-check-interval"])

        # finally, setup the base class
        MultiService.__init__(self)

    def startService(self):
        self.log("informing master of agent startup", level=logging.INFO)

        def start_service(response):
            print "start_service ===========",response
            #print dir(response)
            #from twisted.internet import defer
            #response_body = http.StringReceiver(defer.Deferred())
            #response.delieverBody(lambda: True)
            self.log("connected to master")
            self.scheduled_tasks.start()

        def failure(error):
            print "FAIL", error.value

        # prepare a retry request to POST to the master
        retry_post_agent = RetryDeferred(
            http_post, start_service, failure,
            max_retries=self.config["http-max-retries"],
            timeout=None,
            retry_delay=self.config["http-retry-delay"])

        retry_post_agent(
            self.config["http-api"] + "/", data=json.dumps({"foo": True}))

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
            config["http-api"] = (
                config["http-api-scheme"] +
                "://%s" % ":".join([http_server, str(config["http-api-port"])]))

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
