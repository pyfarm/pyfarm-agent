# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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
Commands
========

The main module which constructs the entrypoint(s) for the agent.
"""

import argparse
import os
import time
from functools import partial
from os.path import abspath, dirname, isfile, join

# Platform specific imports.  These should either all fail or
# import without problems so we're grouping them together.
try:
    from grp import getgrgid
    from pwd import getpwuid
    from os import setuid, getuid, setgid, getgid, fork
except ImportError:  # pragma: no cover
    getgrgid = NotImplemented
    getpwuid = NotImplemented
    setuid = NotImplemented
    getuid = NotImplemented
    setgid = NotImplemented
    getgid = NotImplemented
    fork = NotImplemented

import psutil
import requests
from requests import ConnectionError

from pyfarm.core.enums import OS, WINDOWS, UseAgentAddress, AgentState
from pyfarm.core.sysinfo import user, network, memory, cpu

# start logging before doing anything else
from pyfarm.agent.logger import getLogger, start_logging
start_logging()

from pyfarm.agent.config import config
from pyfarm.agent.entrypoints.argtypes import (
    ip, port, uidgid, direxists, enum, integer, number)
from pyfarm.agent.entrypoints.utility import (
    get_pids, start_daemon_posix, write_pid_file, get_default_ip)


logger = getLogger("agent.cmd")

# determine template and static file location
import pyfarm.agent
TEMPLATE_ROOT = abspath(
    join(dirname(pyfarm.agent.__file__), "http", "templates"))
STATIC_ROOT = abspath(
    join(dirname(pyfarm.agent.__file__), "http", "static"))


class AgentEntryPoint(object):
    """Main object for parsing command line options"""
    def __init__(self):
        self.args = None
        self.parser = argparse.ArgumentParser(
            usage="%(prog)s [status|start|stop]",
            epilog="%(prog)s is a command line client for working with a "
                   "local agent.  You can use it to stop, start, and report "
                   "the general status of a running agent process.")

        # main subparser for start/stop/status/etc
        subparsers = self.parser.add_subparsers(
            help="individual operations %(prog)s can run")
        start = subparsers.add_parser(
            "start", help="starts the agent")
        stop = subparsers.add_parser(
            "stop", help="stops the agent")
        status = subparsers.add_parser(
            "status", help="query the 'running' state of the agent")

        # relate a name and function to each subparser
        start.set_defaults(target_name="start", target_func=self.start)
        stop.set_defaults(target_name="stop", target_func=self.stop)
        status.set_defaults(target_name="status", target_func=self.status)

        # command line flags which configure the agent's network service
        global_network = self.parser.add_argument_group(
            "Agent Network Service",
            description="Main flags which control the network services running "
                        "on the agent.")
        global_network.add_argument(
            "--ip", default=get_default_ip(), type=partial(ip, instance=self),
            help="The IPv4 address which the agent will report to the "
                 "master [default: %(default)s]")
        global_network.add_argument(
            "--port", default=50000,
            type=partial(port, instance=self),
            help="The port number which the gent is either running on or "
                 "will started on.  This port is also reported the master "
                 "when an agent starts. [default: %(default)s]")
        global_network.add_argument(
            "--agent-api-username", default="agent",
            help="The username required to access or manipulate the agent "
                 "using REST. [default: %(default)s]")
        global_network.add_argument(
            "--agent-api-password", default="agent",
            help="The password required to access manipulate the agent "
                 "using REST. [default: %(default)s]")

        # defaults for a couple of the command line flags below
        self.master_api_default = "http://%(master)s/api/v1"

        # command line flags for the connecting the master apis
        global_apis = self.parser.add_argument_group(
            "Network Resources",
            description="Resources which the agent will be communicating with.")
        global_apis.add_argument(
            "--master",
            help="This is a convenience flag which will allow you to set the "
                 "hostname for the master.  By default this value will be "
                 "substituted in --master-api")
        global_apis.add_argument(
            "--master-api", default=self.master_api_default,
            help="The location where the master's REST api is located. "
                 "[default: %(default)s]")

        # global command line flags which apply to top level
        # process control
        global_process = self.parser.add_argument_group(
            "Process Control",
            description="These settings apply to the parent process of the "
                        "agent and contribute to allowing the process to run "
                        "as other users or remain isolated in an environment. "
                        "They also assist in maintaining the 'running state' "
                        "via a process id file.")
        global_process.add_argument(
            "--pidfile", default="pyfarm-agent.pid",
            help="The file to store the process id in, defaulting to "
                 "%(default)s.  Any provided path will be fully resolved "
                 "using os.path.abspath prior to running an operation such "
                 "as `start`")
        global_process.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            help="If provided then do not run the process in the background.")
        global_process.add_argument(
            "--chroot",
            type=partial(direxists, instance=self, flag="chroot"),
            help="The directory to chroot the agent do upon launch.  This is "
                 "an optional security measure and generally is not ")
        global_process.add_argument(
            "--uid",
            type=partial(
                uidgid, flag="uid",
                get_id=getuid, check_id=getpwuid, set_id=setuid, instance=self),
            help="The user id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        global_process.add_argument(
            "--gid",
            type=partial(
                uidgid, flag="uid",
                get_id=getgid, check_id=getgrgid, set_id=setgid, instance=self),
            help="The group id to run the agent as.  *This setting is "
                 "ignored on Windows.*")

        # start general group
        start_general_group = start.add_argument_group(
            "General Configuration",
            description="These flags configure parts of the agent related to "
                        "hardware, state, and certain timing and scheduling "
                        "attributes.")
        start_general_group.add_argument(
            "--projects", default=[], nargs="+",
            help="The project or projects this agent is dedicated to.  By "
                 "default the agent will service any project however specific "
                 "projects may be specified.  For example if you wish this "
                 "agent to service 'Foo Part I' and 'Foo Part II' only just "
                 "specify it as `--projects \"Foo Part I\" \"Foo Part II\"`")
        start_general_group.add_argument(
            "--state", default=AgentState.ONLINE,
            type=partial(enum, instance=self, enum=AgentState, flag="state"),
            help="The current agent state, valid values are "
                 "" + str(list(AgentState)) + ". [default: %(default)s]")
        start_general_group.add_argument(
            "--time-offset",
            type=partial(integer, instance=self, flag="time-offset", min_=0),
            help="If provided then don't talk to the NTP server at all to "
                 "calculate the time offset.  If you know for a fact that this "
                 "host's time is always up to date then setting this to 0 is "
                 "probably a safe bet.")
        start_general_group.add_argument(
            "--ntp-server", default="pool.ntp.org",
            help="The default network time server this agent should query to "
                 "retrieve the real time.  This will be used to help determine "
                 "the agent's clock skew if any.  Setting this value to '' "
                 "will effectively disable this query. [default: %(default)s]")
        start_general_group.add_argument(
            "--ntp-server-version", default=2,
            type=partial(integer, instance=self, flag="ntp-server-version"),
            help="The version of the NTP server in case it's running an older"
                 "or newer version. [default: %(default)s]")
        start_general_group.add_argument(
            "--no-pretty-json", default=True, action="store_false",
            dest="pretty_json",
            help="If provided do not dump human readable json via the agent's "
                 "REST api")

        # start hardware group
        start_hardware_group = start.add_argument_group(
            "Physical Hardware",
            description="Command line flags which describe the hardware of "
                        "the agent.")
        start_hardware_group.add_argument(
            "--cpus", default=int(cpu.total_cpus()),
            type=partial(integer, instance=self, flag="cpus"),
            help="The total amount of cpus installed on the "
                 "system [default: %(default)s]")
        start_hardware_group.add_argument(
            "--ram", default=int(memory.total_ram()),
            type=partial(integer, instance=self, flag="ram"),
            help="The total amount of ram installed on the system in "
                 "megabytes.  [default: %(default)s]")
        start_hardware_group.add_argument(
            "--swap", default=int(memory.total_swap()),
            type=partial(integer, instance=self, flag="swap"),
            help="The total amount of swap installed on the system in "
                 "megabytes.  [default: %(default)s]")

        # start interval controls
        start_interval_group = start.add_argument_group(
            "Interval Controls",
            description="Controls which dictate when certain internal "
                        "intervals should occur.")
        start_interval_group.add_argument(
            "--memory-check-interval", default=10,
            type=partial(integer, instance=self, flag="memory-check-interval"),
            help="How often swap and ram resources should be checked for "
                 "changes. Ram and swap are also checked after certain events "
                 "too such as when a job starts or errors out. [default: "
                 "%(default)s]")
        start_interval_group.add_argument(
            "--ram-report-delta", default=100,
            type=partial(integer, instance=self, flag="ram-report-delta"),
            help="Only report a change in ram if the value has changed "
                 "at least this many megabytes. [default: %(default)s]")
        start_interval_group.add_argument(
            "--swap-report-delta", default=100,
            type=partial(integer, instance=self, flag="ram-report-delta"),
            help="Only report a change in swap if the value has changed "
                 "at least this many megabytes. [default: %(default)s]")


        # start logging options
        logging_group = start.add_argument_group(
            "Logging Options",
            description="Settings which control logging of the agent's parent "
                        "process and/or any subprocess it runs.")
        logging_group.add_argument(
            "--log", default="pyfarm-agent.log",
            help="If provided log all output from the agent to this path.  "
                 "This will append to any existing log data.  [default: "
                 "%(default)s]")
        logging_group.add_argument(
            "--logerr",
            help="If provided then split any output from stderr into this file "
                 "path, otherwise send it to the same file as --log.")

        # network options for the agent when start is called
        start_network = start.add_argument_group(
            "Network Service",
            description="Controls how the agent is seen or interacted with "
                        "by external services such as the master.")
        start_network.add_argument(
            "--ip-remote", type=partial(ip, instance=self),
            help="The remote IPv4 address to report.  In situation where the "
                 "agent is behind a firewall this value will typically be "
                 "different.")
        start_network.add_argument(
            "--use-address", default=UseAgentAddress.REMOTE,
            type=partial(
                enum, instance=self, flag="use-address",
                enum=UseAgentAddress),
            help="The default way the master should contact the agent.  The "
                 "default is '%(default)s' but it could be any "
                 "of " + str(list(UseAgentAddress)))
        start_network.add_argument(
            "--hostname", default=network.hostname(),
            help="The agent's hostname to send to the master "
                 "[default: %(default)s]")

        # various options for how the agent will interact with the
        # master server
        start_http_group = start.add_argument_group(
            "HTTP Configuration",
            description="Options for how the agent will interact with the "
                        "master's REST api and how it should run it's own "
                        "REST api.")
        start_http_group.add_argument(
            "--html-templates", default=TEMPLATE_ROOT,
            type=partial(direxists, instance=self, flag="html-templates"),
            help="The default location where the agent's http server should "
                 "find the html templates. [default: %(default)s]")
        start_http_group.add_argument(
            "--html-templates-reload", default=False,
            action="store_true",
            help="If provided then force Jinja2, the html template system, "
                 "to check the file system for changes with every request. "
                 "This flag should not be used in production but is useful "
                 "for development and debugging purposes.")
        start_http_group.add_argument(
            "--static-files", default=STATIC_ROOT,
            type=partial(direxists, instance=self, flag="static-files"),
            help="The default location where the agent's http server should "
                 "find static files to serve. [default: %(default)s]")
        start_http_group.add_argument(
            "--http-max-retries", default="unlimited",
            type=partial(integer, instance=self, allow_inf=True, min_=0),
            help="The max number of times to retry a request to the master "
                 "after it has failed.  [default: %(default)s]")
        start_http_group.add_argument(
            "--http-retry-delay", default=3,
            type=partial(number, instance=self),
            help="If a http request to the master has failed, wait this amount "
                 "of time before trying again")

        # start_ram_group = start.add_argument_group("Memory")

        # options when stopping the agent
        stop_process_group = stop.add_argument_group(
            "Process Control",
            description="Flags that control how the agent is shutdown")
        stop_process_group.add_argument(
            "--force", default=False, action="store_true",
            help="Ignore any previous errors not covered by other flags and "
                 "terminate or restart the process.")
        stop_process_group.add_argument(
            "--no-wait", default=False, action="store_true",
            help="If provided then don't wait on the agent to shut itself "
                 "down.  By default we would want to wait on each task to stop "
                 "so we can catch any errors and then finally wait on the "
                 "agent to shutdown too.  If you're in a hurry or stopping a "
                 "bunch of agents at once then setting this flag will let the "
                 "agent continue to stop itself without providing feedback "
                 "directly.")
        stop_process_group.add_argument(
            "--ignore-pid-mismatch", default=False, action="store_true",
            help="If provided then any discrepancies between the pid reported "
                 "by the agent process itself and the pid file will be "
                 "ignored. This will cause stop to only produce a "
                 "warning and the continue on by trusting the pid provided by "
                 "the running agent over the value in the pid file.")

    def __call__(self):
        self.args = self.parser.parse_args()

        if not self.args.master:
            self.parser.error(
                "--master must be provided (ex. "
                "'pyfarm-agent --master=foobar start')")

        # replace %(master)s in --master-api if --master-api was not set
        if self.args.master_api == self.master_api_default:
            self.args.master_api = self.args.master_api % {
                "master": self.args.master}

        # if we're on windows, produce some warnings about
        # flags which are not supported
        if WINDOWS and self.args.uid:
            logger.warning("--uid is not currently supported on Windows")

        if WINDOWS and self.args.gid:
            logger.warning("--gid is not currently supported on Windows")

        if WINDOWS and self.args.no_daemon:
            logger.warning("--no-daemon is not currently supported on Windows")

        if self.args.target_name == "start":
            # if --logerr was not set then we have to set it
            # to --log's value here
            if self.args.logerr is None:
                self.args.logerr = self.args.log

            # since the agent process could fork we must make
            # sure the log file paths are fully specified
            self.args.log = abspath(self.args.log)
            self.args.logerr = abspath(self.args.logerr)
            self.args.pidfile = abspath(self.args.pidfile)
            self.args.html_templates = abspath(self.args.html_templates)
            self.args.static_files = abspath(self.args.static_files)

            if self.args.chroot is not None:
                self.args.chroot = abspath(self.args.chroot)

            # update configuration with values from the command line
            config_flags = {
                "master-api": self.args.master_api,
                "hostname": self.args.hostname,
                "ip": self.args.ip,
                "port": self.args.port,
                "use-address": self.args.use_address,
                "state": self.args.state,
                "ram": self.args.ram,
                "swap": self.args.swap,
                "cpus": self.args.cpus,
                "projects": list(set(self.args.projects)),
                "http-max-retries": self.args.http_max_retries,
                "http-retry-delay": self.args.http_retry_delay,
                "memory-check-interval": self.args.memory_check_interval,
                "ram-report-delta": self.args.ram_report_delta,
                "swap-report-delta": self.args.swap_report_delta,
                "static-files": self.args.static_files,
                "html-templates": self.args.html_templates,
                "html-templates-reload": self.args.html_templates_reload,
                "ntp-server": self.args.ntp_server,
                "ntp-server-version": self.args.ntp_server_version,
                "time-offset": self.args.time_offset,
                "pretty-json": self.args.pretty_json,
                "api_endpoint_prefix": "/api/v1"}

            config.update(config_flags)

        self.agent_api = "http://%s:%s/" % (self.args.ip, self.args.port)
        self.args.target_func()

    def start(self):
        # make sure the agent is not already running
        if any(get_pids(self.args.pidfile, self.agent_api)):
            logger.error("agent appears to be running")
            return

        logger.info("starting agent")

        # create the directory for the stdout log
        if not self.args.no_daemon and not isfile(self.args.log):
            try:
                os.makedirs(dirname(self.args.log))
            except OSError:
                logger.warning(
                    "failed to create %s" % dirname(self.args.log))

        # create the directory for the stderr log
        if all([not self.args.no_daemon,
                self.args.logerr != self.args.log,
                not isfile(self.args.logerr)]):
            try:
                os.makedirs(dirname(self.args.logerr))
            except OSError:
                logger.warning(
                    "failed to create %s" % dirname(self.args.logerr))

        # so long as fork could be imported and --no-daemon was not set
        # then setup the log files
        if not self.args.no_daemon and fork is not NotImplemented:
            logger.info("sending stdout to %s" % self.args.log)
            logger.info("sending stderr to %s" % self.args.logerr)
            start_daemon_posix(
                self.args.log, self.args.logerr, self.args.chroot,
                self.args.uid, self.args.gid)

        elif not self.args.no_daemon and fork is NotImplemented:
            logger.warning(
                "`fork` is not implemented on %s, starting in "
                "foreground" % OS.title())

        pid = os.getpid()
        write_pid_file(self.args.pidfile, pid)
        logger.info("pid: %s" % pid)

        if getuid is not NotImplemented:
            logger.info("uid: %s" % getuid())

        if getgid is not NotImplemented:
            logger.info("gid: %s" % getgid())

        from twisted.internet import reactor
        from pyfarm.agent.service import Agent

        service = Agent()
        service.run()

        reactor.run()

    def stop(self):
        logger.info("stopping agent")
        file_pid, http_pid = get_pids(self.args.pidfile, self.agent_api)

        # the pids, process object, and child jobs were not found
        # by any means
        if file_pid is None and http_pid is None:
            logger.info("agent is not running")
            return

        # check to see if the two pids we have are different
        pid = file_pid
        if all([file_pid is not None, http_pid is not None,
                file_pid != http_pid]):
            message = (
                "The pid on disk is %s but the pid the agent is reporting "
                "is %s." % (file_pid, http_pid))

            if not self.args.ignore_pid_mismatch:
                logger.warning(message)
                pid = http_pid

            else:
                message += ("  Normally this is because the pid file is either "
                            "stale or became outdated for some reason.  If "
                            "this is the case you may relaunch with the "
                            "--ignore-pid-mismatch flag.")
                logger.error(message)
                return

        # since the http server did give us a pid we assume we can post
        # the shutdown request directly to it
        if http_pid:
            url = self.agent_api + "shutdown?wait=%s" % self.args.no_wait
            logger.debug("POST %s" % url)
            logger.info("requesting agent shutdown over REST api")

            try:
                response = requests.post(
                    url,
                    auth=(self.args.api_username, self.args.api_password),
                    data={"user": user.username(), "time": time.time()},
                    headers={"Content-Type": "application/json"})
                logger.debug("text: %s" % response.data)

            except ConnectionError as e:
                logger.traceback(e)
                logger.error("failed to POST our request to the agent")
            else:
                if not response.ok:
                    logger.error(
                        "response to shutdown was %s" % repr(response.reason))
                else:
                    if not self.args.no_wait:
                        logger.info("agent has shutdown")

                    # remove the pid file
                    try:
                        os.remove(self.args.pidfile)
                        logger.debug("removed %s" % self.args.pidfile)
                    except OSError:
                        logger.warning(
                            "failed to remove %s" % self.args.pidfile)

        else:
            if not self.args.force:
                logger.error(
                    "The agent's REST interface is either down or the agent "
                    "did not respond to our request.  If you still wish to "
                    "terminate process %s use --force" % pid)
                return

            logger.warning("using force to terminate process %s" % pid)

            try:
                process = psutil.Process(pid)
            except psutil.NoSuchProcess:
                logger.error("no such process %s" % pid)
            else:
                process.terminate()

                # remove the pid file
                try:
                    os.remove(self.args.pidfile)
                    logger.debug("removed %s" % self.args.pidfile)
                except OSError:
                    logger.warning(
                        "failed to remove %s" % self.args.pidfile)

    def status(self):
        logger.info("checking status")
        if any(get_pids(self.args.pidfile, self.agent_api)):
            logger.error("agent appears to be running")
            return True
        else:
            return False

