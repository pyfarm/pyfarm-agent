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
Main
~~~~

The main module which constructs the entrypoint for ``pyfarm-agent``
"""

import argparse
import os
import time
from functools import partial
from os.path import abspath, dirname, isfile

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

from pyfarm.core.logger import getLogger
from pyfarm.core.enums import OS, WINDOWS, UseAgentAddress
from pyfarm.core.sysinfo import user, network
from pyfarm.agent.config import config
from pyfarm.agent.entrypoints.check import (
    ip, port, uidgid, chroot, enum, integer, number)
from pyfarm.agent.entrypoints.utility import (
    get_pids, start_daemon_posix, write_pid_file, get_default_ip)


logger = getLogger("agent")


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
        global_network = self.parser.add_argument_group("Agent Network Service")
        global_network.add_argument(
            "--port", default=50000,
            type=partial(port, instance=self),
            help="The port number which the agent is either running on or "
                 "will started on.  This port is also reported the master "
                 "when an agent starts.")
        global_network.add_argument(
            "--agent-api-username", default="agent",
            help="The username required to access or manipulate the agent "
                 "using REST.")
        global_network.add_argument(
            "--agent-api-password", default="agent",
            help="The password required to access manipulate the agent "
                 "using REST.")

        # command line flags for the connecting the master apis
        global_apis = self.parser.add_argument_group("Master Resources")
        global_apis.add_argument(
            "--master",
            help="This is a convenience flag which will allow you to set the "
                 "hostname for the master.  By default this value will be "
                 "substituted in --master-api and --master-redis")
        global_apis.add_argument(
            "--master-api", default="http://%(master)s/api/v1",
            help="The location where the master's REST api is located.")

        global_apis.add_argument(
            "--master-api", default="http://%(master)s/api/v1",
            help="The location where the master's REST api is located, "
                 "defaulting to %(default)s")
        global_apis.add_argument(
            "--redis", default="%(master)s:6379/0",
            help="The location where redis can be contacted, defaulting "
                 "to %(default)s")

        # global command line flags which apply to top level
        # process control
        global_process = self.parser.add_argument_group("Process Control")
        global_process.add_argument(
            "--pidfile", default="pyfarm-agent.pid",
            help="The file to store the process id in, defaulting to "
                 "%(default)s.  Any provided path will be fully resolved "
                 "using os.path.abspath prior to running an operation such "
                 "as `start`")

        # start logging options
        logging_group = start.add_argument_group("Logging Options")
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
        start_network = start.add_argument_group("Network Service")
        start_network.add_argument(
            "--ip", default=get_default_ip(), type=partial(ip, instance=self),
            help="The IPv4 address which the agent will report to the "
                 "master [default: %(default)s]")
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

        # options for controlling how the process is launched
        start_process_group = start.add_argument_group("Process Control")
        start_process_group.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            help="If provided then do not run the process in the background.")
        start_process_group.add_argument(
            "--chroot",
            type=partial(chroot, instance=self),
            help="The directory to chroot the agent do upon launch.  This is "
                 "an optional security measure and generally is not ")
        start_process_group.add_argument(
            "--uid",
            type=partial(
                uidgid, flag="uid",
                get_id=getuid, check_id=getpwuid, set_id=setuid, instance=self),
            help="The user id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        start_process_group.add_argument(
            "--gid",
            type=partial(
                uidgid, flag="uid",
                get_id=getgid, check_id=getgrgid, set_id=setgid, instance=self),
            help="The group id to run the agent as.  *This setting is "
                 "ignored on Windows.*")

        # various options for how the agent will interact with the
        # master server
        start_http_group = start.add_argument_group("HTTP Configuration")
        start_http_group.add_argument(
            "--http-max-retries", default="unlimited",
            type=partial(integer, instance=self),
            help="The max number of times to retry a request to the master "
                 "after it has failed.  [default: %(default)s]")
        start_http_group.add_argument(
            "--http-retry-delay", default=3,
            type=partial(number, instance=self),
            help="If a http request to the master has failed, wait this amount "
                 "of time before trying again")

        # options when stopping the agent
        stop_process_group = stop.add_argument_group("Process Control")
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

            if self.args.chroot is not None:
                self.args.chroot = abspath(self.args.chroot)

            # update configuration with values from the command line
            config["http-max-retries"] = self.args.http_max_retries
            config["http-retry-delay"] = self.args.http_retry_delay
            config["ip"] = self.args.ip
            config["port"] = self.args.port
            config["hostname"] = self.args.hostname
            config["use_address"] = self.args.use_address

        self.index_url = "http://127.0.0.1:%s/" % self.args.port
        self.args.target_func()

    def start(self):
        # make sure the agent is not already running
        if any(get_pids(self.args.pidfile, self.index_url)):
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

    def stop(self):
        logger.info("stopping agent")
        file_pid, http_pid = get_pids(self.args.pidfile, self.index_url)

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
            url = self.index_url + "shutdown?wait=%s" % self.args.no_wait
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

        from twisted.internet import reactor
        from pyfarm.agent.service import agent
        agent(self.args.uid, self.args.gid)
        reactor.run()

    def status(self):
        logger.info("checking status")
        if any(get_pids(self.args.pidfile, self.index_url)):
            logger.error("agent appears to be running")
            return True
        else:
            return False
