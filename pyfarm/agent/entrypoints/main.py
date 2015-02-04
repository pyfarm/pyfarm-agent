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
----

The main module which constructs the entrypoint for the
``pyfarm-agent`` command line tool.
"""

from __future__ import division

import atexit
import os
import sys
import pdb
import time
from errno import ENOENT, EEXIST

from json import dumps

from os.path import dirname, isfile, isdir, expandvars, expanduser

try:
    from httplib import ACCEPTED, OK, responses
except ImportError:  # pragma: no cover
    from http.client import ACCEPTED, OK, responses

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
import signal
from requests import ConnectionError
from twisted.internet import reactor

from pyfarm.core.enums import (
    OS, WINDOWS, AgentState, INTEGER_TYPES, operating_system)

from pyfarm.agent.logger import getLogger
from pyfarm.agent.logger.twistd import Observer
from pyfarm.agent.config import config
from pyfarm.agent.entrypoints.parser import (
    AgentArgumentParser, ip, port,  uidgid, enum, number, uuid_type)
from pyfarm.agent.entrypoints.utility import start_daemon_posix
from pyfarm.agent.sysinfo import memory, cpu
from pyfarm.agent.utility import AgentUUID


logger = getLogger("agent.cmd")

config["start"] = time.time()


class AgentEntryPoint(object):
    """Main object for parsing command line options"""
    def __init__(self):
        self.args = None
        self.parser = AgentArgumentParser(
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
            "--port", config="agent_api_port", type=port,
            type_kwargs=dict(get_uid=lambda: self.args.uid == 0),
            help="The port number which the agent is either running on or "
                 "will run on when started.  This port is also reported the "
                 "master when an agent starts. [default: %(default)s]")
        global_network.add_argument(
            "--host", config="agent_hostname",
            help="The host to communicate with or hostname to present to the "
                 "master when starting.  Defaults to the fully qualified "
                 "hostname.")
        global_network.add_argument(
            "--agent-api-username", default="agent", config=False,
            help="The username required to access or manipulate the agent "
                 "using REST. [default: %(default)s]")
        global_network.add_argument(
            "--agent-api-password", default="agent", config=False,
            help="The password required to access manipulate the agent "
                 "using REST. [default: %(default)s]")
        global_network.add_argument(
            "--agent-id", config="agent_id", type=uuid_type,
            default=None,
            help="The UUID used to identify this agent to the master.  By "
                 "default the agent will attempt to load a cached value "
                 "however a specific UUID could be provided with this flag.")
        global_network.add_argument(
            "--agent-id-file", config="agent_id_file",
            default=expanduser(expandvars(
                config["agent_id_file_platform_defaults"][operating_system()])),
            help="The location to store the agent's id.  By default the path "
                 "is platform specific and defined by the "
                 "`agent_id_file_platform_defaults` key in the configuration.  "
                 "[default: %(default)s]")

        # command line flags for the connecting the master apis
        global_apis = self.parser.add_argument_group(
            "Network Resources",
            description="Resources which the agent will be communicating with.")
        global_apis.add_argument(
            "--master", config="master",
            help="This is a convenience flag which will allow you to set the "
                 "hostname for the master.  By default this value will be "
                 "substituted in --master-api")
        global_apis.add_argument(
            "--master-api", config="master_api",
            help="The location where the master's REST api is located. "
                 "[default: %(default)s]")
        global_apis.add_argument(
            "--master-api-version", config="master_api_version",
            help="Sets the version of the master's REST api the agent should"
                 "use [default: %(default)s]")

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
            "--pidfile", config="agent_lock_file",
            help="The file to store the process id in. [default: %(default)s]")
        global_process.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            config=False,
            help="If provided then do not run the process in the background.")
        global_process.add_argument(
            "--chdir", config="agent_chdir", type=isdir,
            help="The working directory to change the agent into upon launch")
        global_process.add_argument(
            "--uid", type=uidgid, config=False,
            type_kwargs=dict(get_id=getuid, check_id=getpwuid, set_id=setuid),
            help="The user id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        global_process.add_argument(
            "--gid", type=uidgid, config=False,
            type_kwargs=dict(get_id=getgid, check_id=getgrgid, set_id=setgid),
            help="The group id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        global_process.add_argument(
            "--pdb-on-unhandled", action="store_true",
            help="When set pdb.set_trace() will be called if an unhandled "
                 "error is caught in the logger")

        # start general group
        start_general_group = start.add_argument_group(
            "General Configuration",
            description="These flags configure parts of the agent related to "
                        "hardware, state, and certain timing and scheduling "
                        "attributes.")
        start_general_group.add_argument(
            "--state", default=AgentState.ONLINE, config=False,
            type=enum, type_kwargs=dict(enum=AgentState),
            help="The current agent state, valid values are "
                 "" + str(list(AgentState)) + ". [default: %(default)s]")
        start_general_group.add_argument(
            "--time-offset", config="agent_time_offset",
            type=int, type_kwargs=dict(min_=0),
            help="If provided then don't talk to the NTP server at all to "
                 "calculate the time offset.  If you know for a fact that this "
                 "host's time is always up to date then setting this to 0 is "
                 "probably a safe bet.")
        start_general_group.add_argument(
            "--ntp-server", config="agent_ntp_server",
            help="The default network time server this agent should query to "
                 "retrieve the real time.  This will be used to help determine "
                 "the agent's clock skew if any.  Setting this value to '' "
                 "will effectively disable this query. [default: %(default)s]")
        start_general_group.add_argument(
            "--ntp-server-version", config="agent_ntp_server_version",
            type=int,
            help="The version of the NTP server in case it's running an older"
                 "or newer version. [default: %(default)s]")
        start_general_group.add_argument(
            "--no-pretty-json", config="agent_pretty_json",
            action="store_false",
            help="If provided do not dump human readable json via the agent's "
                 "REST api")
        start_general_group.add_argument(
            "--shutdown-timeout", config="agent_shutdown_timeout",
            type=int, type_kwargs=dict(min_=0),
            help="How many seconds the agent should spend attempting to inform "
                 "the master that it's shutting down.")
        start_general_group.add_argument(
            "--updates-drop-dir", config="agent_updates_dir",
            help="The directory to drop downloaded updates in. This should be "
            "the same directory pyfarm-supervisor will look for updates in. "
            "[default: %(default)s]")
        start_general_group.add_argument(
            "--run-control-file", config="run_control_file",
            default=expanduser(expandvars(
                config["run_control_file_by_platform"][operating_system()])),
            help="The path to a file that will signal to the supervisor that "
                 "agent is supposed to be restarted if it stops for whatever "
                 "reason."
                 "[default: %(default)s]")

        # start hardware group
        start_hardware_group = start.add_argument_group(
            "Physical Hardware",
            description="Command line flags which describe the hardware of "
                        "the agent.")
        start_hardware_group.add_argument(
            "--cpus", default=cpu.total_cpus(),
            config="agent_cpus", type=int,
            help="The total amount of cpus installed on the "
                 "system.  Defaults to the number of cpus installed "
                 "on the system.")
        start_hardware_group.add_argument(
            "--ram", default=memory.total_ram(),
            config="agent_ram", type=int,
            help="The total amount of ram installed on the system in "
                 "megabytes.  Defaults to the amount of ram the "
                 "system has installed.")

        # start interval controls
        start_interval_group = start.add_argument_group(
            "Interval Controls",
            description="Controls which dictate when certain internal "
                        "intervals should occur.")
        start_interval_group.add_argument(
            "--ram-check-interval",
            config="agent_ram_check_interval", type=int,
            help="How often ram resources should be checked for changes. "
                 "The amount of memory currently being consumed on the system "
                 "is checked after certain events occur such as a process but "
                 "this flag specifically controls how often we should check "
                 "when no such events are occurring. [default: %(default)s]")
        start_interval_group.add_argument(
            "--ram-max-report-frequency",
            config="agent_ram_max_report_frequency", type=int,
            help="This is a limiter that prevents the agent from reporting "
                 "memory changes to the master more often than a specific "
                 "time interval.  This is done in order to ensure that when "
                 "100s of events fire in a short period of time cause changes "
                 "in ram usage only one or two will be reported to the "
                 "master. [default: %(default)s]")
        start_interval_group.add_argument(
            "--ram-report-delta", config="agent_ram_report_delta", type=int,
            help="Only report a change in ram if the value has changed "
                 "at least this many megabytes. [default: %(default)s]")
        start_interval_group.add_argument(
            "--master-reannounce", config="agent_master_reannounce", type=int,
            help="Controls how often the agent should reannounce itself "
                 "to the master.  The agent may be in contact with the master "
                 "more often than this however during long period of "
                 "inactivity this is how often the agent will 'inform' the "
                 "master the agent is still online.")

        # start logging options
        logging_group = start.add_argument_group(
            "Logging Options",
            description="Settings which control logging of the agent's parent "
                        "process and/or any subprocess it runs.")
        logging_group.add_argument(
            "--log", config="agent_log",
            help="If provided log all output from the agent to this path.  "
                 "This will append to any existing log data.  [default: "
                 "%(default)s]")
        logging_group.add_argument(
            "--capture-process-output", config="jobtype_capture_process_output",
            action="store_true",
            help="If provided then all log output from each process launched "
                 "by the agent will be sent through agent's loggers.")
        logging_group.add_argument(
            "--task-log-dir", config="jobtype_task_logs",
            type=isdir, type_kwargs=dict(create=True),
            help="The directory tasks should log to.")

        # network options for the agent when start is called
        start_network = start.add_argument_group(
            "Network Service",
            description="Controls how the agent is seen or interacted with "
                        "by external services such as the master.")
        start_network.add_argument(
            "--ip-remote", type=ip, config=False,
            help="The remote IPv4 address to report.  In situation where the "
                 "agent is behind a firewall this value will typically be "
                 "different.")

        start_manhole = start.add_argument_group(
            "Manhole Service",
            description="Controls the manhole service which allows a telnet "
                        "connection to be made directly into the agent as "
                        "it's running.")
        start_manhole.add_argument(
            "--enable-manhole", config="agent_manhole",
            action="store_true",
            help="When provided the manhole service will be started once the "
                 "reactor is running.")
        start_manhole.add_argument(
            "--manhole-port", config="agent_manhole_port", type=port,
            type_kwargs=dict(get_uid=lambda: self.args.uid == 0),
            help="The port the manhole service should run on if enabled.")
        start_manhole.add_argument(
            "--manhole-username", config="agent_manhole_username",
            help="The telnet username that's allowed to connect to the "
                 "manhole service running on the agent.")
        start_manhole.add_argument(
            "--manhole-password", config="agent_manhole_password",
            help="The telnet password to use when connecting to the "
                 "manhole service running on the agent.")

        # various options for how the agent will interact with the
        # master server
        start_http_group = start.add_argument_group(
            "HTTP Configuration",
            description="Options for how the agent will interact with the "
                        "master's REST api and how it should run it's own "
                        "REST api.")
        start_http_group.add_argument(
            "--html-templates-reload", config="agent_html_template_reload",
            action="store_true",
            help="If provided then force Jinja2, the html template system, "
                 "to check the file system for changes with every request. "
                 "This flag should not be used in production but is useful "
                 "for development and debugging purposes.")
        start_http_group.add_argument(
            "--static-files", config="agent_static_root", type=isdir,
            help="The default location where the agent's http server should "
                 "find static files to serve.")
        start_http_group.add_argument(
            "--http-retry-delay", config="agent_http_retry_delay", type=number,
            help="If a http request to the master has failed, wait this amount "
                 "of time before trying again")

        jobtype_group = start.add_argument_group("Job Types")
        jobtype_group.add_argument(
            "--jobtype-no-cache", config="jobtype_enable_cache",
            action="store_true",
            help="If provided then do not cache job types, always directly "
                 "retrieve them.  This is beneficial if you're testing the "
                 "agent or a new job type class.")

        # options when stopping the agent
        stop_group = stop.add_argument_group(
            "optional flags",
            description="Flags that control how the agent is stopped")
        stop_group.add_argument(
            "--no-wait", default=False, action="store_true", config=False,
            help="If provided then don't wait on the agent to shut itself "
                 "down.  By default we would want to wait on each task to stop "
                 "so we can catch any errors and then finally wait on the "
                 "agent to shutdown too.  If you're in a hurry or stopping a "
                 "bunch of agents at once then setting this flag will let the "
                 "agent continue to stop itself without waiting for each agent")

    def __call__(self):
        logger.debug("Parsing command line arguments")
        self.args = self.parser.parse_args()

        # No daemon mode must be set with --pdb-on-unhanded, without this
        # you could end up with a blocking agent and no way of knowing
        # about it.
        if not self.args.no_daemon and self.args.pdb_on_unhandled:
            self.parser.error(
                "You cannot set --pdb-on-unhandled without --no-daemon")

        if self.args.pdb_on_unhandled:
            Observer.PDB_ON_UNHANDLED_ERROR = True

            def excepthook(exctype, value, traceback):
                pdb.set_trace()

            sys.excepthook = excepthook

        if not config["master"] and self.args.target_name == "start":
            self.parser.error(
                "--master must be provided (ex. "
                "'pyfarm-agent --master=foobar start')")

        # if we're on windows, produce some warnings about
        # flags which are not supported
        if WINDOWS and self.args.uid:
            logger.warning("--uid is not currently supported on Windows")

        if WINDOWS and self.args.gid:
            logger.warning("--gid is not currently supported on Windows")

        if WINDOWS and self.args.no_daemon:
            logger.warning("--no-daemon is not currently supported on Windows")

        if self.args.agent_id is None:
            agent_id = AgentUUID.load(self.args.agent_id_file)

            # No agent id saved, generate one then try to save it.  If we
            # can't then an error will be raised when AgentUUID.save is called.
            if agent_id is None:
                agent_id = AgentUUID.generate()
                AgentUUID.save(agent_id, self.args.agent_id_file)

            self.args.agent_id = agent_id

        # A custom --agent-id was provided, warn if it varies from one
        # we load from disk.  We won't try to save it however because
        # that could cause conflicts if someone is using --agent-id
        # and trying to run multiple agents.
        else:
            saved_agent_id = AgentUUID.load(self.args.agent_id_file)
            if (saved_agent_id is not None
                    and saved_agent_id != self.args.agent_id):
                logger.warning(
                    "Custom agent ID has been provided by --agent-id")

        config["agent_id"] = self.args.agent_id

        if self.args.target_name == "start":
            # update configuration with values from the command line
            config_flags = {
                "state": self.args.state,
                "pids": {
                    "parent": os.getpid()}}

            config.update(config_flags)

        return_code = self.args.target_func()

        # If a specific return code is provided then use it
        # directly in sys.exit
        if isinstance(return_code, INTEGER_TYPES):
            sys.exit(return_code)

    @property
    def agent_api(self):
        return "http://%s:%s/api/v1" % (
            config["agent_hostname"], config["agent_api_port"])

    def start(self):
        url = self.agent_api + "/status"
        try:
            response = requests.get(
                url, headers={"Content-Type": "application/json"})
        except ConnectionError:
            pid = None
            remove_lock_file = False

            # Try to open an existing lock file
            try:
                with open(config["agent_lock_file"], "r") as pidfile:
                    try:
                        pid = int(pidfile.read().strip())
                    except ValueError:
                        logger.warning(
                            "Could not convert pid in %s to an integer.",
                            config["agent_lock_file"])
                        remove_lock_file = True

            # If the file is missing we ignore the error and read
            # pid/remove_lock_file later on.
            except (OSError, IOError) as e:
                if e.errno == ENOENT:
                    logger.debug(
                        "Process ID file %s does not exist",
                        config["agent_lock_file"])
                else:
                    raise

            else:
                assert pid is not None, "pid was not set"
                logger.debug(
                    "Process ID file %s exists", config["agent_lock_file"])

            if pid is not None:
                try:
                    process = psutil.Process(pid)

                # Process in the pid file does not exist
                except psutil.NoSuchProcess:
                    logger.debug(
                        "Process ID in %s is stale.",
                        config["agent_lock_file"])
                    remove_lock_file = True

                # Process does exist, does it appear to be our agent?
                else:
                    if process.name() == "pyfarm-agent":
                        logger.error(
                            "Agent is already running, pid %s", pid)
                        return 1

                    # Not our agent so the lock file is probably wrong.
                    else:
                        logger.debug(
                            "Process %s does not appear to be the "
                            "agent.", pid)
                        remove_lock_file = True

            if remove_lock_file:
                logger.debug(
                    "Attempting to remove PID file %s",
                    config["agent_lock_file"])

                try:
                    os.remove(config["agent_lock_file"])
                except OSError as e:
                    if e.errno != ENOENT:
                        logger.warning(
                            "Failed to remove PID file %s: %s (will retry)",
                            config["agent_lock_file"], e)

                        # Try again when the process exits since the
                        # OS is more likely to have released the file
                        # handle by then.
                        @atexit.register
                        def remove_pid_file_on_exit():
                            logger.debug(
                                "Trying to remove %s at exit",
                                config["agent_lock_file"])
                            try:
                                os.remove(config["agent_lock_file"])
                            except OSError as e:
                                if e.errno != ENOENT:
                                    logger.error(
                                        "Failed to remove PID file %s: %s",
                                        config["agent_lock_file"], e)

                    return 1
        else:
            code = "%s %s" % (
                response.status_code, responses[response.status_code])
            pid = response.json()["pids"]["parent"]
            logger.error(
                "Agent at pid %s is already running, got %s from %s.",
                pid, code, url)
            return 1

        logger.info("Starting agent")

        if not isdir(config["jobtype_task_logs"]):
            logger.debug("Creating %s", config["jobtype_task_logs"])
            try:
                os.makedirs(config["jobtype_task_logs"])
            except OSError:
                logger.error("Failed to create %s", config["jobtype_task_logs"])
                return 1

        # create the directory for log
        if not self.args.no_daemon and not isfile(config["agent_log"]):
            try:
                os.makedirs(dirname(config["agent_log"]))
            except OSError:
                # Not an error because it could be created later on
                logger.warning(
                    "failed to create %s" % dirname(config["agent_log"]))

        # so long as fork could be imported and --no-daemon was not set
        # then setup the log files
        if not self.args.no_daemon and fork is not NotImplemented:
            logger.info("sending log output to %s" % config["agent_log"])
            daemon_start_return_code = start_daemon_posix(
                config["agent_log"], config["agent_chdir"],
                self.args.uid, self.args.gid)

            if isinstance(daemon_start_return_code, INTEGER_TYPES):
                return daemon_start_return_code

        elif not self.args.no_daemon and fork is NotImplemented:
            logger.warning(
                "`fork` is not implemented on %s, starting in "
                "foreground" % OS.title())

        # PID file should not exist now.  Either the last agent instance
        # should have removed it or we should hae above.
        if isfile(config["agent_lock_file"]):
            logger.error("PID file should not exist on disk at this point.")
            return 1

        # Create the directory for the pid file if necessary
        pid_dirname = dirname(config["agent_lock_file"])
        if not isdir(pid_dirname):
            try:
                os.makedirs(pid_dirname)
            except OSError:  # pragma: no cover
                logger.error(
                    "Failed to create parent directory for %s",
                    config["agent_lock_file"])
                return 1
            else:
                logger.debug("Created directory %s", pid_dirname)

        # Write the PID file
        pid = os.getpid()
        try:
            with open(config["agent_lock_file"], "w") as pidfile:
                pidfile.write(str(pid))
        except OSError as e:
            logger.error(
                "Failed to write PID file %s: %s", config["agent_lock_file"], e)
            return 1
        else:
            logger.debug("Wrote PID to %s", config["agent_lock_file"])

        logger.info("pid: %s", pid)

        if not isfile(config["run_control_file"]):
            directory = dirname(config["run_control_file"])
            try:
                os.makedirs(directory)
            except (OSError, IOError) as e:  # pragma: no cover
                if e.errno != EEXIST:
                    logger.error(
                        "Failed to create parent directory for %s: %s: %s",
                        config["run_control_file"], type(e).__name__, e)
                    raise
            else:
                logger.debug("Created directory %s", directory)
            try:
                with open(config["run_control_file"], "a"):
                    pass
            except (OSError, IOError) as e:
                logger.error("Failed to create run control file %s: %s: %s",
                             config["run_control_file"], type(e).__name__, e)
            else:
                logger.info("Created run control file %s",
                            config["run_control_file"])

        if getuid is not NotImplemented:
            logger.info("uid: %s" % getuid())

        if getgid is not NotImplemented:
            logger.info("gid: %s" % getgid())

        from pyfarm.agent.service import Agent

        # Setup the agent, register stop(), then run the agent
        service = Agent()
        signal.signal(signal.SIGINT, service.sigint_handler)
        reactor.callWhenRunning(service.start)
        reactor.run()

    def stop(self):
        logger.info("Stopping the agent")
        url = self.agent_api + "/stop"

        try:
            # TODO: this NEEDS to be an authenticated request
            response = requests.post(
                url,
                data=dumps({"wait": not self.args.no_wait}),
                headers={"Content-Type": "application/json"})

        except Exception as e:
            logger.error("Failed to contact %s: %s", url, e)
            return 1

        if response.status_code == ACCEPTED:
            logger.info("Agent is stopping")
        elif response.status_code == OK:
            logger.info("Agent has stopped")
        else:
            logger.error(
                "Received code %s when attempting to access %s: %s",
                response.status_code, url, response.text)

    def status(self):
        url = self.agent_api + "/status"
        logger.debug("Checking agent status via api using 'GET %s'", url)
        try:
            response = requests.get(
                url, headers={"Content-Type": "application/json"})

        # REST request failed for some reason, try with the pid file
        except Exception as e:
            logger.debug(str(e))
            logger.warning(
                "Failed to communicate with %s's API.  We can only roughly "
                "determine if agent is offline or online.",
                config["agent_hostname"])

            # TODO: use config for pidfile, --pidfile should be an override
            if not isfile(config["agent_lock_file"]):
                logger.debug(
                    "Process ID file %s does not exist",
                    config["agent_lock_file"])
                logger.info("Agent is offline")
                return 1

            else:
                with open(config["agent_lock_file"], "r") as pidfile:
                    try:
                        pid = int(pidfile.read().strip())
                    except ValueError:
                        logger.error(
                            "Could not convert pid in %s to an integer.",
                            config["agent_lock_file"])
                        return 1

                try:
                    process = psutil.Process(pid)
                except psutil.NoSuchProcess:
                    logger.info("Agent is offline.")
                    return 1
                else:
                    if process.name() == "pyfarm-agent":
                        logger.info("Agent is online.")
                    else:
                        logger.warning(
                            "Process %s does not appear to be the agent", pid)
                        logger.info("Agent is offline.")

            return

        data = response.json()
        pid_parent = data["pids"]["parent"]
        pid_child = data["pids"]["child"]

        # Print some general information about the agent
        logger.info("Agent %(agent_hostname)s is %(state)s" % data)
        logger.info("               Uptime: %(uptime)s seconds" % data)
        logger.info(
            "  Last Master Contact: %(last_master_contact)s seconds" % data)
        logger.info("    Parent Process ID: %(pid_parent)s" % locals())
        logger.info("           Process ID: %(pid_child)s" % locals())
        logger.info("          Database ID: %(id)s" % data)
        logger.info("            System ID: %(agent_id)s" % data)
        logger.info(
            "      Child Processes: %(child_processes)s "
            "(+%(grandchild_processes)s grandchildren)" % data)
        logger.info("   Memory Consumption: %(consumed_ram)sMB" % data)


agent = AgentEntryPoint()
