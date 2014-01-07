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
Entry Points
------------

Entry points for the agent, mainly :cmd:`pyfarm-agent`
"""

import argparse
import atexit
import os
import sys
from os.path import isdir, isfile, dirname, abspath
from functools import partial

try:
    from grp import getgrgid
    from pwd import getpwuid
except ImportError:  # pragma: no cover
    getgrgid = lambda _: _
    getpwuid = lambda _: _

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

try:
    import json
except ImportError:  # pragma: no cover
    import simplejson as json

try:
    os.fork
    FORK = True
except AttributeError:
    FORK = False

import netaddr
import psutil
import requests
from netaddr.core import AddrFormatError
from requests.exceptions import ConnectionError

from pyfarm.core.enums import OperatingSystem, OS
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert

logger = getLogger("agent")


class AgentEntryPoint(object):
    """Main object for parsing command line options"""
    def __init__(self):
        self.args = None
        self.parser = argparse.ArgumentParser()

        # add subparsers
        subparsers = self.parser.add_subparsers(
            help="individual operations %(prog)s can run")
        start = subparsers.add_parser(
            "start", help="starts the agent")
        stop = subparsers.add_parser(
            "stop", help="stops the agent")
        restart = subparsers.add_parser(
            "restart", help="restarts the agent")
        status = subparsers.add_parser(
            "status", help="query the 'running' state of the agent")

        # setup the target names and functions for the subparsers
        start.set_defaults(target_name="start", target_func=self.start)
        stop.set_defaults(target_name="stop", target_func=self.stop)
        restart.set_defaults(target_name="restart", target_func=self.restart)
        status.set_defaults(target_name="status", target_func=self.status)

        global_options = self.parser.add_argument_group("Global Options")
        global_options.add_argument(
            "--ip", default=self._default_ip(), type=self._validate_ip,
            help="The IPv4 address the agent is either currently running on "
                 "or will be running on when `start` is called.  By default "
                 "this should be the remotely reachable address which right "
                 "now appears to be %(default)s")
        global_options.add_argument(
            "--pidfile", default="pyfarm-agent.pid",
            help="The file to store the process id in, defaulting to "
                 "%(default)s.  Any provided path will be fully resolved "
                 "using os.path.abspath prior to running an operation such "
                 "as `start`")
        # TODO: implement log level control

        #
        # start target options
        #

        logging_group = start.add_argument_group("Logging Options")
        logging_group.add_argument(
            "--log", default=os.devnull,
            help="If provided log all output from the agent to this path.  "
                 "This will append to any existing log data.  [default: "
                 "%(default)s]")
        logging_group.add_argument(
            "--logerr",
            help="If provided then split any output from stderr into this file "
                 "path, otherwise send it to the same file as --log.")

        process_group = start.add_argument_group("Process Control")
        process_group.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            help="If provided then do not run the process in the background.")
        process_group.add_argument(
            "--chroot", type=self._check_chroot_directory,
            help="The directory to chroot the agent do upon launch.  This is "
                 "an optional security measure and generally is not ")
        process_group.add_argument(
            "--uid", type=partial(self._get_id, flag="uid", get_id=getpwuid),
            help="The user id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        process_group.add_argument(
            "--gid", type=partial(self._get_id, flag="uid", get_id=getgrgid),
            help="The group id to run the agent as.  *This setting is "
                 "ignored on Windows.*")

    def __call__(self):
        self.args = self.parser.parse_args()

        # if we're on windows, produce some warnings about
        # flags which are not supported
        if OS == OperatingSystem.WINDOWS:
            if self.args.uid:
                logger.warning(
                    "--uid is not currently supported on Windows")

            if self.args.gid:
                logger.warning(
                    "--gid is not currently supported on Windows")

            if self.args.no_daemon:
                logger.warning(
                    "--no-daemon is not currently supported on Windows")

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

        self.args.target_func()

    def _default_ip(self):
        try:
            from pyfarm.core.sysinfo import network
            return network.ip()
        except ValueError:
            logger.error("failed to determine ip address, using 127.0.0.1")
            return "127.0.0.1"

    def _validate_ip(self, value):
        try:
            netaddr.IPAddress(value)
            return value
        except (ValueError, AddrFormatError):
            self.parser.error("%s is not a valid ip address")

    def _get_port(self, value):

        # convert the incoming argument to a number or fail
        try:
            value = convert.ston(value)
        except ValueError:
            self.parser.error("failed to convert --port to a number")

        # make sure the port is within a permissible range
        highest_port = 65535
        if self.args.uid == 0:
            lowest_port = 1
        else:
            lowest_port = 49152

        if lowest_port <= value <= highest_port:
            self.parser.error(
                "valid port range is %s-%s" % (lowest_port, highest_port))

        return value

    def _get_id(self, value=None, flag=None, get_id=None):
        """retrieves the user or group id for a command line argument"""
        if OS == OperatingSystem.WINDOWS:
            return

        # convert the incoming argument to a number or fail
        try:
            value = convert.ston(value)
        except ValueError:
            self.parser.error("failed to convert --%s to a number" % flag)

        # ensure that the uid or gid actually exists
        try:
            get_id(value)
        except KeyError:
            self.parser.error("%s %s does not seem to exist" % value)

        # before returning the value, be sure we're a user that actually
        # has the power to change the effective user or group
        if os.getuid() != 0:
            self.parser.error("only root can set --%s" % flag)

        return value

    def _check_chroot_directory(self, path):
        if not isdir(path):
            self.parser.error(
                "cannot chroot into '%s', it does not exist" % path)

        return path

    def _mkdirs(self, path):
        if path and not isdir(path):
            os.makedirs(path)

    def load_pid_file(self):
        # read all data from the pid file
        if not isfile(self.args.pidfile):
            logger.debug("pid file does not exist, nothing to do")
            return

        with open(self.args.pidfile, "r") as pidfile:
            pid = pidfile.read().strip()
            if not pid:
                self.remove_pid_file(stale=True)
                return

        # convert the pid to a number
        try:
            pid = convert.ston(pid)
        except ValueError:
            logger.error("failed to convert pid to a number")
            raise

        # remove stale pid file
        if not psutil.pid_exists(pid):
            self.remove_pid_file(stale=True)

        return pid

    def remove_pid_file(self, stale=False):
        if isfile(self.args.pidfile):
            os.remove(self.args.pidfile)
            stale = " " if not stale else " stale "
            logger.debug("removed%spid file %s" % (stale, self.args.pidfile))
        else:
            logger.debug(
                "attempted to remove non-existent "
                "pid file: %s" % self.args.pidfile)

    def write_pid_file(self, pid):
        self._mkdirs(dirname(self.args.pidfile))

        # hard error, we should have handled this already
        assert not isfile(self.args.pidfile)

        with open(self.args.pidfile, "w") as pidfile:
            pidfile.write(str(pid))

        logger.debug("wrote pid file: %s" % pidfile.name)
        atexit.register(self.remove_pid_file, pidfile.name)

    def start_daemon_posix(self):
        """
        Runs the agent process via a double fork.  This basically a duplicate
        of Marcechal's original code with some adjustments:

            http://www.jejik.com/articles/2007/02/
            a_simple_unix_linux_daemon_in_python/

        Source files from his post are here:
            http://www.jejik.com/files/examples/daemon.py
            http://www.jejik.com/files/examples/daemon3x.py
        """
        # first fork
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            logger.error(
                "fork 1 failed (errno: %s): %s" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from the parent environment
        os.chdir(self.args.chroot or "/")
        os.setsid()
        os.umask(0)

        # second fork
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            logger.error(
                "fork 2 failed (errno: %s): %s" % (e.errno, e.strerror))
            sys.exit(1)

        # flush any pending data before we duplicate
        # the file descriptors
        sys.stdout.flush()
        sys.stderr.flush()

        # open up file descriptors for the new process
        stdin = open(os.devnull, "r")
        stdout = open(self.args.log, "a+")
        stderr = open(self.args.logerr, "a+", 0)
        os.dup2(stdin.fileno(), sys.stdin.fileno())
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

        #
        # new process begins
        #

        # if requested, set the user id of this process
        if self.args.uid:
            os.setuid(self.args.uid)

        # if requested, set the group id of this process
        if self.args.gid:
            os.setgid(self.args.gid)

    def start(self):
        # check for an existing pid file
        pid = self.load_pid_file()
        if pid is not None and os.getpid() != pid:
            logger.error("agent already running (pid: %s)" % pid)
            return

        elif pid is not None and os.getpid() == pid:
            self.remove_pid_file(self.args.pidfile)

        logger.info("starting agent")

        if not isfile(self.args.log):
            self._mkdirs(dirname(self.args.log))

        if self.args.logerr != self.args.log and not \
                isfile(self.args.logerr):
            self._mkdirs(dirname(self.args.logerr))

        # so long as FORK is True and we were not
        # told run in the foreground start the daemon so
        # all code after this executes in another process
        if not self.args.no_daemon and FORK:
            logger.info("sending stdout to %s" % self.args.log)
            logger.info("sending stderr to %s" % self.args.logerr)
            self.start_daemon_posix()

        elif not self.args.no_daemon and not FORK:
            logger.warning(
                "this platform does not support forking, "
                "starting in foreground")

        # always write out some information about the process
        pid = os.getpid()
        self.write_pid_file(pid)

        logger.info("pid: %s" % pid)
        logger.info("uid: %s" % os.getuid())
        logger.info("gid: %s" % os.getgid())

        i = 0
        while True:
            i += 1
            import time
            print i
            time.sleep(1)

        # NOTE: this code *might* be running inside a daemon

    def stop(self):
        logger.info("stopping agent")
        base_url = "http://%s:%s" % (self.args.bind, self.args.port)

        # attempt to get the list of processes running
        processes_url = base_url + "/processes"
        logger.debug("querying %s" % processes_url)
        try:
            request = requests.get(processes_url)
        except ConnectionError:
            logger.warning(
                "failed to retrieve running child processes via REST, agent "
                "may already be stopped")
        else:
            # even if the request went through the status code
            # should be OK
            if request.status_code != OK:
                logger.error(
                    "status code %s received when trying to "
                    "query processes" % request.reason)
                return

            logger.info("stopping process on agent")
            processes_url_template = base_url + "/processes/%s?wait=1"
            for data in request.json():
                uuid = data[0]
                logger.debug("stopping %s" % uuid)
                process_url = processes_url_template % (self.args.bind, self.args.port, uuid)
                result = requests.delete(process_url)

                if result.status_code != OK:
                    logger.error("failed to stop process %s" % uuid)
                    return

            logger.info("shutting down agent")
            result = requests.post(
                base_url + "/shutdown?wait=1",
                data=json.dumps({"reason": "command line shutdown"}),
                headers={"Content-Type": "application/json"})

            if result.status_code != OK:
                logger.error("agent shutdown failed")
                return

        # retrieve the parent process id, address, and port
        try:
            pid = self.load_pid_file()
        except (IOError, OSError, ValueError, TypeError):
            return  # error comes from load_pid_file

        if pid is None or not psutil.pid_exists(pid):
            logger.info("process is not running")
        else:
            logger.debug("killing process %s" % pid)
            process = psutil.Process(pid)
            process.terminate()
            logger.info("agent stopped")

    def restart(self):
        logger.debug("restarting agent")
        self.stop()
        self.start()

    def status(self):
        logger.info("checking status")
        pid = self.load_pid_file()

        if pid is None:
            logger.info("agent is not running")
        else:
            logger.info("agent is running, pid %s" % pid)


agent = AgentEntryPoint()
