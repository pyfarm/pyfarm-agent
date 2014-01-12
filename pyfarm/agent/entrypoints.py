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
import time
from os.path import isdir, isfile, dirname, abspath
from functools import partial

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

try:
    import json
except ImportError:  # pragma: no cover
    import simplejson as json


import netaddr
import psutil
import requests
from netaddr.core import AddrFormatError
from requests.exceptions import ConnectionError

try:
    from collections import namedtuple
except ImportError:
    from pyfarm.core.backports import namedtuple

from pyfarm.core.enums import WINDOWS, OS
from pyfarm.core.sysinfo import user
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
            "--ip", default="127.0.0.1", type=self._validate_ip,
            help="The IPv4 address the agent is either currently running on "
                 "or will be running on when `start` is called.  By default "
                 "this should be the remotely reachable address which right "
                 "now appears to be %(default)s")
        global_options.add_argument(
            "--api-username", default="agent",
            help="The username required to access or manipulate the agent "
                 "using REST.")
        global_options.add_argument(
            "--api-password", default="agent",
            help="The password required to access manipulate the agent "
                 "using REST.")
        global_options.add_argument(
            "--port", default=50000, type=self._get_port,
            help="The port number which the agent is either running on or "
                 "will started on.")
        global_options.add_argument(
            "--pidfile", default="pyfarm-agent.pid",
            help="The file to store the process id in, defaulting to "
                 "%(default)s.  Any provided path will be fully resolved "
                 "using os.path.abspath prior to running an operation such "
                 "as `start`")
        # TODO: implement log level control

        #
        # start - logging options
        #
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

        #
        # start - process control
        #
        start_process_group = start.add_argument_group("Process Control")
        start_process_group.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            help="If provided then do not run the process in the background.")
        start_process_group.add_argument(
            "--chroot", type=self._check_chroot_directory,
            help="The directory to chroot the agent do upon launch.  This is "
                 "an optional security measure and generally is not ")
        start_process_group.add_argument(
            "--uid",
            type=partial(
                self._get_id, flag="uid",
                get_id=getuid, check_id=getpwuid, set_id=setuid),
            help="The user id to run the agent as.  *This setting is "
                 "ignored on Windows.*")
        start_process_group.add_argument(
            "--gid",
            type=partial(
                self._get_id, flag="uid",
                get_id=getgid, check_id=getgrgid, set_id=setgid),
            help="The group id to run the agent as.  *This setting is "
                 "ignored on Windows.*")

        #
        # stop -- process control
        #
        stop_process_group = stop.add_argument_group("Process Control")
        force = stop_process_group.add_argument(
            "--force", default=False, action="store_true",
            help="Ignore any previous errors not covered by other flags and "
                 "terminate or restart the process.")
        no_wait = stop_process_group.add_argument(
            "--no-wait", default=False, action="store_true",
            help="If provided then don't wait on the agent to shut itself "
                 "down.  By default we would want to wait on each task to stop "
                 "so we can catch any errors and then finally wait on the "
                 "agent to shutdown too.  If you're in a hurry or stopping a "
                 "bunch of agents at once then setting this flag will let the "
                 "agent continue to stop itself without providing feedback "
                 "directly.")
        ignore_pid_mismatch = stop_process_group.add_argument(
            "--ignore-pid-mismatch", default=False, action="store_true",
            help="If provided then any discrepancies between the pid reported "
                 "by the agent process itself and the pid file will be "
                 "ignored. This will cause stop to only produce a "
                 "warning and the continue on by trusting the pid provided by "
                 "the running agent over the value in the pid file.")

        #
        # restart -- process control (mostly referencing the above)
        #
        restart_process_group = restart.add_argument_group("Process Control")
        restart_process_group.add_argument(
            *force.option_strings, help=force.help,
            default=force.default, action="store_true")
        restart_process_group.add_argument(
            *no_wait.option_strings, help=no_wait.help,
            default=no_wait.default, action="store_true")
        restart_process_group.add_argument(
            *ignore_pid_mismatch.option_strings, help=ignore_pid_mismatch.help,
            default=ignore_pid_mismatch.default, action="store_true")

    def __call__(self):
        self.args = self.parser.parse_args()

        # if we're on windows, produce some warnings about
        # flags which are not supported
        if WINDOWS:
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

        self.index_url = "http://%s:%s/" % (self.args.ip, self.args.port)
        self.args.target_func()

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

    def _get_id(self, value=None, flag=None,
                get_id=None, check_id=None, set_id=None):
        """retrieves the user or group id for a command line argument"""
        # make sure the partial function is setting
        # the input values
        assert flag is not None
        assert get_id is not None
        assert check_id is not None
        assert set_id is not None

        if set_id is NotImplemented:
            logger.info("--%s is ignored on %s" % (flag, OS.title()))
            return

        elif not value:
            return

        # convert the incoming argument to a number or fail
        try:
            value = convert.ston(value)
        except ValueError:
            self.parser.error("failed to convert --%s to a number" % flag)

        # make sure the id actually exists
        try:
            check_id(value)
        except KeyError:
            self.parser.error("%s %s does not seem to exist" % (flag, value))


        # get the uid/gid of the current process
        # so we can reset it after checking it
        original_id = get_id()

        # Try to set the uid/gid to the value requested, fail
        # otherwise.  We're doing this here because we'll be
        # able to stop the rest of the code from running faster
        # and provide a more useful error message.
        try:
            set_id(value)
        except OSError:
            self.parser.error(
                "Failed to set %s to %s, please make sure you have "
                "the necessary permissions to perform this action.  "
                "Typically you must be running as root." % (flag, value))

        # set the uid/gid back to the original value since
        # the id change should occur inside the form or right
        # before the agent is started
        try:
            set_id(original_id)
        except OSError:
            self.parser.error()

        return value

    def _check_chroot_directory(self, path):
        if not isdir(path):
            self.parser.error(
                "cannot chroot into '%s', it does not exist" % path)

        return path

    def _mkdirs(self, path):
        if path and not isdir(path):
            logger.debug("mkdir %s" % path)
            os.makedirs(path)

    def get_json(self, url):
        """retrieve the json data from the given url or returns None"""
        try:
            page = requests.get(
                url, headers={"Content-Type": "application/json"})
        except ConnectionError:
            logger.debug("GET %s (connection error)" % url)
            return None
        else:
            logger.debug("GET %s" % url)
            logger.debug("  contents: %s" % page.text)
            if not page.ok:
                logger.warning("%s status was %s" % repr(page.reason))
                return None
            else:
                return page.json()

    def remove_pid_file(self):
        if isfile(self.args.pidfile):
            os.remove(self.args.pidfile)
            logger.debug("removed %s" % self.args.pidfile)

    def write_pid_file(self, pid):
        # if this fails is because we've done something wrong somewhere
        # else in this code
        assert not isfile(self.args.pidfile), "pid file should not exist now!"

        # write out the pid to the file
        self._mkdirs(dirname(self.args.pidfile))
        with open(self.args.pidfile, "w") as pidfile:
            pidfile.write(str(pid))

        logger.debug("wrote %s to %s" % (pid, pidfile.name))

        if self.args.no_daemon:
            atexit.register(self.remove_pid_file)

    def get_process(self):
        """
        Returns (pid, process_object) after loading the pid file.  If we
        have problems loading the pid file or the process does not exist
        this function will return (None, None)
        """
        if not isfile(self.args.pidfile):
            logger.debug("%s does not exist" % self.args.pidfile)
            return None, None

        # retrieve the pid from the file
        logger.debug("opening %s" % self.args.pidfile)
        with open(self.args.pidfile, "r") as pid_file_object:
            pid = pid_file_object.read().strip()

        # safeguard in case the file does not contain data
        if not pid:
            logger.debug("no pid in %s" % self.args.pidfile)
            self.remove_pid_file()
            return None, None

        # convert the pid to a number from a string
        try:
            pid = convert.ston(pid)
        except ValueError:
            logger.error(
                "failed to convert pid in %s to a number" % self.args.pidfile)
            raise

        # this shouldn't happen....but just in case
        if pid == os.getpid():
            logger.warning(
                "%s contains the current process pid" % self.args.pidfile)

        # try to load up the process object
        try:
            process = psutil.Process(pid)
        except psutil.NoSuchProcess:
            logger.debug("no such process %s" % pid)
            self.remove_pid_file()
        else:
            process_name = process.name.lower()
            logger.debug("%s is named '%s'" % (pid, process_name))

            # the vast majority of the time, the process will be
            # ours in this case
            if process_name == "pyfarm-agent" \
                    or process_name.startswith("pyfarm"):
                return pid, process

            # if it's a straight Python process it might still
            # be ours depending on how it was launched but we can't
            # do much else without more information
            elif process_name.startswith("python"):
                logger.warning(
                    "%s appears to be a normal Python process and may not "
                    "be pyfarm-agent" % pid)
                return pid, process

            else:
                logger.warning(
                    "Process name is neither python or "
                    "pyfarm-agent, instead it was '%s'." % process_name)

        return None, None

    def get_pids(self):
        """
        Returns a named tuple of the pid from the file, pid from the http
        server, main process object
        """
        # get the pid from the file, the process object and some information
        # from the REST api
        pid_file, process = self.get_process()
        index = self.get_json(self.index_url) or {}

        if not index:
            logger.debug(
                "failed to retrieve agent information using the REST api")

        return pid_file, index.get("pid")

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
            pid = fork()
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
            pid = fork()
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

        # if requested, set the user id of this process
        if self.args.uid:
            os.setuid(self.args.uid)

        # if requested, set the group id of this process
        if self.args.gid:
            os.setgid(self.args.gid)

    def start(self):
        # make sure the agent is not already running
        if any(self.get_pids()):
            logger.error("agent appears to be running")
            return

        logger.info("starting agent")

        if not isfile(self.args.log):
            self._mkdirs(dirname(self.args.log))

        if self.args.logerr != self.args.log and not \
            isfile(self.args.logerr):
            self._mkdirs(dirname(self.args.logerr))

        # so long as FORK is True and we were not
        # told run in the foreground start the daemon so
        # all code after this executes in another process
        if not self.args.no_daemon and fork is not NotImplemented:
            logger.info("sending stdout to %s" % self.args.log)
            logger.info("sending stderr to %s" % self.args.logerr)
            self.start_daemon_posix()

        elif not self.args.no_daemon and fork is NotImplemented:
            logger.warning(
                "this platform does not support forking, "
                "starting in foreground")

        # always write out some information about the process
        pid = os.getpid()
        self.write_pid_file(pid)

        logger.info("pid: %s" % pid)

        if getuid is not NotImplemented:
            logger.info("uid: %s" % getuid())

        if getgid is not NotImplemented:
            logger.info("gid: %s" % getgid())

        i = 0
        while True:
            i += 1
            import time
            print i
            time.sleep(1)

    def stop(self):
        logger.info("stopping agent")
        file_pid, http_pid = self.get_pids()

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

                    self.remove_pid_file()

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
                self.remove_pid_file()

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


commands = AgentEntryPoint()
