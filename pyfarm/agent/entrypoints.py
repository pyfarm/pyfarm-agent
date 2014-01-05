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

import os
import argparse
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

from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert
from pyfarm.core.enums import OperatingSystem, OS

import psutil
import requests
from requests.exceptions import ConnectionError

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

        process_group = self.parser.add_argument_group("Parent Process Control")
        process_group.add_argument(
            "-n", "--no-daemon", default=False, action="store_true",
            help="If provided then do not run the process in the background.")
        process_group.add_argument(
            "--pidfile", default="pyfarm-agent.pid",
            help="The file to store the process id in (default: %(default)s)")
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
        process_group.add_argument(
            "--share-environment", default=False, action="store_true",
            help="If provided pass along the environment this was tool "
                 "launched in to the agent process.  By default this is not "
                 "done so any processes the agent runs will have a clean "
                 "environment.")

    def _get_id(self, value=None, flag=None, get_id=None):
        """retrieves the user or group id for a command line argument"""
        if OS == OperatingSystem.WINDOWS:
            return

        try:
            value = convert.ston(value)
        except ValueError:
            self.parser.error("failed to convert --%s to a number" % flag)

        try:
            get_id(value)
        except KeyError:
            self.parser.error("%s %s does not seem to exist" % value)
        else:
            return value

    def _check_chroot_directory(self, path):
        if not os.path.isdir(path):
            self.parser.error(
                "cannot chroot into '%s', it does not exist" % path)

        return path

    def __call__(self):
        self.args = self.parser.parse_args()
        self.args.target_func()

    def load_pid_file(self, path):
        # read all data from the pid file
        if not os.path.isfile(self.args.pidfile):
            logger.info("pid file does not exist")
            return

        with open(self.args.pidfile, "r") as pidfile:
            data = pidfile.read().strip()
            logger.debug("pidfile data: %s" % repr(data))

            try:
                pid, address, port = data.split(",")
            except ValueError:
                logger.error("invalid format in pid file")
                raise

        # convert the pid to a number
        try:
            pid = convert.ston(pid)
        except ValueError:
            logger.error("failed to convert pid to a number")
            raise

        # convert the port to a number
        try:
            port = convert.ston(port)
        except ValueError:
            logger.error("failed to convert port to a number")
            raise

        # remove stale pid file
        if not psutil.pid_exists(pid):
            logger.debug("process %s does not exist" % pid)
            logger.info("removing stale process id file")
            os.remove(self.args.pidfile)

        return pid, address, port

    def start(self):
        logger.info("starting agent")
        # TODO: if daemon on unix, double fork
        # TODO: if --no-daemon start ``application`` in this process
        # TODO: if daemon on windows, warn then start same as --no-daemon

    def stop(self):
        logger.info("stopping agent")

        # retrieve the parent process id, address, and port
        try:
            pid, address, port = self.load_pid_file(self.args.pidfile)
        except (IOError, OSError, ValueError, TypeError):
            return  # error comes from load_pid_file

        base_url = "http://%s:%s" % (address, port)

        # attempt to get the list of processes running
        processes_url = base_url + "/processes"
        logger.debug("querying %s" % processes_url)
        try:
            request = requests.get(processes_url)
        except ConnectionError:
            logger.error(
                "failed to retrieve process list, agent may already be stopped")
            return

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
            process_url = processes_url_template % (address, port, uuid)
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

        logger.info("agent stopped")

    def restart(self):
        logger.debug("restarting agent")
        self.stop()
        self.start()

    def status(self):
        logger.info("checking status")

agent = AgentEntryPoint()
