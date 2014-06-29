# No shebang line, this module is meant to be imported
#
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

import argparse
import os
import signal
import subprocess
import sys
import time
import zipfile
import shutil

from os.path import join, expanduser

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

from pyfarm.core.enums import INTEGER_TYPES, OS
from pyfarm.core.config import read_env, read_env_int
from pyfarm.agent.logger import getLogger
from pyfarm.agent.entrypoints.utility import start_daemon_posix

logger = getLogger("agent.supervisor")

default_data_root = read_env("PYFARM_AGENT_DATA_ROOT", ".pyfarm_agent")


def supervisor():
    logger.debug("Supervisor called with: %r", sys.argv)

    supervisor_args = []
    agent_args = []
    in_agent_args = False
    tmp_argv = list(sys.argv)
    del tmp_argv[0]
    for arg in tmp_argv:
        if not in_agent_args and arg != "--":
            supervisor_args.append(arg)
        elif not in_agent_args and arg == "--":
            in_agent_args = True
        else:
            agent_args.append(arg)

    logger.debug("supervisor_args: %s", supervisor_args)

    parser = argparse.ArgumentParser(description="Start and monitor the "
                                        "agent process")
    parser.add_argument("--updates-drop-dir", default=join(expanduser("~"),
                                                           ".pyfarm",
                                                           "agent",
                                                           "updates"),
                        help="Where to look for agent updates")
    parser.add_argument("--agent-package-dir",
                        help="Path to the actual agent code")
    parser.add_argument("--pidfile", default=join(default_data_root,
                                                  "supervisor.pid"),
                        help="The file to store the process id in. "
                             "[default: %(default)s]")
    parser.add_argument("-n", "--no-daemon", default=False, action="store_true",
                        help="If provided then do not run the process in the "
                             "background.")
    parser.add_argument("--chroot",
                        help="The directory to chroot to upon launch.")
    parser.add_argument("--uid", type=int,
                        help="The user id to run the supervisor as.  "
                             "*This setting is ignored on Windows.*")
    parser.add_argument("--gid", type=int,
                        help="The group id to run the supervisor as.  "
                             "*This setting is ignored on Windows.*")
    args = parser.parse_args(supervisor_args)

    if not args.no_daemon and fork is not NotImplemented:
        logger.info("sending supervisor log output to %s" % args.log)
        daemon_start_return_code = start_daemon_posix(
            args.log, args.chroot, args.uid, args.gid)

        if isinstance(daemon_start_return_code, INTEGER_TYPES):
            return daemon_start_return_code

    elif not args.no_daemon and fork is NotImplemented:
        logger.warning(
            "`fork` is not implemented on %s, starting in "
            "foreground" % OS.title())
    else:
        logger.debug("Not forking to background")

    pid = os.getpid()
    # Write the PID file
    try:
        with open(args.pidfile, "w") as pidfile:
            pidfile.write(str(os.getpid()))
    except OSError as e:
        logger.error(
            "Failed to write PID file %s: %s", args.pidfile, e)
        return 1
    else:
        logger.debug("Wrote PID to %s", args.pidfile)

    logger.info("supervisor pid: %s" % pid)

    if getuid is not NotImplemented:
        logger.info("uid: %s" % getuid())

    if getgid is not NotImplemented:
        logger.info("gid: %s" % getgid())

    def terminate_handler(signum, frame):
        subprocess.call(["pyfarm-agent"] + agent_args + ["stop"])
        sys.exit(0)

    def restart_handler(signum, frame):
        subprocess.call(["pyfarm-agent"] + agent_args + ["stop"])

    logger.debug("Setting signal handlers")

    signal.signal(signal.SIGTERM, terminate_handler)
    signal.signal(signal.SIGINT, terminate_handler)
    signal.signal(signal.SIGHUP, restart_handler)

    update_file_path = join(args.updates_drop_dir, "pyfarm-agent.zip")

    loop_interval = read_env_int("PYFARM_AGENT_SUPERVISOR_INTERVAL", 5)

    while True:
        if subprocess.call(["pyfarm-agent", "status"]) != 0:
            logger.info("pyfarm-agent is not running")
            if (os.path.isfile(update_file_path) and
                zipfile.is_zipfile(update_file_path)):
                logger.info("Found an upgrade to pyfarm-agent")
                try:
                    shutil.rmtree(args.agent_package_dir)
                    os.makedirs(args.agent_package_dir)
                    with zipfile.ZipFile(update_file_path, "r") as archive:
                        archive.extractall(args.agent_package_dir)
                    os.remove(update_file_path)
                except Exception as e:
                    logger.error("Caught exception trying to update agent: %r",e)

            logger.info("starting pyfarm-agent now")
            if subprocess.call(["pyfarm-agent"] + agent_args + ["start"]) != 0:
                logger.error("Could not start pyfarm-agent")
                sys.exit(1)

        time.sleep(loop_interval)
