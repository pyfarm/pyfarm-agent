# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Job Type Internals
==================

Contains classes which contain internal methods for
the :class:`pyfarm.jobtypes.core.jobtype.JobType` class.
"""

import os
import tempfile
from datetime import datetime
from os.path import dirname, isdir, join, isfile
from string import Template

try:
    import pwd
    import grp
except ImportError:  # pragma: no cover
    pwd = NotImplemented
    grp = NotImplemented

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred

from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WINDOWS, WorkState
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.http.core.client import get, http_retry_delay
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.jobtypes.core.log import STDOUT, logpool

USER_GROUP_TYPES = list(STRING_TYPES) + list(INTEGER_TYPES) + [type(None)]
ITERABLE_CONTAINERS = (list, tuple, set)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
logfile = getLogger("jobtypes.log")


class Cache(object):
    """Internal methods for caching job types"""
    cache = {}
    CACHE_DIRECTORY = Template(
        config.get("jobtype_cache_directory", "")).safe_substitute(
        temp=tempfile.gettempdir())

    if not CACHE_DIRECTORY:
        CACHE_DIRECTORY = None
        logger.warning("Job type cache directory has been disabled.")

    elif not isdir(CACHE_DIRECTORY):
        try:
            os.makedirs(CACHE_DIRECTORY)

        except OSError:
            logger.error(
                "Failed to create %r.  Job type caching is "
                "now disabled.", CACHE_DIRECTORY)
            CACHE_DIRECTORY = None

        else:
            logger.info("Created job type cache directory %r", CACHE_DIRECTORY)

    else:
        logger.debug("Job type cache directory is %r", CACHE_DIRECTORY)

    @classmethod
    def _download_jobtype(cls, name, version):
        """
        Downloads the job type specified in ``assignment``.  This
        method will pass the response it receives to :meth:`_cache_jobtype`
        however failures will be retried.
        """
        url = config["master-api"] + "/jobtypes/" + name + "/" + str(version)
        result = Deferred()
        download = lambda *_: \
            get(url,
                callback=result.callback,
                errback=lambda: reactor.callLater(http_retry_delay(), download))
        download()
        return result

    @classmethod
    def _cache_jobtype(cls, cache_key, jobtype):
        """
        Once the job type is downloaded this classmethod is called
        to store it on disk.  In the rare even that we fail to write it
        to disk, we store it in memory instead.
        """
        filename = str(join(
            cls.CACHE_DIRECTORY,
            "_".join(map(str, cache_key)) + "_" + jobtype["classname"] + ".py"))
        success = Deferred()

        def write_to_disk(filename):
            try:
                os.makedirs(dirname(filename))
            except (IOError, OSError):
                pass

            if isfile(filename):
                logcache.debug("%s is already cached on disk", filename)
                jobtype.pop("code", None)
                return filename, jobtype

            try:
                with open(filename, "w") as stream:
                    stream.write(jobtype["code"])

                jobtype.pop("code", None)
                return filename, jobtype

            # If the above fails, use a temp file instead
            except (IOError, OSError) as e:
                fd, tmpfilepath = tempfile.mkstemp(suffix=".py")
                logcache.warning(
                    "Failed to write %s, using %s instead: %s",
                    filename, tmpfilepath, e)

                with open(tmpfilepath, "w") as stream:
                    stream.write(jobtype["code"])

                jobtype.pop("code", None)
                return tmpfilepath, jobtype

        def written_to_disk(results):
            filename, jobtype = results
            cls.cache[cache_key] = (jobtype, filename)
            logcache.info("Created cache for %r at %s", cache_key, filename)
            success.callback((jobtype, filename))

        def failed_to_write_to_disk(error):
            logcache.error(
                "Failed to write job type cache to disk, will use "
                "memory instead: %s", error)

            # The code exists in the job type because it's
            # only removed on success.
            cls.cache[cache_key] = (jobtype, None)
            success.callback((jobtype, None))

        # Defer the write process to a thread so we don't
        # block the reactor if the write is slow
        writer = threads.deferToThread(write_to_disk, filename)
        writer.addCallbacks(written_to_disk, failed_to_write_to_disk)
        return success


class Process(object):
    """Methods related to process control and management"""
    logging = {}

    def _start(self):
        return self.start()

    def _stop(self):
        return self.stop()

    def _get_uid_gid(self, user, group):
        uid = None
        gid = None

        # Convert user to uid
        if all([user is not None, pwd is not NotImplemented]):
            uid = self._get_uid_gid_value(
                user, "username", "get_uid", pwd, "pwd")

        # Convert group to gid
        if all([group is not None, grp is not NotImplemented]):
            gid = self._get_uid_gid_value(
                group, "group", "get_gid", grp, "grp")

        return uid, gid

    # TODO: set state
    def _process_started(self, protocol):
        """
        Internal implementation for :meth:`process_started`.

        This method logs the start of a process and informs the master of
        the state change.
        """
        logger.info("%r started", protocol)
        logpool.log(protocol.uuid, STDOUT, "Started %r" % protocol)

    def _process_stopped(self, protocol, reason):
        """
        Internal implementation for :meth:`process_stopped`.

        If ``--capture-process-output`` was set when the agent was launched
        all standard output from the process will be sent to the stdout
        of the agent itself.  In all other cases we send the data to the
        logger pool so it can be stored in a file without blocking the
        event loop.
        """
        logger.info("%r stopped (code: %r)", protocol, reason.value.exitCode)

        if self.is_successful(reason):
            logpool.log(
                protocol.uuid, STDOUT,
                "Process has terminated successfully, code %s" %
                reason.value.exitCode)
        else:
            self.failed_processes.append((protocol, reason))
            logpool.log(
                protocol.uuid, STDOUT,
                "Process has not terminated successfully, code %s" %
                reason.value.exitCode)

        # pop off the protocol and thread since the process has terminated
        protocol = self.protocols.pop(protocol.id)
        thread = self.logging.pop(protocol.id)
        thread.stop()

        # If this was the last process running
        # TODO: sequential processes
        if not self.protocols:
            if not self.failed_processes:
                self.deferred.callback(reason)
                for task in self.assignment["tasks"]:
                    self.set_task_state(task, WorkState.DONE, reason)
            else:
                self.deferred.errback()
                for task in self.assignment["tasks"]:
                    self.set_task_state(task, WorkState.FAILED, reason)

    def _get_uid_gid_value(
            self, value, value_name, func_name, module, module_name):
        """
        Internal function which handles both user name and group conversion.
        """
        if not isinstance(value, STRING_TYPES):
            raise TypeError("Expected string for `value`")

        # This platform does not implement the module
        if module is NotImplemented:
            logger.warning(
                "This platform does not implement the %r module, skipping "
                "%s()", module_name, func_name)

        # Convert a user/group string to an integer
        elif isinstance(value, STRING_TYPES):
            try:
                if module_name == "pwd":
                    return pwd.getpwnam(value).pw_uid
                elif module_name == "grp":
                    return grp.getgrnam(value).gr_gid
                else:
                    raise ValueError(
                        "Internal error, failed to get module to use for "
                        "conversion.  Was given %r" % module)
            except KeyError:
                logger.error(
                    "Failed to convert %s to a %s",
                    value, func_name.split("_")[1])

                if not config.get("jobtype_ignore_id_mapping_errors"):
                    raise

        # Verify that the provided user/group string is real
        elif isinstance(value, INTEGER_TYPES):
            try:
                if module_name == "pwd":
                    pass
                elif module_name == "grp":
                    pass
                else:
                    raise ValueError(
                        "Internal error, failed to get module to use for "
                        "conversion.  Was given %r" % module)

                # Seems to check out, return the original value
                return value
            except KeyError:
                logger.error(
                    "%s %s does not seem to exist", value_name, value)

                if not config.get("jobtype_ignore_id_mapping_errors"):
                    raise
        else:
            raise ValueError(
                "Expected an integer or string for `%s`" % value_name)


class TypeChecks(object):
    def _check_spawn_process_inputs(
            self, command, arguments, working_dir, environment, user, group):
        """Checks input arguments for :meth:`spawn_process"""
        if not isinstance(command, STRING_TYPES):
            raise TypeError("Expected a string for `command`")

        if not isinstance(arguments, (list, tuple)):
            raise TypeError("Expected a list or tuple for `arguments`")

        if isinstance(working_dir, STRING_TYPES) \
                and not isdir(working_dir):
            raise OSError(
                "`working_directory` %s does not exist" % working_dir)

        elif working_dir is not None:
            raise TypeError("Expected a string for `working_directory`")

        if not isinstance(environment, dict):
            raise TypeError("Expected a dictionary for `environment`")

        if not isinstance(user, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `user`")

        if not isinstance(group, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `group`")

        admin = is_administrator()

        if WINDOWS:
            if user is not None:
                logger.warning("`user` is ignored on Windows")

            if group is not None:
                logger.warning("`group` is ignored on Windows")

        elif user is not None or group is not None and not admin:
            raise EnvironmentError(
                "Cannot change user or group without being admin.")

    def _check_expandvars_inputs(self, value, environment):
        """Checks input arguments for :meth:`expandvars`"""
        if not isinstance(value, STRING_TYPES):
            raise TypeError("Expected a string for `value`")

        if environment is not None or not isinstance(environment, dict):
            raise TypeError("Expected None or a dictionary for `environment`")

    def _check_map_path_inputs(self, path):
        """Checks input arguments for :meth:`map_path`"""
        if not isinstance(path, STRING_TYPES):
            raise TypeError("Expected string for `path`")

    def _check_csvlog_path_inputs(self, tasks, now):
        """Checks input arguments for :meth:`get_csvlog_path`"""
        if not isinstance(tasks, ITERABLE_CONTAINERS):
            raise TypeError("Expected tuple, list or set for `tasks`")

        if now is not None and not isinstance(now, datetime):
            raise TypeError("Expected None or datetime for `now`")

    def _check_command_list_inputs(self, cmdlist):
        """Checks input arguments for :meth:`get_command_list`"""
        if not isinstance(cmdlist, (tuple, list)):
            raise TypeError("Expected tuple or list for `cmdlist`")

    def _check_set_states_inputs(self, tasks, state):
        """Checks input arguments for :meth:`set_states`"""
        if not isinstance(tasks, ITERABLE_CONTAINERS):
            raise TypeError("Expected tuple, list or set for `tasks`")

        if state not in WorkState:
            raise ValueError("Expected `state` to be in %s" % list(WorkState))
