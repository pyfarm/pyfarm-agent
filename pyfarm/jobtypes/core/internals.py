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

import imp
import os
import sys
import tempfile
from errno import EEXIST
from datetime import datetime
from os.path import dirname, isdir, join, isfile
from string import Template
from uuid import UUID

try:
    from httplib import OK, INTERNAL_SERVER_ERROR
except ImportError:  # pragma: no cover
    from http.client import OK, INTERNAL_SERVER_ERROR

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

USER_GROUP_TYPES = tuple(
    list(STRING_TYPES) + list(INTEGER_TYPES) + [type(None)])
ITERABLE_CONTAINERS = (list, tuple, set)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
logfile = getLogger("jobtypes.log")


class Cache(object):
    """Internal methods for caching job types"""
    cache = {}
    JOBTYPE_VERSION_URL = \
        "%(master_api)s/jobtypes/%(name)s/versions/%(version)s"
    CACHE_DIRECTORY = config.get("jobtype_cache_directory", "")

    if not CACHE_DIRECTORY:  # pragma: no cover
        CACHE_DIRECTORY = None  # make sure it's None
        logger.warning("Job type cache directory has been disabled.")

    else:
        try:
            os.makedirs(CACHE_DIRECTORY)

        except OSError as e:  # pragma: no cover
            if e.errno != EEXIST:
                logger.error(
                    "Failed to create %r.  Job type caching is "
                    "now disabled.", CACHE_DIRECTORY)
                raise
        else:
            logger.info("Created job type cache directory %r", CACHE_DIRECTORY)

        logger.debug("Job type cache directory is %r", CACHE_DIRECTORY)

    @classmethod
    def _download_jobtype(cls, name, version):
        """
        Downloads the job type specified in ``assignment``.  This
        method will pass the response it receives to :meth:`_cache_jobtype`
        however failures will be retried.
        """
        logger.debug("Downloading job type %r version %s", name, version)
        url = str(cls.JOBTYPE_VERSION_URL % {
            "master_api": config["master_api"],
            "name": name, "version": version})

        result = Deferred()
        download = lambda *_: \
            get(url,
                callback=result.callback,
                errback=lambda: reactor.callLater(http_retry_delay(), download))
        download()
        return result

    @classmethod
    def _cache_filepath(cls, cache_key, classname, version):
        return str(join(
            cls.CACHE_DIRECTORY, "%s_%s_v%s.py" % (
                cache_key, classname, version)))

    @classmethod
    def _cache_key(cls, assignment):
        return assignment["jobtype"]["name"], assignment["jobtype"]["version"]

    @classmethod
    def _jobtype_download_complete(cls, response, cache_key):
        result = Deferred()

        # Server is offline or experiencing issues right
        # now so we should retry the request.
        if response.code >= INTERNAL_SERVER_ERROR:
            return reactor.callLater(
                http_retry_delay(),
                response.request.retry)

        downloaded_data = response.json()

        if not config["jobtype_enable_cache"]:
            deferred = cls._load_jobtype(downloaded_data, None)
            deferred.chainDeferred(result)
        else:
            # When the download is complete, cache the results
            caching = cls._cache_jobtype(cache_key, downloaded_data)
            caching.addCallback(
                lambda result: cls._load_jobtype(*result))
            caching.chainDeferred(result)
        return result


    @classmethod
    def _cache_jobtype(cls, cache_key, jobtype):
        """
        Once the job type is downloaded this classmethod is called
        to store it on disk.  In the rare even that we fail to write it
        to disk, we store it in memory instead.
        """
        if isinstance(cache_key, tuple):
            cache_key = cache_key[0]

        assert isinstance(cache_key, STRING_TYPES)
        assert isinstance(jobtype, dict)
        filename = cls._cache_filepath(
            cache_key, jobtype["classname"], jobtype["version"])
        success = Deferred()
        jobtype = jobtype.copy()

        def write_to_disk(filename):
            parent_dir = dirname(filename)
            try:
                os.makedirs(parent_dir)
            except (IOError, OSError) as e:  # pragma: no cover
                if e.errno != EEXIST:
                    logger.error("Failed to create %s: %s", parent_dir, e)
            else:
                logger.debug("Created %s", parent_dir)

            if isfile(filename):  # pragma: no cover
                logcache.warning("%s is already cached on disk", filename)
                jobtype.pop("code", None)
                return jobtype, filename

            try:
                with open(filename, "w") as stream:
                    stream.write(jobtype["code"])

            # If the above fails, use a temp file instead
            except (IOError, OSError) as e:  # pragma: no cover
                _, tmpfilepath = tempfile.mkstemp(suffix=".py")
                logcache.warning(
                    "Failed to write %s, using %s instead: %s",
                    filename, tmpfilepath, e)

                with open(tmpfilepath, "w") as stream:
                    stream.write(jobtype["code"])

                jobtype.pop("code", None)
                return jobtype, tmpfilepath

            else:
                logger.debug(
                    "Wrote job type %s version %s to %s",
                    jobtype["name"], jobtype["version"], filename)
                jobtype.pop("code", None)
                return jobtype, filename

        def written_to_disk(results):
            jobtype, filename = results
            cls.cache[cache_key] = (jobtype, filename)
            logcache.info("Created cache for %r at %s", cache_key, filename)
            success.callback((jobtype, filename))

        def failed_to_write_to_disk(error):  # pragma: no cover
            logcache.error(
                "Failed to write job type cache to disk, will use "
                "memory instead: %s", error)

            # The code exists in the job type because it's
            # only removed on success.
            cls.cache[cache_key] = (jobtype, None)
            success.callback((jobtype, None))

        # Defer the write process to a thread so we don't
        # block the reactor if the write is slow
        logger.debug(
            "Caching job type %s version %s to %s",
            jobtype["classname"], jobtype.get("version", "?"), filename)
        writer = threads.deferToThread(write_to_disk, filename)
        writer.addCallbacks(written_to_disk, failed_to_write_to_disk)
        return success

    @classmethod
    def _module_for_jobtype(cls, jobtype):
        return "pyfarm.jobtypes.cached.%s%s%s" % (
            jobtype["classname"], jobtype["name"], jobtype["version"])

    @classmethod
    def _load_jobtype(cls, jobtype, filepath):
        def load_jobtype(jobtype_data, path):
            module_name = cls._module_for_jobtype(jobtype_data)

            # Create or load the module
            if filepath is not None:
                module = imp.load_source(module_name, path)
            else:
                logcache.warning(
                    "Loading (%s, %s) directly from memoy",
                    jobtype_data["name"], jobtype_data["version"])
                module = imp.new_module(module_name)
                exec jobtype_data["code"] in module.__dict__
                sys.modules[module_name] = module

            return getattr(module, jobtype_data["classname"])

        # Load the job type itself in a thread so we limit disk I/O
        # and other blocking issues in the main thread.
        return threads.deferToThread(load_jobtype, jobtype, filepath)


class Process(object):
    """Methods related to process control and management"""
    logging = {}

    def _start(self):
        def start_assignment(_):
            # Make sure _start() is not called twice
            if self.start_called:
                raise RuntimeError("%s has already been started" % self)
            else:
                logger.debug("Calling start() for assignment %s", self)
                self._before_start()
                self.start()
                self.start_called = True
                self.started_deferred.callback(None)

        self.started_deferred = Deferred()
        self.stopped_deferred = Deferred()
        reactor.callLater(0, start_assignment, None)
        return self.started_deferred, self.stopped_deferred

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

    def _before_start(self):
        logger.info("%r.before_start()", self)
        self.before_start()

    def _process_started(self, protocol):
        """
        Called by :meth:`.ProcessProtocol.connectionMade` when a process has
        started running.
        """
        logger.info("%r started", protocol)
        logpool.log(protocol.uuid, STDOUT, "Started %r" % protocol)
        proess_data = self.processes[protocol.uuid]
        proess_data.started.callback(protocol)
        self.process_started(protocol)

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
        process_data = self.processes.pop(protocol.uuid)

        if self.is_successful(reason):
            logpool.log(
                protocol.uuid, STDOUT,
                "Process has terminated successfully, code %s" %
                reason.value.exitCode)
        else:
            self.failed_processes.add((protocol, reason))
            logpool.log(
                protocol.uuid, STDOUT,
                "Process has not terminated successfully, code %s" %
                reason.value.exitCode)

        self.process_stopped(protocol, reason)
        logpool.close_log(protocol)
        process_data.stopped.callback(reason)

        # If there are no processes running at this point, we assume
        # the assign is finished
        if len(self.processes) == 0:
            # TODO Mark tasks that have not yet been marked otherwise as FAILED
            if not self.failed_processes:
                logger.info("Processes in assignment %s stopped, no failures",
                            self)
            else:
                logger.warning("There was at least one failed process in "
                               "assignment %s", self)
            self.stopped_deferred.callback(None)

    # complete coverage provided by other tests
    def _get_uid_gid_value(self, value, value_name, func_name,
                           module, module_name):  # pragma: no cover
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

        if isinstance(working_dir, STRING_TYPES):
            if not isdir(working_dir):
                raise OSError(
                    "`working_dir` %s does not exist" % working_dir)
        else:
            raise TypeError("Expected a string for `working_dir`")

        if not isinstance(environment, dict):
            raise TypeError("Expected a dictionary for `environment`")

        if not isinstance(user, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `user`")

        if not isinstance(group, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `group`")

        admin = is_administrator()

        if WINDOWS:  # pragma: no cover
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

        if environment is not None and not isinstance(environment, dict):
            raise TypeError("Expected None or a dictionary for `environment`")

    def _check_map_path_inputs(self, path):
        """Checks input arguments for :meth:`map_path`"""
        if not isinstance(path, STRING_TYPES):
            raise TypeError("Expected string for `path`")

    def _check_csvlog_path_inputs(self, protocol_uuid, now):
        """Checks input arguments for :meth:`get_csvlog_path`"""
        if not isinstance(protocol_uuid, UUID):
            raise TypeError("Expected UUID for `protocol_uuid`")

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
