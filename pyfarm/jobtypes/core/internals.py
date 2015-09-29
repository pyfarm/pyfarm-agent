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
import time
from collections import namedtuple
from errno import EEXIST
from datetime import datetime
from os.path import dirname, join, isfile, basename
from uuid import UUID
from functools import partial

try:
    import pwd
except ImportError:  # pragma: no cover
    pwd = NotImplemented

try:
    import grp
except ImportError:  # pragma: no cover
    grp = NotImplemented

try:
    from httplib import (
        OK, INTERNAL_SERVER_ERROR, CREATED, ACCEPTED, CONFLICT, NOT_FOUND,
        BAD_REQUEST)
except ImportError:  # pragma: no cover
    from http.client import (
        OK, INTERNAL_SERVER_ERROR, CREATED, ACCEPTED, CONFLICT, NOT_FOUND,
        BAD_REQUEST)

from psutil import disk_usage

from twisted.internet import reactor, threads
from twisted.internet.defer import (
    Deferred, DeferredList, succeed, inlineCallbacks, returnValue)
from twisted.web._newclient import (
    ResponseNeverReceived, RequestTransmissionFailed)

import treq

from pyfarm.core.enums import WINDOWS, INTEGER_TYPES, STRING_TYPES, WorkState
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.http.core.client import (
    get_direct, get, post, http_retry_delay)
from pyfarm.agent.utility import remove_file, remove_directory
from pyfarm.jobtypes.core.log import STDOUT, STDERR, logpool
from pyfarm.jobtypes.core.process import ReplaceEnvironment, ProcessProtocol

USER_GROUP_TYPES = tuple(
    list(STRING_TYPES) + list(INTEGER_TYPES) + [type(None)])
ITERABLE_CONTAINERS = (list, tuple, set)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
logfile = getLogger("jobtypes.log")
ProcessData = namedtuple(
    "ProcessData", ("protocol", "started", "stopped"))


class InsufficientSpaceError(Exception):
    pass


class JobTypeDownloadError(Exception):
    """
    Raised when there is some problem download a job type.
    """


class JobTypeNotFound(JobTypeDownloadError):
    """
    Raised when we fail to download a job type because
    it no longer exists on the master.
    """
    def __init__(self, name, version):
        super(JobTypeNotFound, self).__init__(
            "Job type %s v%s not found" % (name, version))
        self.name = name
        self.version = version


class JobTypeLoader(object):
    """Class for retrieval, loading and caching of job types."""
    def __init__(self):
        if not config["jobtype_enable_cache"]:
            cache_directory = None
        else:
            try:
                cache_directory = config["jobtype_cache_directory"]

                # Include the farm name in the config path as well.  This
                # should prevent the wrong cache from being used if an agent
                # switches farms.
                farm_name = config.get("farm_name")
                if farm_name is not None and farm_name:
                    cache_directory = join(cache_directory, farm_name)

            except KeyError:
                cache_directory = None

            if not cache_directory or (
                isinstance(cache_directory, STRING_TYPES)
                    and not cache_directory.strip()):
                logger.warning("Cache directory is blank, disabling cache.")
                cache_directory = None

        if cache_directory is not None:
            try:
                os.makedirs(cache_directory)
            except OSError as e:  # pragma: no cover
                if e.errno != EEXIST:
                    logger.error(
                        "Failed to create %r.  Job type caching is "
                        "now disabled.", cache_directory)
                    raise

        self.cache_directory = cache_directory

    def cache_path(self, name, version):
        """
        Returns the path to create a cache file for the given job type
        name and version.  This function will return None if caching is
        currently disabled.

        :param str name:
            Name of the job type to cache
        :param str version:
            Version of the job type to cache
        """
        if self.cache_directory:
            return join(
                self.cache_directory,
                "{name}_{version}.py".format(name=name, version=version))

    @inlineCallbacks
    def load(self, name, version):
        """
        Main method used by a job type to load the job type class.  Internally
        this handles retrieval of the job type either from the cache or from
        the master.  This will also cache the job type we retrieve from the
        master if caching is enabled.

        :param str name:
            The name of the job type to load and return.

        :param str version:
            The version of the job type to load and return.
        """
        source_code = yield self.cached_source(name, version)

        # Caching may be disabled if source_code is still None
        if source_code is None:
            source_code = yield self.download_source(name, version)
            yield self.write_cache(name, version, source_code)



    @inlineCallbacks
    def download_source(self, name, version):
        """
        Downloads and returns the source code for the given name and version
        of a job type.

        :param str name:
            The name of the job type to download the source code for.

        :param str version:
            The version of the job type to download the source code for.
        """
        url = "{master_api}/jobtypes/{name}/versions/{version}".format(
            master_api=config["master_api"], name=name, version=version
        )
        logger.debug("Downloading %s", url)

        while True:
            try:
                response = yield get_direct(url)
            except Exception as error:
                logger.error(
                    "Failed to download %s: %s. Request will be retried.",
                    url, error)
                delay = Deferred()
                reactor.callLater(http_retry_delay(), delay.callback, None)
                yield delay
            else:
                if response.code == OK:
                    data = yield treq.json_content(response)
                    returnValue(data)

                elif response.code == NOT_FOUND:
                    raise JobTypeNotFound(name, version)

                # Server is offline or experiencing issues right
                # now so we should retry the request.
                elif response.code >= INTERNAL_SERVER_ERROR:
                    logger.debug(
                        "Could not download job type, response %r while "
                        "downloading %s.  Request will be retried.",
                        response.code, url
                    )
                    delay = Deferred()
                    reactor.callLater(http_retry_delay(), delay.callback, None)
                    yield delay

                # If we got a bad request coming back from the
                # master we've done something wrong.  Don't
                # retry the request because we shouldn't
                # expect the response code to change with
                # a retry.
                elif response.code >= BAD_REQUEST:
                    raise JobTypeDownloadError(
                        "HTTP %s error when downloading %s" % (
                            response.code, url))

                else:
                    logger.warning(
                        "Unknown response %r while downloading %s.  Request "
                        "will be retried.", response.code, url)
                    delay = Deferred()
                    reactor.callLater(http_retry_delay(), delay.callback, None)
                    yield delay

    @inlineCallbacks
    def cached_source(self, name, version):
        """
        Searches for and returns the cached ource code for the given name
        and version of a job type.  Returns None if the job type is not
        current cached or of caching is disabled.

        :param str name:
            The name of the job type to return the cached entry for.

        :param str version:
            The version of the job type to return the cached entry for.
        """
        cache_path = self.cache_path(name, version)

        if not self.cache_directory or not isfile(cache_path):
            returnValue(None)

        stream = yield threads.deferToThread(open, cache_path, "rb")
        try:
            data = yield threads.deferToThread(stream.read)
        finally:
            stream.close()
        returnValue(data)

    @inlineCallbacks
    def write_cache(self, name, version, source_code):
        """
        Writes the given ``source_code`` to the disk cache for the given
        job type name and version.

        :param str name:
            The name of the job type we're writing a cache for.

        :param str version:
            The version of the job type we're writing a cache for.

        :param str source_code:
            The source code of the job type we're writing a cache for.

        :returns:
            This function does not return anything.
        """
        if not self.cache_directory:
            returnValue(None)

        error = False
        cache_path = self.cache_path(name, version)
        output = yield threads.deferToThread(open, cache_path, "wb")

        # Write the data
        try:
            yield threads.deferToThread(output.write, source_code)
        except Exception as error:
            logger.error(
                "Failed to write %r v%s to %s: %s", name, version, error)
            error = True
            raise
        finally:
            output.close()

            if error:
                remove_file(output.name, raise_=False)


class CacheOld(object):
    """Internal methods for caching job types"""

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
                logcache.debug("%s is already cached on disk", filename)
                jobtype.pop("code", None)
                return jobtype, filename

            try:
                with open(filename, "w") as stream:
                    stream.write(jobtype["code"])

            # If the above fails, use a temp file instead
            except (IOError, OSError) as e:  # pragma: no cover
                fd, tmpfilepath = tempfile.mkstemp(suffix=".py")
                logcache.warning(
                    "Failed to write %s, using %s instead: %s",
                    filename, tmpfilepath, e)

                with os.fdopen(fd, "w") as stream:
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
                logger.debug("Attempting to load module from file path %s",
                             filepath)
                try:
                    module = imp.load_source(module_name, path)
                except Exception as e:
                    type = sys.exc_info()[0]
                    value = sys.exc_info()[1]
                    logger.error("Importing module from jobtype file failed: "
                                 "%s, value: %s", type, value)
                    raise
            else:
                logcache.warning(
                    "Loading (%s, %s) directly from memory",
                    jobtype_data["name"], jobtype_data["version"])

                module = imp.new_module(module_name)
                exec jobtype_data["code"] in module.__dict__
                sys.modules[module_name] = module

            logger.debug("Returning class %s from module",
                         jobtype_data["classname"])
            return getattr(module, jobtype_data["classname"])

        # Load the job type itself in a thread so we limit disk I/O
        # and other blocking issues in the main thread.
        return threads.deferToThread(load_jobtype, jobtype, filepath)


class Process(object):
    """Methods related to process control and management"""
    logging = {}

    def __init__(self):
        self.start_called = False
        self.stop_called = False

    def _before_spawn_process(self, command, protocol):
        logger.debug(
            "%r._before_spawn_process(%r, %r)", self, command, protocol)
        self.before_spawn_process(command, protocol)

    def _spawn_twisted_process(
            self, command, process_protocol, kwargs):
        """
        Handles the spawning of the process itself using
        :func:`reactor.spawnProcess`.

        :param tuple _:
            An ignored argument containing the protocol id and
            csv log file
        """
        self._before_spawn_process(command, process_protocol)

        # The way Twisted handles the env keyword varies by platform.  To
        kwargs.setdefault("env", None)
        if kwargs["env"] is not None:
            raise RuntimeError(
                "The `env` keyword should always be set to None.")

        with ReplaceEnvironment(command.env):
            try:
                reactor.spawnProcess(process_protocol, command.command, **kwargs)
            except Exception as e:
                logger.error("Exception on starting process: %s", e)
                self.processes.pop(process_protocol.uuid).stopped.callback(None)
                self.failed_processes.add((process_protocol, e))

                # If there are no processes running at this point, we assume
                # the assignment is finished
                if len(self.processes) == 0:
                    logger.error("There was at least one failed process in the "
                                 "assignment %s", self)

                for task in self.assignment["tasks"]:
                    if task["id"] not in self.finished_tasks:
                        self.set_task_state(task, WorkState.FAILED)
                    else:
                        logger.info(
                            "Task %r is already in finished tasks, not setting "
                            "state to %s", task["id"], WorkState.FAILED)
                    self.stopped_deferred.callback(None)

    def _start(self):
        """
        The internal method that gets called to start the job type.  Usually
        this calls :meth:`start` though more advanced behaviors could
        override this and :meth`start`.

        .. warning::

            Read the source code before overriding this method on your
            own.  This method sets a couple of instance variables and returns
            a tuple that is relied upon elsewhere.
        """
        # Make sure _start() is not called twice
        if self.start_called:
            raise RuntimeError("%s has already been started" % self)

        log_path = self.get_csvlog_path(self.uuid)
        logpool.open_log(self.uuid, log_path)
        self.log_identifier = basename(log_path)

        register_log_deferred = self._register_logfile_on_master(
            self.log_identifier)

        self.started_deferred = Deferred()
        self.stopped_deferred = Deferred()

        def start_processes(_):
            logpool.log(self.uuid, "internal",
                        "Starting work on job %s, assignment of %s tasks." %
                        (self.assignment["job"]["title"],
                        len(self.assignment["tasks"])))

            self._before_start()
            logger.debug("%r.start()", self.__class__.__name__)
            try:
                self.start()
                self.start_called = True
                logger.debug("Collecting started deferreds from spawned "
                             "processes")

                if not self.processes:
                    logger.warning(
                        "No processes have been started, firing deferreds "
                        "immediately.")
                    self.started_deferred.callback(None)
                    self.stopped_deferred.callback(None)
                else:
                    logger.debug("Making deferred list for %s started "
                                 "processes", len(self.processes))
                    processes_deferred = DeferredList(
                        [process.started for process in self.processes.values()])
                    processes_deferred.addCallback(
                        lambda x: self.started_deferred.callback(x))
            except Exception as e:
                self.started_deferred.errback(e)
                self.stopped_deferred.errback(e)

        register_log_deferred.addCallback(start_processes)
        register_log_deferred.addErrback(
            lambda x: self.started_deferred.errback(x))
        register_log_deferred.addErrback(
            lambda x: self.stopped_deferred.errback(x))

        return self.started_deferred, self.stopped_deferred

    def _stop(self):
        if self.stop_called:
            raise RuntimeError("%s has already been stopped" % self)

        return self.stop()

    def _before_start(self):
        logger.debug("%r._before_start()", self)
        self.before_start()

    def _process_started(self, protocol):
        """
        Called by :meth:`.ProcessProtocol.connectionMade` when a process has
        started running.
        """
        logger.debug("%r._process_started(%r)", self, protocol)
        logpool.log(self.uuid, "internal", "Started %r" % protocol,
                    protocol.pid)
        process_data = self.processes[protocol.uuid]
        process_data.started.callback(protocol)
        if not self.stop_called:
            self.process_started(protocol)
        else:
            self.stop()

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

        try:
            successful = self.is_successful(protocol, reason)
        except Exception as e:
            message = ("Exception caught from is_successful(): %r. "
                       "Assuming not successful." % e)
            logger.error(message)
            self._log(message)
            successful = False
        if successful:
            logpool.log(
                self.uuid, "internal",
                "Process has terminated successfully, code %s" %
                reason.value.exitCode, protocol.pid)
        else:
            self.failed_processes.add((protocol, reason))
            logpool.log(
                self.uuid, "internal",
                "Process has not terminated successfully, code %s" %
                reason.value.exitCode, protocol.pid)

        try:
            self.process_stopped(protocol, reason)
        except Exception as e:
            logger.error("Exception caught from process_stopped: %s", e)
        process_data.stopped.callback(reason)

        # If there are no processes running at this point, we assume
        # the assignment is finished
        if len(self.processes) == 0:
            self.stopped_deferred.callback(None)
        return succeed([])

    def _spawn_process(self, command):
        """
        Starts one child process using input from :meth:`command_data`.
        Job types should never start child processes through any other
        means.  The only exception to this rule is code that resides in
        :meth:`prepare_for_job`, which should use
        :meth:`spawn_persistent_job_process` instead.

        :raises OSError:
            Raised if `working_dir` was provided but the provided
            path does not exist

        :raises EnvironmentError:
            Raised if an attempt is made to change the user or
            group without root access.  This error will only occur on
            Linux or Unix platforms.
        """
        process_protocol = self.PROCESS_PROTOCOL(self)

        process_protocol.id = getattr(command, "id", None)

        if not isinstance(process_protocol, ProcessProtocol):
            raise TypeError("Expected ProcessProtocol for `protocol`")

        # The first argument should always be the command name by convention.
        # Under Windows, this needs to be the whole path, under POSIX only the
        # basename.
        if WINDOWS:
            arguments = [command.command] + list(command.arguments)
        else:
            arguments = [basename(command.command)] + list(command.arguments)

        # WARNING: `env` should always be None to ensure the same operation
        # of the environment setup across platforms.  See Twisted's
        # documentation for more information on why `env` should be None:
        #    http://twistedmatrix.com/documents/current/api/
        #    twisted.internet.interfaces.IReactorProcess.spawnProcess.html
        kwargs = {"args": arguments, "env": None}
        uid, gid = self.get_uid_gid(command.user, command.group)

        if uid is not None:
            kwargs.update(uid=uid)

        if gid is not None:
            kwargs.update(gid=gid)

        # Capture the protocol instance so we can keep track
        # of the process we're about to spawn.
        self.processes[process_protocol.uuid] = ProcessData(
            protocol=process_protocol, started=Deferred(), stopped=Deferred())

        return self._spawn_twisted_process(command, process_protocol, kwargs)

    def _process_output(self, protocol, output, stream):
        """
        Called by :meth:`.ProcessProtocol.outReceived` and
        :meth:`.ProcessProtocol.errReceived` whenever output is produced
        by a process.  This method will wire up the proper calls under the
        hood to process the output.
        """
        if stream == STDOUT:
            line_fragments = self._stdout_line_fragments
            line_handler = self.handle_stdout_line

        elif stream == STDERR:
            line_fragments = self._stderr_line_fragments
            line_handler = self.handle_stderr_line

        else:
            raise ValueError("Expected STDOUT or STDERR for `stream`")

        self.process_output(protocol, output, line_fragments, line_handler)

    def _has_running_processes(self):
        """
        Internal functionto determine whether the batch represented by this
        instance still has running child processes.
        """
        for process in self.processes.values():
            if process.protocol.running():
                return True

        return False

    def _register_logfile_on_master(self, log_path):
        def post_logfile(task, log_path, post_deferred=None, num_retry_errors=0,
                         delay=0):
            deferred = post_deferred or Deferred()
            url = "%s/jobs/%s/tasks/%s/attempts/%s/logs/" % (
                config["master_api"], self.assignment["job"]["id"], task["id"],
                task["attempt"])
            data = {"identifier": log_path,
                    "agent_id": self.node()["id"]}
            post_func = partial(
                post, url, data=data,
                callback=lambda x: result_callback(task, log_path, deferred, x),
                errback=lambda x: error_callback(task, log_path, deferred,
                                                 num_retry_errors, x))
            reactor.callLater(delay, post_func)
            return deferred

        def result_callback(task, log_path, deferred, response):
            if 500 <= response.code < 600:
                delay = http_retry_delay()
                logger.error(
                    "Server side error while registering log file %s for "
                    "task %s (frame %s) in job %s (id %s), status code: %s. "
                    "Retrying in %s seconds",
                    log_path, task["id"], task["frame"],
                    self.assignment["job"]["title"],
                    self.assignment["job"]["id"], response.code, delay)
                post_logfile(task, log_path, post_deferred=deferred,
                             delay=delay)

            # The server will return CONFLICT if we try to register a logfile
            # twice.
            elif response.code not in [OK, CONFLICT, CREATED]:
                # Nothing else we could do about that, this is
                # a problem on our end.
                logger.error(
                    "Could not register logfile %s for task %s (frame %s) in "
                    "job %s (id %s), status code: %s. This is a client side "
                    "error, giving up.",
                    log_path, task["id"], task["frame"],
                    self.assignment["job"]["title"],
                    self.assignment["job"]["id"], response.code)
                deferred.errback(None)

            else:
                logger.info("Registered logfile %s for task %s on master",
                            log_path, task["id"])
                deferred.callback(None)

        def error_callback(task, log_path, deferred, num_retry_errors,
                           failure_reason):
            if num_retry_errors > config["broken_connection_max_retry"]:
                logger.error(
                    "Error while registering logfile %s for task %s on master. "
                    "Maximum number of retries reached. Not retrying the "
                    "request.", log_path, task["id"])
                deferred.errback(None)
            else:
                if (failure_reason.type in
                    (ResponseNeverReceived, RequestTransmissionFailed)):
                    logger.debug(
                        "Error while registering logfile %s for task %s on "
                        "master: %s, retrying immediately",
                        log_path, task["id"], failure_reason.type.__name__)
                    post_logfile(task, log_path, post_deferred=deferred)
                else:
                    delay = http_retry_delay()
                    logger.error(
                        "Error while registering logfile %s for task %s on "
                        "master: %r, retrying in %s seconds.",
                        log_path, task["id"], failure_reason, delay)
                    post_logfile(task, log_path, post_deferred=deferred,
                                 delay=delay)

        deferreds = []
        for task in self.assignment["tasks"]:
            deferreds.append(post_logfile(task, log_path))

        return DeferredList(deferreds)

    def _upload_logfile(self):
        path = join(config["jobtype_task_logs"], self.log_identifier)
        url = "%s/jobs/%s/tasks/%s/attempts/%s/logs/%s/logfile" % (
                config["master_api"], self.assignment["job"]["id"],
                self.assignment["tasks"][0]["id"],
                self.assignment["tasks"][0]["attempt"],
                self.log_identifier)
        upload_deferred = Deferred()

        def upload(url, log_identifier, delay=0):
            logfile = open(path, "rb")
            if delay != 0:
                reactor.callLater(delay, upload, url,
                                  log_identifier=log_identifier)
            else:
                # FIXME persistent=False is a workaround to help with some
                # problems in unit testing.
                deferred = treq.put(url=url, data=logfile,
                                    headers={"Content-Type": ["text/csv"]},
                                    persistent=False)
                deferred.addCallback(lambda x: result_callback(
                    url, log_identifier, x))
                deferred.addErrback(lambda x: error_callback(
                    url, log_identifier, x))

        def result_callback(url, log_identifier, response):
            if 500 <= response.code < 600:
                delay = http_retry_delay()
                logger.error(
                    "Server side error while uploading log file %s, "
                    "status code: %s. Retrying. in %s seconds",
                    log_identifier, response.code, delay)
                upload(url, log_identifier, delay=delay)

            elif response.code not in [OK, CREATED, ACCEPTED]:
                # Nothing else we could do about that, this is
                # a problem on our end.
                logger.error(
                    "Could not upload logfile %s status code: %s. "
                    "This is a client side error, giving up.",
                    log_identifier, response.code)
                try:
                    upload_deferred.errback(ValueError(
                        "Bad return code on uploading logfile: %s"
                        % response.code))
                except Exception as e:
                    logger.error(
                        "Caught exception calling upload_deferred.errback: %s",
                        e)

            else:
                logger.info("Uploaded logfile %s for to master",
                            log_identifier)
                try:
                    upload_deferred.callback(None)
                except Exception as e:
                    logger.error(
                        "Caught exception calling upload_deferred.callback: %s",
                        e)

        def error_callback(url, log_identifier, failure_reason):
            if (failure_reason.type in
                (ResponseNeverReceived, RequestTransmissionFailed)):
                logger.debug(
                    "Error while uploading logfile %s to master: "
                    "%s, retrying immediately",
                    log_identifier, failure_reason.type.__name__)
                upload(url, log_identifier)
            else:
                delay = http_retry_delay()
                logger.error(
                    "Error while uploading logfile %s to master: "
                    "%r, retrying in %s seconds.",
                    log_identifier, failure_reason, delay)
                upload(url, log_identifier, delay=delay)

        logger.info("Uploading log file %s to master, URL %r",
                    self.log_identifier, url)
        upload(url, self.log_identifier)
        return upload_deferred


class System(object):
    # overridden in the job type
    _tempdirs = NotImplemented
    uuid = NotImplemented

    def _get_uid_gid_value(self, value, value_name, func_name,
                           module, module_name):
        """
        Internal function which handles both user name and group conversion.
        """
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
                    pwd.getpwuid(value)
                elif module_name == "grp":
                    grp.getgrgid(value)
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
            raise TypeError(
                "Expected an integer or string for `%s`" % value_name)

    def _remove_directories(self, directories, retry_on_exit=True):
        """
        Removes multiple multiple directories at once, retrying on exit
        for each failure.
        """
        assert isinstance(directories, (list, tuple, set))

        for directory in directories:
            remove_directory(
                directory, raise_=False, retry_on_exit=retry_on_exit)

    def _remove_tempdirs(self):
        """
        Iterates over all temporary directories in ``_tempdirs`` and removes
        them from disk.  This work will be done in a thread so it does not
        block the reactor.
        """
        assert isinstance(self._tempdirs, set)
        if not self._tempdirs:
            return

        reactor.callInThread(self._remove_directories, self._tempdirs.copy())
        self._tempdirs.clear()

    def _cleanup_system_temp_dir(self, minimum_age=604800):
        """
        Cleans up old file from the system's temp directory to reclaim space.
        Files that cannot be deleted, for example because of missing
        permissions, are silently ignored.

        .. warning::

            This will delete files from the system temp directory.

        """
        tempdir = tempfile.gettempdir()

        # followlinks=False is the default for os.walk.  I am specifying it
        # explicitly here to make it more obvious.  Setting this to True
        # instead might make us delete files outside of tempdir, if there is
        # a symlink in there somewhere.
        for root, dirs, files in os.walk(tempdir, topdown=True,
                                         followlinks=False):
            # Don't delete our own temp files
            if "pyfarm" in dirs:
                dirs.remove("pyfarm")
            for filename in files:
                fullpath = join(root, filename)
                stat_result = os.stat(fullpath)
                timestamp = max(stat_result.st_atime, stat_result.st_mtime)
                if timestamp + minimum_age < time.time():
                    logger.debug("Deleting tempfile %s", fullpath)
                    remove_file(fullpath, retry_on_exit=False, raise_=False)

    def _ensure_free_space_in_temp_dir(self, tempdir, space, minimum_age=None):
        """
        Ensures that at least space bytes of data can be stored on the volume
        on which tempdir is located, deleting files from tempdir if necessary.

        .. warning::

            This will delete files in tempdir to reclaim storage space.

        :raises InsufficientSpaceError:
            Raised if enough space cannot be claimed.
        """
        assert isinstance(tempdir, STRING_TYPES), "Expected string for tempdir"
        try:
            space = int(space)
        except (ValueError, TypeError):
            raise TypeError(
                "Could not convert value %r for `space` in "
                "_ensure_free_space_in_temp_dir() to an integer." % space)

        try:
            os.makedirs(tempdir)
        except OSError as e:  # pragma: no cover
            if e.errno != EEXIST:
                raise

        if disk_usage(tempdir).free >= space:
            return

        tempfiles = []
        # followlinks=False is the default for os.walk.  I am specifying it
        # explicitly here to make it more obvious.  Setting this to True
        # instead might make us delete files outside of tempdir, if there is
        # a symlink in there somewhere.
        for root, dirs, files in os.walk(tempdir, followlinks=False):
            for filename in files:
                fullpath = join(root, filename)
                atime = os.stat(fullpath).st_atime
                tempfiles.append({"filepath": fullpath, "atime": atime})

        tempfiles.sort(key=lambda x: x["atime"])

        while disk_usage(tempdir).free < space:
            if not tempfiles:
                raise InsufficientSpaceError("Cannot free enough space in temp "
                                             "directory %s" % tempdir)
            element = tempfiles.pop(0)
            if (not minimum_age or
                os.stat(element["filepath"]).st_mtime + minimum_age <
                    time.time()):
                logger.debug("Deleting tempfile %s", element["filepath"])
                remove_file(
                    element["filepath"], retry_on_exit=False, raise_=False)
            else:  # pragma: no cover
                logger.debug("Not deleting tempfile %s, it is newer than %s "
                             "seconds", element["filepath"], minimum_age)

    def _log(self, message):
        """
        Log a message from the jobtype itself to the process' log file.
        Useful for debugging jobtypes.
        """
        assert isinstance(self.uuid, UUID)
        logpool.log(self.uuid, "jobtype", message)


class TypeChecks(object):
    """
    Helper static methods for performing type checks on
    input arguments.
    """
    @staticmethod
    def _check_expandvars_inputs(value, environment):
        """Checks input arguments for :meth:`expandvars`"""
        if not isinstance(value, STRING_TYPES):
            raise TypeError("Expected a string for `value`")

        if environment is not None and not isinstance(environment, dict):
            raise TypeError("Expected None or a dictionary for `environment`")

    @staticmethod
    def _check_map_path_inputs(path):
        """Checks input arguments for :meth:`map_path`"""
        if not isinstance(path, STRING_TYPES):
            raise TypeError("Expected string for `path`")

    @staticmethod
    def _check_csvlog_path_inputs(protocol_uuid, now):
        """Checks input arguments for :meth:`get_csvlog_path`"""
        if not isinstance(protocol_uuid, UUID):
            raise TypeError("Expected UUID for `protocol_uuid`")

        if now is not None and not isinstance(now, datetime):
            raise TypeError("Expected None or datetime for `now`")

    @staticmethod
    def _check_command_list_inputs(cmdlist):
        """Checks input arguments for :meth:`get_command_list`"""
        if not isinstance(cmdlist, (tuple, list)):
            raise TypeError("Expected tuple or list for `cmdlist`")

    @staticmethod
    def _check_set_states_inputs(tasks, state):
        """Checks input arguments for :meth:`set_states`"""
        if not isinstance(tasks, ITERABLE_CONTAINERS):
            raise TypeError("Expected tuple, list or set for `tasks`")

        if state not in WorkState:
            raise ValueError("Expected `state` to be in %s" % list(WorkState))
