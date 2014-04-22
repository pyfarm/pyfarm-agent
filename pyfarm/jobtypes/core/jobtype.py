# No shebang line, this module is meant to be imported
#
# Copyright 2013 Oliver Palmer
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

import imp
import json
import os
import tempfile
import threading
import sys
from datetime import datetime
from string import Template
from os.path import join, dirname, isfile, isdir
from Queue import Queue, Empty

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

try:
    import pwd
    import grp
except ImportError:  # pragma: no cover
    pwd = NotImplemented
    grp = NotImplemented

from twisted.internet import threads, reactor
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone

from pyfarm.core.config import read_env, read_env_float, read_env_int
from pyfarm.core.enums import INTERGER_TYPES, STRING_TYPES, WorkState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo.user import is_administrator
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.agent.utility import UnicodeCSVWriter
from pyfarm.jobtypes.core.process import (
    ProcessProtocol, ProcessInputs, ReplaceEnvironment)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
process_stdout = getLogger("process.stdout")
process_stderr = getLogger("process.stderr")

# Construct the base environment that all job types will use.  We do this
# once per process so a job type can't modify the running environment
# on purpose or by accident.
DEFAULT_ENVIRONMENT_CONFIG = read_env("PYFARM_JOBTYPE_DEFAULT_ENVIRONMENT", "")
if isfile(DEFAULT_ENVIRONMENT_CONFIG):
    logger.info(
        "Attempting to load default environment from %r",
        DEFAULT_ENVIRONMENT_CONFIG)
    with open(DEFAULT_ENVIRONMENT_CONFIG, "rb") as stream:
        DEFAULT_ENVIRONMENT = json.load(stream)
else:
    DEFAULT_ENVIRONMENT = os.environ.copy()

assert isinstance(DEFAULT_ENVIRONMENT, dict)

DEFAULT_CACHE_DIRECTORY = read_env(
    "PYFARM_JOBTYPE_CACHE_DIRECTORY", ".jobtypes")
STDOUT = 0
STDERR = 1


class LoggingThread(threading.Thread):
    """
    This class runs a thread which writes lines in csv format
    to the log file.
    """
    FLUSH_AFTER_LINES = read_env_int(
        "PYFARM_JOBTYPE_LOGGING_FLUSH_AFTER_LINES", 50)
    QUEUE_GET_TIMEOUT = read_env_float(
        "PYFARM_JOBTYPE_LOGGING_QUEUE_TIMEOUT", .25)

    def __init__(self, filepath):
        super(LoggingThread, self).__init__()
        self.queue = Queue()
        self.stream = open(filepath, "wb")
        self.writer = UnicodeCSVWriter(self.stream)
        self.lineno = 1
        self.next_flush = self.FLUSH_AFTER_LINES
        self.stopped = False
        self.shutdown_event = \
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)

    def put(self, streamno, message):
        """Put a message in the queue for the thread to pickup"""
        assert self.stopped is False, "Cannot put(), thread is stopped"
        now = datetime.utcnow()
        self.queue.put_nowait(
            (now.isoformat(), streamno, self.lineno, message))
        self.lineno += 1

    def stop(self):
        self.stopped = True
        reactor.removeSystemEventTrigger(self.shutdown_event)

    def run(self):
        stopping = False
        while True:
            # Pull data from the queue or retry again
            try:
                timestamp, streamno, lineno, message = self.queue.get(
                    timeout=self.QUEUE_GET_TIMEOUT)
            except Empty:
                pass
            else:
                # Write data from the queue to a file
                self.writer.writerow(
                    [timestamp, str(streamno), str(lineno), message])
                if self.lineno >= self.next_flush:
                    self.stream.flush()
                    self.next_flush += self.FLUSH_AFTER_LINES

            # We're either being told to stop or we
            # need to run one more iteration of the
            # loop to pickup any straggling messages.
            if self.stopped and stopping:
                logger.debug("Closing %s", self.stream.name)
                self.stream.close()
                break

            # Go around one more time to pickup remaining messages
            elif self.stopped:
                stopping = True


class JobType(object):
    """
    Base class for all other job types.  This class is intended
    to abstract away many of the asynchronous necessary to run
    a job type on an agent.

    :attribute bool ignore_uid_gid_mapping_errors:
        If True, then ignore any errors produced by :meth:`usrgrp_to_uidgid`
        and return ``(None, None`)` instead.  If this value is False
        the original exception will be raised instead and report as
        an error which can prevent a task from running.

    :attribute cache:
        Stores the cached job types

    :attribute process_protocol:
        The protocol object used to communicate between the process
        and the job type.

    :attribute list success_codes:
        A list of exit codes which indicate the process has terminated
        successfully.  This list does not have to be used but is the
        defacto attribute we just inside of :meth:`` to determine success.
    """
    success_codes = set([0])
    ignore_uid_gid_mapping_errors = False
    process_protocol = ProcessProtocol
    logging = {}
    cache = {}

    def __init__(self, assignment):
        assert isinstance(assignment, dict)
        self.protocols = {}
        self.assignment = ImmutableDict(assignment)

        # NOTE: Don't call this logging statement before the above, we need
        # self.assignment
        logger.debug("Instanced %r", self)

    def __repr__(self):
        return "JobType(job=%r, tasks=%r, jobtype=%r, version=%r, title=%r)" % (
            self.assignment["job"]["id"],
            tuple(task["id"] for task in self.assignment["tasks"]),
            str(self.assignment["jobtype"]["name"]),
            self.assignment["jobtype"]["version"],
            str(self.assignment["job"]["title"]))

    def _get_uidgid(self, value, value_name, func_name, module, module_name):
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

                if not self.ignore_uid_gid_mapping_errors:
                    raise

        # Verify that the provided user/group string is real
        elif isinstance(value, INTERGER_TYPES):
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

                if not self.ignore_uid_gid_mapping_errors:
                    raise
        else:
            raise ValueError(
                "Expected an integer or string for `%s`" % value_name)

    @classmethod
    def _cache_key(cls, assignment):
        """Returns the key used to store and retrieve the cached job type"""
        return assignment["jobtype"]["name"], assignment["jobtype"]["version"]

    @classmethod
    def _url(cls, name, version):
        """Returns the remote url for the requested job type and version"""
        return str(
            "%(master-api)s/jobtypes/%(jobtype)s/versions/%(version)s" % {
            "master-api": config["master-api"],
            "jobtype": name,
            "version": version})

    @classmethod
    def _cache_directory(cls):
        """Returns the path where job types should be cached"""
        return config.get("jobtype_cache_directory") or DEFAULT_CACHE_DIRECTORY

    @classmethod
    def _cache_filename(cls, cache_key, jobtype):
        """
        Returns the path where this specific job type should be
        stored or loaded from
        """
        return str(join(
            cls._cache_directory(),
            "_".join(map(str, cache_key)) + "_" + jobtype["classname"] + ".py"))

    @classmethod
    def _download_jobtype(cls, assignment):
        """
        Downloads the job type specified in ``assignment``.  This
        method will pass the response it receives to :meth:`_cache_jobtype`
        however failures will be retried.
        """
        result = Deferred()
        url = cls._url(
            assignment["jobtype"]["name"],
            assignment["jobtype"]["version"])

        def download(*_):
            get(url, callback=result.callback, errback=download)

        download()
        return result

    @classmethod
    def _cache_jobtype(cls, cache_key, jobtype):
        """
        Once the job type is downloaded this classmethod is called
        to store it on disk.  In the rare even that we fail to write it
        to disk, we store it in memory instead.
        """
        filename = cls._cache_filename(cache_key, jobtype)
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
                return filename, jobtype

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

    @classmethod
    def load(cls, assignment):
        """
        Given ``data`` this class method will load the job type either
        from cache or from the master and then instance it with the
        incoming assignment data
        """
        result = Deferred()
        cache_key = cls._cache_key(assignment)

        def load_jobtype(args):
            jobtype, filepath = args
            module_name = "pyfarm.jobtypes.cached.%s%s%s" % (
                jobtype["classname"], jobtype["name"], jobtype["version"])

            if filepath is not None:
                # If we've made it to this point we have to assume
                # we have access to the file.
                module = imp.load_source(module_name, filepath)
            else:
                # TODO: this needs testing
                logcache.warning(
                    "Loading (%s, %s) directly from memory",
                    jobtype["name"], jobtype["version"])
                module = imp.new_module(module_name)

                # WARNING: This is dangerous and only used as a last
                # resort.  There's probably something wrong with the
                # file system.
                exec jobtype["code"] in module.__dict__

                sys.modules[module_name] = module

            # Finally, send the results to the callback
            result.callback(getattr(module, jobtype["classname"]))

        if config["jobtype-no-cache"] or cache_key not in cls.cache:
            def download_complete(response):
                if response.code != OK:
                    return response.request.retry()

                if config["jobtype-no-cache"]:
                    return load_jobtype((response.json(), None))

                # When the download is complete, cache the results
                caching = cls._cache_jobtype(cache_key, response.json())
                caching.addCallback(load_jobtype)

            # Start the download
            download = cls._download_jobtype(assignment)
            download.addCallback(download_complete)
        else:
            load_jobtype((cls.cache[cache_key]))

        return result

    def assignments(self):
        """Short cut method to access tasks"""
        return self.assignment["tasks"]

    def build_process_protocol(
            self, jobtype, process_inputs, command, arguments,
            environment, chdir, uid, gid):
        """
        Returns the process protocol object used to spawn connect
        a job type to a running process.  By default this instance
        :cvar:`.process_protocol` class variable.

        :raises TypeError:
            raised if the protocol object is not a subclass of
            :class:`.ProcessProtocol`
        """
        instance = self.process_protocol(
            jobtype, process_inputs, command, arguments, environment,
            chdir, uid, gid)

        if not isinstance(instance, ProcessProtocol):
            raise TypeError(
                "Expected of pyfarm.jobtypes.core.protocol.ProcessProtocol")

        return instance

    def get_default_environment(self):
        """
        Constructs a default environment dictionary that will replace
        ``os.environ`` before the process is launched in
        :meth:`spawn_process`.  This ensures that ``os.environ`` is consistent
        before and after each process and so that each process can't modify
        the original environment.
        """
        return dict(
            list(DEFAULT_ENVIRONMENT.items()) +
            list(self.assignment["job"].get("environ", {}).items()))

    def get_uid(self, username):
        """
        Convert ``username`` into an integer representing the user id.  If
        ``username`` is an integer verify that it exists instead.

        .. warning::

            This method returns ``None`` on platforms that don't implement
            the :mod:`pwd` module.
        """
        if username is None:
            return username
        return self._get_uidgid(username, "username", "get_uid", pwd, "pwd")

    def get_gid(self, group):
        """
        Convert ``group`` into an integer representing the group id.  If
        ``group`` is an integer verify that it exists instead.

        .. warning::

            This method returns ``None`` on platforms that don't implement
            the :mod:`grp` module.
        """
        if group is None:
            return group
        return self._get_uidgid(group, "group", "get_gid", grp, "grp")

    # TODO: This needs more command line arguments and configuration options
    def get_csvlog_path(self, task, now=None):
        """
        Returns the path to the comma separated value (csv) log file.
        The agent stores logs from processes in a csv format so we can store
        additional information such as a timestamp, line number, stdout/stderr
        identification and the the log message itself.
        """
        assert isinstance(task, dict)
        if now is None:
            now = datetime.utcnow()

        return join(
            config["task-log-dir"],
            "%s_%s_%s.csv" % (
                now.strftime("%G%m%d%H%M%S"),
                self.assignment["job"]["id"], task["id"]))

    def build_process_inputs(self):
        """
        This method constructs and returns all the arguments necessary
        to execute one or multiple processes on the system.  An example
        implementation may look similar to this:

        >>> from pyfarm.jobtypes.core.jobtype import ProcessInputs
        >>> def build_process_inputs(self):
        ...     for task in self.assignments():
        ...         ProcessInputs(
        ...             task,
        ...             ("/bin/ls", "/tmp/foo%s" % task["frame"]),
        ...             environ={"FOO": "1"},
        ...             user="foo",
        ...             group="bar")

        The above is a generator that will return everything that's needed to
        run the process. See :class:`ProcessInputs` for some more detailed
        documentation on each attribute's function.
        """
        raise NotImplementedError("`build_process_inputs` must be implemented")

    def expandvars(self, value, environment):
        """Expands variables inside of a string using an environment"""
        if isinstance(value, STRING_TYPES):
            template = Template(value)
            return template.safe_substitute(**environment)

        logger.warning("Expected a string for `value` to expandvars()")
        return value

    def spawn_process(self, process_inputs):
        """
        Spawns a process using :func:`.reactor.spawnProcess` and returns
        a deferred object which will fire when t
        """
        assert isinstance(process_inputs, ProcessInputs)

        # assert `task` is a valid type
        if not isinstance(process_inputs.task, dict):
            self.set_state(
                process_inputs.task, WorkState.FAILED,
                "`task` must be a dictionary, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    process_inputs.task))
            return

        # assert `command` is a valid type
        if not isinstance(process_inputs.command, (list, tuple)):
            self.set_state(
                process_inputs.task, WorkState.FAILED,
                "`command` must be a list or tuple, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    process_inputs.command))
            return

        # username/group to uid/gid mapping
        uid, gid = None, None
        if all([pwd is not NotImplemented,
                grp is not NotImplemented,
                process_inputs.user is not None or
                                process_inputs.group is not None]):

            # Regardless of platform, we run a command as
            # another user unless we're an admin
            if is_administrator():
                # map user
                try:
                    uid = self.get_uid(process_inputs.user)
                except (ValueError, KeyError):
                    self.set_state(
                        process_inputs.task, WorkState.FAILED,
                        "Failed to or verify user %r" % process_inputs.user)
                    return

                # map group
                try:
                    gid = self.get_uid(process_inputs.group)
                except (ValueError, KeyError):
                    self.set_state(
                        process_inputs.task, WorkState.FAILED,
                        "Failed to or verify group %r" % process_inputs.group)
                    return

            else:
                logger.warning(
                    "User and/or group was provided but the agent is not "
                    "running as an administrator which is required to run"
                    "processes as another user.")

        # generate environment and ensure
        if process_inputs.env is None:
            environment = self.get_default_environment()
        else:
            environment = process_inputs.env

        if not isinstance(environment, dict):
            self.set_state(
                process_inputs.task, WorkState.FAILED,
                "`process_inputs.env` must be a dictionary, got %s instead.  "
                "Check the output produced by "
                "`build_process_inputs`" % type(environment))
            return

        # Prepare the arguments for the spawnProcess call
        kwargs = {"env": None}

        # update kwargs with uid/gid
        if uid is not None:
            kwargs.update(uid=uid)
        if gid is not None:
            kwargs.update(gid=gid)

        # process_inputs.chdir may be a file path
        chdir = config["chroot"]
        if isinstance(process_inputs.chdir, STRING_TYPES) \
                and process_inputs.expandvars:
            # Convert process_inputs.chdir to a template first so we
            # can resolve any environment variables it may contain
            chdir = self.expandvars(process_inputs.chdir, environment)

        if chdir is not None and not isdir(chdir):
            self.set_state(
                process_inputs.task, WorkState.FAILED,
                "Directory provided for `process_inputs.chdir` does not "
                "exist: %r" % chdir)
            return

        if chdir is not None:
            kwargs.update(path=chdir)

        # Expand any environment variables in the command
        command = process_inputs.command
        if process_inputs.expandvars:
            command = map(
                lambda value: self.expandvars(value, environment),
                process_inputs.command)

        arguments = command[1:]
        kwargs.update(args=arguments)

        protocol = self.build_process_protocol(
            self, process_inputs, command[0], arguments, environment, chdir,
            uid, gid)

        # Internal data setup
        logfile = self.get_csvlog_path(process_inputs.task)
        self.protocols[protocol.id] = protocol

        # Start the logging thread
        thread = self.logging[protocol.id] = LoggingThread(logfile)
        thread.start()

        # reactor.spawnProcess does different things with the environment
        # depending on what platform you're on and what you're passing in.
        # To avoid inconsistent behavior, we replace os.environ with
        # our own environment so we can launch the process.  After launching
        # we replace the original environment.
        with ReplaceEnvironment(environment):
            reactor.spawnProcess(protocol, command[0], **kwargs)

        return protocol

    def start(self):
        """
        This method is called when the job type should start
        working.  Depending on the job type's implementation this will
        prepare and start one more more processes.
        """
        for process_inputs in self.build_process_inputs():
            self.spawn_process(process_inputs)

        # TODO: verify this does the right thing since `spawned` might not
        # work with this
        # return DeferredList(tasks)

    def format_exception(self, exception):
        """
        Takes an :class:`.Exception` instance and formats it so it could
        be used as an error message.  By default this simply runs the
        exception through :func:`str` but this could be overridden to
        do something more complex.
        """
        return str(exception)

    def set_state(self, task, state, error=None):
        """
        Sets the state of the given task

        :param dict task:
            The dictionary containing the task we're changing the state
            for.

        :param string state:
            The state to change ``task`` to

        :type error: string, :class:`Exception`
        :param error:
            If the state is changing to 'error' then also set the
            ``last_error`` column.  Any exception instance that is
            passed to this keyword will be passed through
            :meth:`format_exception` first to format it.
        """
        if state not in WorkState:
            logger.error(
                "Cannot set state for task %r to %r, %r is an invalid "
                "state", task, state, state)
        elif not isinstance(task, dict):
            logger.error(
                "Expected a dictionary for `task`, cannot change state")

        elif not "id" in task or not isinstance(task["id"], INTERGER_TYPES):
            logger.error(
                "Expected to find 'id' in `task` or for `task['id']` to "
                "be an integer.")

        elif task not in self.assignment["tasks"]:
            logger.error(
                "Cannot set state, expected task %r to be a member of this "
                "job type's assignments", task)

        else:
            if state != WorkState.ERROR and error:
                logger.warning(
                    "Resetting `error` to None, state is not being "
                    "WorkState.ERROR")
                error = None

            job_id = self.assignment["job"]["id"]
            task_id = task["id"]

            # TODO: POST change
            if state == WorkState.ERROR:
                if isinstance(error, Exception):
                    error = self.format_exception(error)

                logger.error("Task %r failed: %r", task, error)

            # TODO: if new state is the equiv. of 'stopped', stop the process
            # and POST the change

    # TODO: check other exit types
    def is_successful(self, reason):
        return (
            reason.type is ProcessDone and
            reason.value.exitCode in self.success_codes)

    # TODO: documentation
    # TODO: POST status
    def process_stopped(self, protocol, reason):
        logger.info("%r stopped (code: %r)", protocol, reason.value.exitCode)

        self.protocols.pop(protocol.id, None)
        thread = self.logging.pop(protocol.id)

        if self.is_successful(reason):
            thread.put(
                STDOUT, "Process has terminated successfully, code %s" %
                reason.value.exitCode)
        else:
            thread.put(
                STDOUT, "Process has not terminated successfully, code %s" %
                reason.value.exitCode)

        thread.stop()

    # TODO: documentation
    def process_started(self, protocol):
        logger.info("%r started", protocol)
        # TODO: POST status

    # TODO: documentation
    def received_stdout(self, protocol, data):
        if config["capture-process-output"]:
            process_stdout.info("task %r: %s", protocol.id, data)
        else:
            self.logging[protocol.id].put(STDOUT, data)

    # TODO: documentation
    def received_stderr(self, protocol, data):
        if config["capture-process-output"]:
            process_stderr.info("task %r: %s", protocol.id, data)
        else:
            self.logging[protocol.id].put(STDERR, data)
