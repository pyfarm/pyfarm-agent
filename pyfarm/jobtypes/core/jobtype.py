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
from os.path import join, dirname, isfile, basename
from Queue import Queue, Empty
from functools import partial

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

from twisted.internet import threads, reactor
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone, ProcessTerminated
from twisted.python.failure import Failure

from pyfarm.core.config import (
    read_env, read_env_float, read_env_int, read_env_bool)
from pyfarm.core.enums import WINDOWS, INTEGER_TYPES, STRING_TYPES, WorkState
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get, post, http_retry_delay
from pyfarm.agent.sysinfo.user import is_administrator
from pyfarm.agent.utility import UnicodeCSVWriter, uuid
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
STREAMS = set([STDOUT, STDERR])


# TODO: if we get fail the task if we have errors
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
        assert streamno in STREAMS
        assert isinstance(message, STRING_TYPES)
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
    # TODO: add command line flags for some of these
    expand_path_vars = read_env_bool(
        "PYFARM_JOBTYPE_DEFAULT_EXPANDVARS", True)
    ignore_uid_gid_mapping_errors = read_env_bool(
        "PYFARM_JOBTYPE_DEFAULT_IGNORE_UIDGID_MAPPING_ERRORS", False)
    process_protocol = ProcessProtocol
    success_codes = set([0])
    logging = {}
    cache = {}

    def __init__(self, assignment):
        assert isinstance(assignment, dict)

        # JobType objects in the future may or may not have explicit tasks
        # associated with when them.  The format of tasks could also change
        # since it's an internal representation so to guard against these
        # changes we just use a simple uuid to represent ourselves in the
        # config dictionary.
        self._uuid = uuid()
        config["jobtypes"][self._uuid] = self

        self.protocols = {}
        self.assignment = ImmutableDict(assignment)
        self.failed_processes = []
        self.stdout_line_fragments = []

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

    def _log_in_thread(self, protocol, stream_type, data):
        """
        Internal implementation called several methods including
        :meth:`_received_stdout`, :meth:`_received_stderr`,
        :meth:`_process_started` and others.

        This method takes the incoming protocol object and retrieves the thread
        which is handling logging for a given process.  Each message will then
        be queued and written to disk at the next opportunity.
        """
        self.logging[protocol.id].put(stream_type, data)

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
            get(
                url,
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
                # Server is offline or experiencing issues right
                # now so we should retry the request.
                if response.code >= INTERNAL_SERVER_ERROR:
                    return reactor.callLater(
                        http_retry_delay(),
                        response.request.retry)

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
            self, jobtype, process_inputs, command, arguments, environment,
            path, uid, gid):
        """
        Returns the process protocol object used to connect a job type
        to a running process.  By default this instances
        the :cvar:`.process_protocol` class variable.

        :raises TypeError:
            Raised if the protocol object is not a subclass of
            :class:`.ProcessProtocol`
        """
        instance = self.process_protocol(
            jobtype, process_inputs, command, arguments, environment,
            path, uid, gid)

        if not isinstance(instance, ProcessProtocol):
            raise TypeError(
                "Expected of pyfarm.jobtypes.core.protocol.ProcessProtocol")

        return instance

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

    def get_environment(self, env=None):
        """
        Constructs a default environment dictionary that will replace
        ``os.environ`` before the process is launched in
        :meth:`spawn_process`.  This ensures that ``os.environ`` is consistent
        before and after each process and so that each process can't modify
        the original environment.
        """
        if isinstance(env, dict):
            return env

        elif env is not None:
            logger.warning(
                "Expected a dictionary for `env`, falling back onto default "
                "environment")

        return dict(
            list(DEFAULT_ENVIRONMENT.items()) +
            list(self.assignment["job"].get("environ", {}).items()))

    def get_path(self, path, environment=None, expandvars=None, task=None):
        """
        Returns the directory a process should change into before
        running.

        :param string path:
            The directory we need to resolve or validate

        :param dict environment:
            The environment to use when expanding environment
            variables in ``path``

        :param bool expandvars:
            If True, use the environment to expand any environment
            variables in ``path``

        :param dict task:
            If provided this task will be set to ``FAILED`` if the
            directory does not exist

        :returns:
            Returns ``None`` if we failed to resolve ``path`` or the
            directory to change into
        """
        if expandvars is None:
            expandvars = self.expand_path_vars

        if isinstance(path, STRING_TYPES) and expandvars:
            if environment is None:
                environment = self.get_environment()

            # Convert path to a template first so we  can resolve
            # any environment variables it may contain
            return self.expandvars(path, environment)

        return config["chroot"]

    def get_command_list(self, cmdlist, environment=None, expandvars=None):
        """
        Return a list of command to be used when running the process
        as a read-only tuple.

        :param dict environment:
            If provided this will be used to perform environment variable
            expansion for each entry in ``cmdlist``

        :param bool expandvars:
            If True then use ``environment`` to expand any environment
            variables in ``cmdlist``
        """
        if expandvars is None:
            expandvars = self.expand_path_vars

        if expandvars and environment is None:
            environment = self.get_environment()

        if expandvars and environment:
            cmdlist = map(
                lambda value: self.expandvars(value, environment), cmdlist)

        return tuple(cmdlist)  # read-only copy (also takes less memory)

    # TODO: This needs more command line arguments and configuration options
    def get_csvlog_path(self, tasks, now=None):
        """
        Returns the path to the comma separated value (csv) log file.
        The agent stores logs from processes in a csv format so we can store
        additional information such as a timestamp, line number, stdout/stderr
        identification and the the log message itself.
        """
        assert isinstance(tasks, (list, tuple))

        if now is None:
            now = datetime.utcnow()

        return join(
            config["task-log-dir"],
            "%s_%s_%s.csv" % (
                now.strftime("%G%m%d%H%M%S"),
                self.assignment["job"]["id"],
                "-".join(map(str, list(task["id"]for task in tasks)))))

    def build_process_inputs(self):
        """
        This method constructs and returns all the arguments necessary
        to execute one or multiple processes on the system.  An example
        implementation may look similar to this:

        >>> from pyfarm.jobtypes.core.jobtype import ProcessInputs
        >>> def build_process_inputs(self):
        ...     for task in self.assignments():
        ...         yield ProcessInputs(
        ...             [task],
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
        Spawns a process using :func:`.reactor.spawnProcess` and return
        the protocol object it generates.
        """
        assert isinstance(process_inputs, ProcessInputs)

        # assert `task` is a valid type
        if not isinstance(process_inputs.tasks, (list, tuple)):
            self.set_states(
                process_inputs.tasks, WorkState.FAILED,
                "`task` must be a dictionary, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    process_inputs.tasks))
            return

        # assert `command` is a valid type
        if not isinstance(process_inputs.command, (list, tuple)):
            self.set_states(
                process_inputs.tasks, WorkState.FAILED,
                "`command` must be a list or tuple, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    process_inputs.command))
            return

        # username/group to uid/gid mapping
        uid, gid = None, None
        if all([not WINDOWS,  # Twisted does not implement this on Windows
                pwd is not NotImplemented,
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
                    self.set_states(
                        process_inputs.tasks, WorkState.FAILED,
                        "Failed to or verify user %r" % process_inputs.user)
                    return

                # map group
                try:
                    gid = self.get_uid(process_inputs.group)
                except (ValueError, KeyError):
                    self.set_state(
                        process_inputs.tasks, WorkState.FAILED,
                        "Failed to or verify group %r" % process_inputs.group)
                    return

            else:
                logger.warning(
                    "User and/or group was provided but the agent is not "
                    "running as an administrator which is required to run"
                    "processes as another user.")

        # Prepare the arguments for the spawnProcess call
        environment = self.get_environment(process_inputs.env)
        commands = self.get_command_list(
            process_inputs.command,
            environment=environment, expandvars=process_inputs.expandvars)

        # args - name of command being run + input arguments
        kwargs = {
            "args": [basename(commands[0])] + list(commands[1:]),
            "env": environment,
            "path": self.get_path(
                process_inputs.path,
                environment=environment, expandvars=process_inputs.expandvars)}

        # Add uid/gid into kwargs
        if uid is not None:
            kwargs.update(uid=uid)
        if gid is not None:
            kwargs.update(gid=gid)

        # Instance the process protocol so the process we create will
        # call into this class
        protocol = self.build_process_protocol(
            self, process_inputs, commands[0], kwargs["args"], kwargs["env"],
            kwargs["path"], uid, gid)

        # Internal data setup
        logfile = self.get_csvlog_path(process_inputs.tasks)
        self.protocols[protocol.id] = protocol

        # Start the logging thread
        # TODO: need a method to generate or retrieve a LoggingThread, we could
        # run up to this point with the same log file
        thread = self.logging[protocol.id] = LoggingThread(logfile)
        thread.start()

        # TODO: do validation of **kwargs here so we don't do it in one location
        # * environment (dict, strings only)
        # * path exists (see previous setup for this in the history)
        #

        # reactor.spawnProcess does different things with the environment
        # depending on what platform you're on and what you're passing in.
        # To avoid inconsistent behavior, we replace os.environ with
        # our own environment so we can launch the process.  After launching
        # we replace the original environment.
        with ReplaceEnvironment(environment):
            reactor.spawnProcess(protocol, commands[0], **kwargs)

        return protocol

    def _start(self):
        return self.start()

    def start(self):
        """
        This method is called when the job type should start
        working.  Depending on the job type's implementation this will
        prepare and start one more more processes.
        """
        # Make sure start() is not called twice
        assert not hasattr(self, "deferred")

        # TODO: add deferred handlers
        # TODO: collect all tasks and depending on the relationship
        # between tasks and processes we have to change how we notify
        # the master (and how we cancel other tasks which are queued)
        for process_inputs in self.build_process_inputs():
            self.spawn_process(process_inputs)

        self.deferred = Deferred()
        return self.deferred

    def format_log_message(self, message, stream_type=None):
        """
        This method may be overridden to format a log message coming from
        a process before we write out out to stdout or to disk.  By default
        this method does nothing except return the original message.

        :param int stream_type:
            When not ``None`` this specifies if the message is from stderr
            or stdout.  This value comes from :const:`.STDOUT` or
            :const:`.STDERR`
        """
        return message

    def format_error(self, error):
        """
        Takes some kind of object, typically an instance of
        :class:`.Exception` or :class`.Failure` and produces a human readable
        string.  If we don't know how to format the request object an error
        will be logged and nothing will be returned
        """
        # It's likely that the process has terminated abnormally without
        # generating a trace back.  Modify the reason a little to better
        # reflect this.
        if isinstance(error, Failure):
            if error.type is ProcessTerminated and error.value.exitCode > 0:
                return "Process may have terminated abnormally, please check " \
                       "the logs.  Message from error " \
                       "was %r" % error.value.message

            # TODO: there are other types of errors from Twisted we should
            # format here

        elif isinstance(error, Exception):
            return str(error)

        elif isinstance(error, STRING_TYPES):
            return error

        elif error is None:
            logger.error("No error was defined for this failure.")

        else:
            logger.error("Don't know how to format %r as a string", error)

    # TODO: modify this function to support batch updates
    def set_states(self, tasks, state, error=None):
        """
        Wrapper around :meth:`set_state` that that allows you to
        the state on the master for multiple tasks at once.
        """
        assert isinstance(tasks, (tuple, list))
        for task in tasks:
            self.set_state(task, state, error=error)

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

        elif not "id" in task or not isinstance(task["id"], INTEGER_TYPES):
            logger.error(
                "Expected to find 'id' in `task` or for `task['id']` to "
                "be an integer.")

        elif task not in self.assignment["tasks"]:
            logger.error(
                "Cannot set state, expected task %r to be a member of this "
                "job type's assignments", task)

        else:
            if state != WorkState.FAILED and error:
                logger.warning(
                    "Resetting `error` to None, state is not WorkState.FAILED")
                error = None

            # The task has failed
            if state == WorkState.FAILED:
                error = self.format_error(error)
                logger.error("Task %r failed: %r", task, error)

            # `error` shouldn't be set if the state is not a failure
            elif error is not None:
                logger.warning(
                    "`set_state` only allows `error` to be set if `state` is "
                    "'failed'.  Discarding error.")
                error = None

            url = "%s/jobs/%s/tasks/%s" % (
                config["master-api"], self.assignment["job"]["id"], task["id"])
            data = {"state": state}

            # If the error has been set then update the data we're
            # going to POST
            if isinstance(error, STRING_TYPES):
                data.update(last_error=error)

            elif error is not None:
                logger.error("Expected a string for `error`")

            def post_update(url, data, delay=0):
                post_func = partial(
                    post,
                    url,
                    data=data,
                    callback=lambda x: result_callback(url, data, task["id"],
                                                       state, x),
                    errback=lambda x: error_callback(url, data, x))
                reactor.callLater(delay, post_func)

            def result_callback(url, data, task_id, state, response):
                if response.code >= 500 and response.code < 600:
                    logger.error("Error while posting state update for task %s "
                                 "to %s, return code is %s, retrying",
                                 task_id, state, response.code)
                    post_update(url, data, delay=http_retry_delay())

                elif response.code != OK:
                    # Nothing else we could do about that
                    logger.error(
                        "Could not set state for task %s to %s, server "
                        "response code was %s", task_id, state, response.code)

                else:
                    logger.info("Set state of task %s to %s on master",
                                task_id, state)

            def error_callback(url, data, failure):
                logger.error("Error while posting state update for task, "
                             "retrying")
                post_update(url, data, delay=http_retry_delay())

            # Initial attempt to make an update with an explicit zero
            # delay.
            post_update(url, data, delay=0)

        # TODO: if new state is the equiv. of 'stopped', stop the process
        # and POST the change

    def is_successful(self, reason):
        """
        Returns True if we should consider ``reason`` to be
        successful.  This function is called by :meth:`_process_stopped`
        to determine if the object returned by a process is an indication
        of failure.
        """
        return (
            reason.type is ProcessDone and
            reason.value.exitCode in self.success_codes)

    def _process_stopped(self, protocol, reason):
        """
        Internal implementation for :meth:`process_stopped`.

        If ``--capture-process-output`` was set when the agent was launched
        all standard output from the process will be sent to the stdout
        of the agent itself.  In all other cases we send the data to
        :meth:`_log_in_thread` so it can be stored in a file without
        blocking the event loop.
        """
        logger.info("%r stopped (code: %r)", protocol, reason.value.exitCode)

        if self.is_successful(reason):
            self._log_in_thread(
                protocol, STDOUT,
                "Process has terminated successfully, code %s" %
                reason.value.exitCode)
        else:
            self.failed_processes.append((protocol, reason))
            self._log_in_thread(
                protocol, STDOUT,
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
                    self.set_state(task, WorkState.DONE, reason)
            else:
                self.deferred.errback()
                for task in self.assignment["tasks"]:
                    self.set_state(task, WorkState.FAILED, reason)

    def process_stopped(self, protocol, reason):
        """
        Called by :meth:`.ProcessProtocol.processEnded` when a process
        stopped running.
        """
        self._process_stopped(protocol, reason)

    # TODO: set state
    def _process_started(self, protocol):
        """
        Internal implementation for :meth:`process_started`.

        This method logs the start of a process and informs the master of
        the state change.
        """
        logger.info("%r started", protocol)
        self._log_in_thread(protocol, STDOUT, "Started %r" % protocol)

    def process_started(self, protocol):
        """
        Called by :meth:`.ProcessProtocol.connectionMade` when a process has
        started running.
        """
        self._process_started(protocol)

    def received_stdout(self, protocol, stdout):
        """
        Called by :meth:`.ProcessProtocol.outReceived` when
        we receive output on standard output (stdout) from a process.
        Not to be overridden.
        """
        new_stdout = self.preprocess_stdout(protocol, stdout)
        if new_stdout is not None:
            stdout = new_stdout

        self.process_stdout(protocol, stdout)

    def preprocess_stdout(self, protocol, stdout):
        pass

    def process_stdout(self, protocol, stdout):
        """
        Overridable function called when we receive data from a child process'
        stdout.
        The default implementation will split the output into lines and forward
        those to received_stdout_line(), which will eventually forward it to
        process_stdout_line()
        """
        if "\n" in stdout:
            ends_on_fragment = True
            if stdout[-1] == "\n":
                ends_on_fragment = False
            lines = stdout.split("\n")
            if ends_on_fragment:
                dangling_fragment = lines.pop(-1)
            for line in lines:
                if protocol.id in self.stdout_line_fragments:
                    l = self.stdout_line_fragments[protocol.id] + line
                    del self.stdout_line_fragments[protocol.id]
                    if len(l) > 0 and l[-1] == "\r":
                        l = l[:-1]
                    self.received_stdout_line(protocol, l)
                else:
                    if len(line) > 0 and line[-1] == "\r":
                        line = line[:-1]
                    self.received_stdout_line(protocol, line)
            if ends_on_fragment:
                self.stdout_line_fragments[protocol.id] = dangling_fragment
        else:
            if protocol.id in self.stdout_line_fragments:
                self.stdout_line_fragments[protocol.id] += stdout
            else:
                self.stdout_line_fragments[protocol.id] = stdout

    def received_stdout_line(self, protocol, line):
        """
        Called when we receive a new line from stdout, and possibly stderr if
        those methods have not been overridden as well.

        If ``--capture-process-output`` was set when the agent was launched
        all standard output from the process will be sent to the stdout
        of the agent itself.  In all other cases we send the data to
        :meth:`_log_in_thread` so it can be stored in a file without
        blocking the event loop.
        """
        new_line = self.preprocess_stdout_line(protocol, line)
        if new_line is not None:
            line = new_line

        line = self.format_log_message(line, stream_type=STDOUT)
        if config["capture-process-output"]:
            process_stdout.info("task %r: %s", protocol.id, line)
        else:
            self._log_in_thread(protocol, STDOUT, line)

        self.process_stdout_line(protocol, line)

    def preprocess_stdout_line(self, protocol, line):
        pass

    def process_stdout_line(self, protocol, line):
        """
        Overridable function called whenever we receive a new line from a child
        process' stdout.
        Default implementation does nothing.
        """
        pass

    def _received_stderr(self, protocol, stderr):
        """
        Internal implementation for :meth:`received_stderr`.

        If ``--capture-process-output`` was set when the agent was launched
        all stderr output will be sent to the stdout of the agent itself.
        In all other cases we send the data to to :meth:`_log_in_thread`
        so it can be stored in a file without blocking the event loop.
        """
        stderr = self.format_log_message(stderr, stream_type=STDERR)
        if config["capture-process-output"]:
            process_stderr.info("task %r: %s", protocol.id, stderr)
        else:
            self._log_in_thread(protocol, STDERR, stderr)

    def received_stderr(self, protocol, stderr):
        """
        Called by :meth:`.ProcessProtocol.errReceived` when
        we receive output on standard error (stderr) from a process.
        """
        self._received_stderr(protocol, stderr)
