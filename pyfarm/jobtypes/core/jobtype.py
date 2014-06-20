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

"""
Job Type Core
=============

This module contains the core job type from which all
other job types are built.  All other job types must
inherit from the :class:`JobType` class in this modle.
"""

import imp
import os
import sys
from datetime import datetime
from string import Template
from os.path import join, basename, expanduser
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

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone, ProcessTerminated
from twisted.python.failure import Failure
from voluptuous import Schema, Required, Optional

from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WorkState
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import post, http_retry_delay
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import memory, system
from pyfarm.agent.sysinfo.user import is_administrator, username
from pyfarm.agent.utility import (
    STRINGS, WHOLE_NUMBERS, TASKS_SCHEMA, JOBTYPE_SCHEMA, uuid)
from pyfarm.jobtypes.core.internals import (
    ITERABLE_CONTAINERS, STDERR, STDOUT, Cache, Process, TypeChecks)
from pyfarm.jobtypes.core.process import (
    ProcessProtocol)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
process_stdout = getLogger("jobtypes.process.stdout")
process_stderr = getLogger("jobtypes.process.stderr")

FROZEN_ENVIRONMENT = ImmutableDict(os.environ.copy())


class JobType(Cache, Process, TypeChecks):
    """
    Base class for all other job types.  This class is intended
    to abstract away many of the asynchronous necessary to run
    a job type on an agent.

    :attribute dict assignment:
        This attribute is a dictionary the keys "job", "jobtype" and "tasks".
        self.assignment["job"] is itself a dict with keys "id", "title",
        "data", "environ" and "by".  The most important of those is usually
        "data", which is the dict specified when submitting the job and
        contains jobtype specific data. self.assignment["tasks"] is a list of
        dicts representing the tasks in the current assignment.  Each of
        these dicts has the keys "id" and "frame".  The
        list is ordered by frame number.

    :attribute cache:
        Stores the cached job types

    :attribute process_protocol:
        The protocol object used to communicate between the process
        and the job type.
    """
    ASSIGNMENT_SCHEMA = Schema({
        Required("job"): Schema({
            Required("id"): WHOLE_NUMBERS,
            Optional("title"): STRINGS,
            Optional("data"): dict}),
        Required("jobtype"): JOBTYPE_SCHEMA,
        Optional("tasks"): TASKS_SCHEMA})
    process_protocol = ProcessProtocol
    cache = {}

    def __init__(self, assignment):
        # JobType objects in the future may or may not have explicit tasks
        # associated with when them.  The format of tasks could also change
        # since it's an internal representation so to guard against these
        # changes we just use a simple uuid to represent ourselves in the
        # config dictionary.
        self.uuid = uuid()
        config["jobtypes"][self.uuid] = self

        self.started = Deferred()
        self.stopped = Deferred()

        self.protocols = {}
        self.failed_processes = set()
        self.finished_tasks = set()
        self.failed_tasks = set()
        self.stdout_line_fragments = []
        self.assignment = ImmutableDict(self.ASSIGNMENT_SCHEMA(assignment))

        # JobType objects in the future may or may not have explicit tasks
        # associated with when them.  The format of tasks could also change
        # since it's an internal representation so to guard against these
        # changes we just use a simple uuid to represent ourselves in the
        # config dictionary.
        self._uuid = uuid()
        config["jobtypes"][self._uuid] = self

        # NOTE: Don't call this logging statement before the above, we need
        # self.assignment
        logger.debug("Instanced %r", self)

    def node(self):
        """
        Returns live information about this host, the operating system,
        hardware, and several other pieces of global data which is useful
        inside of the job type.  Currently data from this method includes:

            * **master_api** - The base url the agent is using to
              communicate with the master.
            * **hostname** - The hostname as reported to the master.
            * **systemid** - The unique identifier used to identify.
              this agent to the master.
            * **id** - The database id of the agent as given to us by
              the master on startup of the agent.
            * **cpus** - The number of CPUs reported to the master
            * **ram** - The amount of ram reported to the master.
            * **total_ram** - The amount of ram, in megabytes,
              that's installed on the system regardless of what
              was reported to the master.
            * **free_ram** - How much ram, in megabytes, is free
              for the entire system.
            * **consumed_ram** - How much ram, in megabytes, is
              being consumed by the agent and any processes it has
              launched.
            * **admin** - Set to True if the current user is an
              administrator or 'root'.
            * **user** - The username of the current user.
            * **case_sensitive_files** - True if the file system is
              case sensitive.
            * **case_sensitive_env** - True if environment variables
              are case sensitive.
            * **machine_architecture** - The architecture of the machine
              the agent is running on.  This will return 32 or 64.
            * **operating_system** - The operating system the agent
              is executing on.  This value will be 'linux', 'mac' or
              'windows'.  In rare circumstances this could also
              be 'other'.

        :raises KeyError:
            Raised if one or more keys are not present in
            the global configuration object.

            This should rarely if ever be a problem under normal
            circumstances.  The exception to this rule is in
            unittests or standalone libraries with the global
            config object may not be populated.
        """
        try:
            machine_architecture = system.machine_architecture()
        except NotImplementedError:
            logger.warning(
                "Failed to determine machine architecture.  This is a "
                "bug, please report it.")
            raise

        return {
            "master_api": config.get("master-api"),
            "hostname": config["hostname"],
            "systemid": config["systemid"],
            "id": config["id"],
            "cpus": config["cpus"],
            "ram": config["ram"],
            "total_ram": int(memory.total_ram()),
            "free_ram": int(memory.ram_free()),
            "consumed_ram": int(memory.total_consumption()),
            "admin": is_administrator(),
            "user": username(),
            "case_sensitive_files": system.filesystem_is_case_sensitive(),
            "case_sensitive_env": system.environment_is_case_sensitive(),
            "machine_architecture": machine_architecture,
            "operating_system": system.operating_system()}

    def __repr__(self):
        formatting = "%s(job=%r, tasks=%r, jobtype=%r, version=%r, title=%r)"
        return formatting % (
            self.__class__.__name__,
            self.assignment["job"]["id"],
            tuple(task["id"] for task in self.assignment["tasks"]),
            str(self.assignment["jobtype"]["name"]),
            self.assignment["jobtype"]["version"],
            str(self.assignment["job"]["title"]))

    @classmethod
    def load(cls, assignment):
        """
        Given ``data`` this class method will load the job type either
        from cache or from the master and then instance it with the
        incoming assignment data
        """
        cls.ASSIGNMENT_SCHEMA(assignment)

        result = Deferred()
        cache_key = (
            assignment["jobtype"]["name"], assignment["jobtype"]["version"])

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

        if cls.CACHE_DIRECTORY is None or cache_key not in cls.cache:
            def download_complete(response):
                # Server is offline or experiencing issues right
                # now so we should retry the request.
                if response.code >= INTERNAL_SERVER_ERROR:
                    return reactor.callLater(
                        http_retry_delay(),
                        response.request.retry)

                if cls.CACHE_DIRECTORY is None:
                    return load_jobtype((response.json(), None))

                # When the download is complete, cache the results
                caching = cls._cache_jobtype(cache_key, response.json())
                caching.addCallback(load_jobtype)

            # Start the download
            download = cls._download_jobtype(
                assignment["jobtype"]["name"],
                assignment["jobtype"]["version"])
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

    def get_environment(self):
        """
        Constructs an environment dictionary that can be used
        when a process is spawned by a job type.
        """
        environment = {}
        config_environment = config.get("jobtype_default_environment")

        if config.get("jobtype_include_os_environ"):
            environment.update(FROZEN_ENVIRONMENT)

        if isinstance(config_environment, dict):
            environment.update(config_environment)

        elif config_environment is not None:
            logger.warning(
                "Expected a dictionary for `jobtype_default_environment`, "
                "ignoring the configuration value.")

        return environment

    def get_command_list(self, cmdlist):
        """
        Return a list of command to be used when running the process
        as a read-only tuple.
        """
        self._check_command_list_inputs(cmdlist)
        return tuple(map(self.expandvars, cmdlist))

    # TODO: This needs more command line arguments and configuration options
    def get_csvlog_path(self, tasks, now=None):
        """
        Returns the path to the comma separated value (csv) log file.
        The agent stores logs from processes in a csv format so we can store
        additional information such as a timestamp, line number, stdout/stderr
        identification and the the log message itself.
        """
        self._check_csvlog_path_inputs(tasks, now)

        if now is None:
            now = datetime.utcnow()

        return join(
            config["task-log-dir"],
            "%s_%s_%s.csv" % (
                now.strftime("%G%m%d%H%M%S"),
                self.assignment["job"]["id"],
                "-".join(map(str, list(task["id"]for task in tasks)))))

    # TODO: internal implementation like the doc string says
    # TODO: reflow the doc string text for a better layout
    def get_command_data(self):
        """
        Overridable.  Returns the command, arguments, environment,
        working directory and optionally a freeform variable for the command
        to execute the current assignment in the form of a CommandData
        (currently ProcessInputs) object. Should not be used when this jobtype
        requires more than one process for one assignment.  May not get called
        at all if start() was overridden.  Should use map_path() on all paths.
        Default implementation does nothing. For simple jobtypes that use
        exactly one process per assignment, this is the most important method
        to implement.
        """
        raise NotImplementedError("`get_command_data` must be implemented")

    # TODO: finish map_path() implementation
    def map_path(self, path):
        """
        Takes a string argument.  Translates a given path for any OS to
        what it should be on this particular node.  Might communicate with
        the master to achieve this.
        """
        self._check_map_path_inputs(path)
        path = self.expandvars(path)
        return path

    def expandvars(self, value, environment=None, expand=None):
        """
        Expands variables inside of a string using an environment.  Exp

        :param string value:
            The path to expand

        :param dict environment:
            The environment to use for expanding ``value``.  If this
            value is None (the default) then we'll use :meth:`get_environment`
            to build this value.

        :param bool expand:
            When not provided we use the ``jobtype_expandvars`` configuration
            value to set the default.  When this value is True we'll
            perform environment variable expansion otherwise we return
            ``value`` untouched.
        """
        self._check_expandvars_inputs(value, environment)

        if expand is None:
            expand = config.get("jobtype_expandvars")

        if not expand:
            return value

        if environment is None:
            environment = self.get_environment()

        return Template(expanduser(value)).safe_substitute(**environment)

    # TODO: finish implementation, remove process_inputs fully
    def spawn_process(
            self, command, arguments, working_directory, environment,
            user, group):
        """
        Starts one child process using input from :meth:`command_data`.
        Job types should never start child processes through any other
        means.  The only exception to this rule is code that resides in
        :meth:`prepare_for_job`, which should use
        :meth:`spawn_persistent_job_process` instead.

        :raises OSError:
            Raised if `working_directory` was provided but the provided
            path does not exist
        """
        self._check_spawn_process_inputs(
            command, arguments, working_directory, environment,
            user, group)

        uid = None
        gid = None

        # Convert user to uid
        if all([user is not None, pwd is not NotImplemented]):
            uid = self._get_uidgid(user, "username", "get_uid", pwd, "pwd")

        # Convert group to gid
        if all([group is not None, grp is not NotImplemented]):
            gid = self._get_uidgid(group, "group", "get_gid", grp, "grp")

        # TODO: finish implementation

        # Prepare the arguments for the spawnProcess call
        environment = self.get_environment()
        commands = self.get_command_list(process_inputs.command)

        # args - name of command being run + input arguments
        kwargs = {
            "args": [basename(commands[0])] + list(commands[1:]),
            "env": environment,
            "path": self.expandvars(process_inputs.path)}

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
        # TODO: this should not use tasks
        logfile = self.get_csvlog_path(process_inputs.tasks)
        self.protocols[protocol.id] = protocol

        self._start_logging(protocol, logfile)

        # TODO: do validation of kwargs here so we don't do it in one location
        # * environment (dict, strings only)
        # * path exists (see previous setup for this in the history)
        #
        self._spawn_process(environment, protocol, commands[0], kwargs)

        return protocol

    def start(self):
        """
        This method is called when the job type should start
        working.  Depending on the job type's implementation this will
        prepare and start one more more processes.
        """
        # Make sure start() is not called twice
        if self.started.called:
            raise RuntimeError("%s has already been started" % self)

        # TODO: add deferred handlers
        # TODO: collect all tasks and depending on the relationship
        # between tasks and processes we have to change how we notify
        # the master (and how we cancel other tasks which are queued)
        for process_inputs in self.get_command_data():
            self.spawn_process(process_inputs)

        return self.started

    def stop(self):
        """
        This method is called when the job type should stop
        running.  This will terminate any processes associated with
        this job type and also inform the master of any state changes
        to an associated task or tasks.
        """
        if self.stopped.called:
            raise RuntimeError("%s has already been stopped" % self)

        # TODO: stop all running processes
        # TODO: notify master of stopped task(s)
        # TODO: chain this callback to the completion of our request to master
        def finished_processes():
            self.stopped.callback(True)
            config["jobtypes"].pop(self.uuid)

        finished_processes()
        return self.stopped

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
        self._check_set_states_inputs(tasks, state)

        for task in tasks:
            self.set_task_state(task, state, error=error)

    # TODO: refactor so `task` is an integer, not a dictionary
    def set_task_state(self, task, state, error=None):
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

        elif "id" not in task or not isinstance(task["id"], INTEGER_TYPES):
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
                if task not in self.failed_tasks:
                    self.failed_tasks.add(task)

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

            def post_update(post_url, post_data, delay=0):
                post_func = partial(
                    post, post_url,
                    data=post_data,
                    callback=lambda x: result_callback(
                        post_url, post_data, task["id"], state, x),
                    errback=lambda x: error_callback(post_url, post_data, x))
                reactor.callLater(delay, post_func)

            def result_callback(cburl, cbdata, task_id, cbstate, response):
                if 500 <= response.code < 600:
                    logger.error(
                        "Error while posting state update for task %s "
                        "to %s, return code is %s, retrying",
                        task_id, cbstate, response.code)
                    post_update(cburl, cbdata, delay=http_retry_delay())

                elif response.code != OK:
                    # Nothing else we could do about that, this is
                    # a problem on our end.  We should only encounter
                    # this error during development
                    logger.error(
                        "Could not set state for task %s to %s, server "
                        "response code was %s",
                        task_id, cbstate, response.code)

                else:
                    logger.info(
                        "Set state of task %s to %s on master",
                        task_id, cbstate)
                    if cbstate == WorkState.DONE \
                            and task_id not in self.finished_tasks:
                        self.finished_tasks.add(task_id)

            def error_callback(cburl, cbdata, _):
                logger.error(
                    "Error while posting state update for task, retrying")
                post_update(cburl, cbdata, delay=http_retry_delay())

            # Initial attempt to make an update with an explicit zero
            # delay.
            post_update(url, data, delay=0)

            # TODO: if new state is the equiv. of 'stopped', stop the process
            # and POST the change

    def get_local_task_state(self, task_id):
        """
        Returns None if the state of this task has not been changed
        locally since this assignment has started.  This method does
        not communicate with the master.
        """
        if task_id in self.finished_tasks:
            return WorkState.DONE
        elif task_id in self.failed_tasks:
            return WorkState.FAILED
        else:
            return None

    def is_successful(self, reason):
        """
        User-overridable method that determines whether the process
        referred to by a protocol instance has exited successfully.
        Default implementation returns true if the process's return
        code was 0 and false in all other cases.
        """
        return (
            reason.type is ProcessDone and
            reason.value.exitCode == 0)

    def process_stopped(self, protocol, reason):
        """
        Called by :meth:`.ProcessProtocol.processEnded` when a process
        stopped running.
        """
        self._process_stopped(protocol, reason)

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
        dangling_fragment = None
        if "\n" in stdout:
            ends_on_fragment = True
            if stdout[-1] == "\n":
                ends_on_fragment = False
            lines = stdout.split("\n")
            if ends_on_fragment:
                dangling_fragment = lines.pop(-1)
            for line in lines:
                if protocol.id in self.stdout_line_fragments:
                    line_out = self.stdout_line_fragments[protocol.id] + line
                    del self.stdout_line_fragments[protocol.id]
                    if len(line_out) > 0 and line_out[-1] == "\r":
                        line_out = line_out[:-1]
                    self.received_stdout_line(protocol, line_out)
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
