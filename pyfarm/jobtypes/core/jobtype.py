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
.. |ProcessProtocol| replace:: pyfarm.jobtypes.core.process.ProcessProtocol

Job Type Core
=============

This module contains the core job type from which all
other job types are built.  All other job types must
inherit from the :class:`JobType` class in this modle.
"""

import os
import tempfile
from errno import EEXIST
from datetime import datetime, timedelta
from functools import partial
from string import Template
from os.path import expanduser, abspath, isdir, join
from pprint import pformat
from uuid import uuid4

try:
    from httplib import OK, INTERNAL_SERVER_ERROR, CONFLICT, CREATED
except ImportError:  # pragma: no cover
    from http.client import OK, INTERNAL_SERVER_ERROR, CONFLICT, CREATED

from twisted.internet import reactor
from twisted.internet.error import ProcessDone, ProcessTerminated
from twisted.python.failure import Failure
from voluptuous import Schema, Required, Optional

from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES, WorkState, WINDOWS
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import post, http_retry_delay
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import memory, system
from pyfarm.agent.sysinfo.user import is_administrator, username
from pyfarm.agent.utility import (
    TASKS_SCHEMA, JOBTYPE_SCHEMA, JOB_SCHEMA, validate_uuid)
from pyfarm.jobtypes.core.internals import (
    USER_GROUP_TYPES, Cache, Process, TypeChecks, System, pwd, grp)
from pyfarm.jobtypes.core.log import STDOUT, STDERR, logpool
from pyfarm.jobtypes.core.process import ProcessProtocol

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
process_stdout = getLogger("jobtypes.process.stdout")
process_stderr = getLogger("jobtypes.process.stderr")


FROZEN_ENVIRONMENT = ImmutableDict(os.environ.copy())


class CommandData(object):
    """
    Stores data to be returned by :meth:`JobType.get_command_data`.  Instances
    of this class are alosed used by :meth:`JobType.spawn_process_inputs` at
    execution time.

    .. note::

        This class does not perform any key of path resolution by default.  It
        is assumed this has already been done using something like
        :meth:`JobType.map_path`

    :arg string command:
        The command that will be executed when the process runs.

    :arg arguments:
        Any additional arguments to be passed along to the command being
        launched.

    :keyword dict env:
        If provided, this will be the environment to launch the command
        with.  If this value is not provided then a default environment
        will be setup using :meth:`set_default_environment` when
        :meth:`JobType.start` is called.  :meth:`JobType.start` itself will
        use :meth:`JobType.set_default_environment` to generate the default
        environment.

    :keyword string cwd:
        The working directory the process should execute in.  If not provided
        the process will execute in whatever the directory the agent is
        running inside of.

    :type user: string or integer
    :keyword user:
        The username or user id that the process should run as.  On Windows
        this keyword is ignored and on Linux this requires the agent to be
        executing as root.  The value provided here will be run through
        :meth:`JobType.get_uid_gid` to map the incoming value to an integer.

    :type group: string or integer
    :keyword group:
        Same as ``user`` above except this sets the group the process will
        execute.

    :keyword id:
        An arbitrary id to associate with the resulting process protocol.  This
        can help identify
    """
    def __init__(self, command, *arguments, **kwargs):
        self.command = command
        self.arguments = tuple(map(str, arguments))
        self.env = kwargs.pop("env", None)
        self.cwd = kwargs.pop("cwd", None)
        self.user = kwargs.pop("user", None)
        self.group = kwargs.pop("group", None)
        self.id = kwargs.pop("id", None)

        if kwargs:
            raise ValueError(
                "Unexpected keywords present in kwargs: %s" % kwargs.keys())

    def __repr__(self):
        return "Command(command=%r, arguments=%r, cwd=%r, user=%r, group=%r, " \
               "env=%r)" % (self.command, self.arguments, self.cwd,
                            self.user, self.group, self.env)

    def validate(self):
        """
        Validates that the attributes on an instance of this class contain
        values we expect.  This method is called externally by the job type in
        :meth:`JobType.start` and may correct some instance attributes.
        """
        if not isinstance(self.command, STRING_TYPES):
            raise TypeError("Expected a string for `command`")

        if not isinstance(self.env, dict) and self.env is not None:
            raise TypeError("Expected a dictionary for `env`")

        if not isinstance(self.user, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `user`")

        if not isinstance(self.group, USER_GROUP_TYPES):
            raise TypeError("Expected string, integer or None for `group`")

        if WINDOWS:  # pragma: no cover
            if self.user is not None:
                logger.warning("`user` is ignored on Windows")
                self.user = None

            if self.group is not None:
                logger.warning("`group` is ignored on Windows")
                self.group = None

        elif self.user is not None or self.group is not None \
                and not is_administrator():
            raise EnvironmentError(
                "Cannot change user or group without being admin.")

        if self.cwd is None:
            if not config["agent_chdir"]:
                self.cwd = os.getcwd()
            else:
                self.cwd = config["agent_chdir"]

        if isinstance(self.cwd, STRING_TYPES):
            if not isdir(self.cwd):
                raise OSError(
                    "`cwd` %s does not exist" % self.cwd)

        elif self.cwd is not None:
            raise TypeError("Expected a string for `cwd`")

    def set_default_environment(self, value):
        """
        Sets the environment to ``value`` if the internal ``env`` attribute
        is None.  By default this method is called by the job type and passed
        in the results from :meth:`pyfarm.jobtype.core.JobType.get_environment`
        """
        if self.env is None:
            assert isinstance(value, dict)
            self.env = value


class JobType(Cache, System, Process, TypeChecks):
    """
    Base class for all other job types.  This class is intended
    to abstract away many of the asynchronous necessary to run
    a job type on an agent.

    :cvar set PERSISTENT_JOB_DATA:
        A dictionary of job ids and data that :meth:`prepare_for_job` has
        produced.  This is used during :meth:`__init__` to set
        ``persistent_job_data``.

    :cvar CommandData COMMAND_DATA_CLASS:
        If you need to provide your own class to represent command data you
        should override this attribute.  This attribute is used by by methods
        within this class to do type checking.

    :cvar ProcessProtocol PROCESS_PROTOCOL:
        The protocol object used to communicate with each process
        spawned

    :cvar voluptuous.Schema ASSIGNMENT_SCHEMA:
        The schema of an assignment.  This object helps to validate the
        incoming assignment to ensure it's not missing any data.

    :arg dict assignment:
        This attribute is a dictionary the keys "job", "jobtype" and "tasks".
        self.assignment["job"] is itself a dict with keys "id", "title",
        "data", "environ" and "by".  The most important of those is usually
        "data", which is the dict specified when submitting the job and
        contains jobtype specific data. self.assignment["tasks"] is a list of
        dicts representing the tasks in the current assignment.  Each of
        these dicts has the keys "id" and "frame".  The
        list is ordered by frame number.

    :ivar UUID uuid:
        This is the unique identifier for the job type instance and is
        automatically set when the class is instanced.  This is used by the
        agent to track assignments and job type instances.

    :ivar set finished_tasks:
        A set of tasks that have had their state changed to finished through
        :meth:`set_task_state`.  At the start of the assignment, this list is
        empty.

    :ivar set failed_tasks:
        This is analogous to ``finished_tasks`` except it contains failed
        tasks only.
    """
    PERSISTENT_JOB_DATA = {}
    COMMAND_DATA = CommandData
    PROCESS_PROTOCOL = ProcessProtocol
    ASSIGNMENT_SCHEMA = Schema({
        Required("id"): validate_uuid,
        Required("job"): JOB_SCHEMA,
        Required("jobtype"): JOBTYPE_SCHEMA,
        Optional("tasks"): TASKS_SCHEMA})

    def __init__(self, assignment):
        super(JobType, self).__init__()

        # Private attributes which persist with the instance.  These
        # generally should not be modified directly.
        self._tempdir = None  # the defacto tempdir for this instance
        self._tempdirs = set()  # list of any temp directories created
        self._stdout_line_fragments = {}
        self._stderr_line_fragments = {}

        # JobType objects in the future may or may not have explicit tasks
        # associated with when them.  The format of tasks could also change
        # since it's an internal representation so to guard against these
        # changes we just use a simple uuid to represent ourselves.
        self.uuid = uuid4()
        self.processes = {}
        self.failed_processes = set()
        self.failed_tasks = set()
        self.finished_tasks = set()
        self.assignment = ImmutableDict(self.ASSIGNMENT_SCHEMA(assignment))
        self.persistent_job_data = None

        # Add our instance to the job type instance tracker dictionary
        # as well as the dictionary containing the current assignment.
        config["jobtypes"][self.uuid] = self
        config["current_assignments"][assignment["id"]]["jobtype"].update(
            id=self.uuid)

        # NOTE: Don't call this logging statement before the above, we need
        # self.assignment
        logger.debug("Instanced %r", self)

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
        Given an assignment this class method will load the job type either
        from cache or from the master.

        :param dict assignment:
            The dictionary containing the assignment.  This will be
            passed into an instance of ``ASSIGNMENT_SCHEMA`` to validate
            that the internal data is correct.
        """
        cls.ASSIGNMENT_SCHEMA(assignment)

        cache_key = cls._cache_key(assignment)
        logger.debug("Cache key for assignment is %s", cache_key)

        if config["jobtype_enable_cache"] or cache_key not in cls.cache:
            logger.debug("Jobtype not in cache or cache disabled")
            download = cls._download_jobtype(
                assignment["jobtype"]["name"],
                assignment["jobtype"]["version"])
            download.addCallback(cls._jobtype_download_complete, cache_key)
            return download
        else:
            logger.debug("Caching jobtype")
            return cls._load_jobtype(cls.cache[cache_key], None)

    @classmethod
    def prepare_for_job(cls, job):
        """
        .. note::
            This method is not yet implemented

        Called before a job executes on the agent first the first time.
        Whatever this classmethod returns will be available as
        ``persistent_job_data`` on the job type instance.

        :param int job:
            The job id which prepare_for_job is being run for

        By default this method does nothing.
        """
        pass

    @classmethod
    def cleanup_after_job(cls, persistent_data):
        """
        .. note::
            This method is not yet implemented

        This classmethod will be called after the last assignment
        from a given job has finished on this node.

        :param persistent_data:
            The persistent data that :meth:`prepare_for_job` produced.  The
            value for this data may be ``None`` if :meth:`prepare_for_job`
            returned None or was not implemented.
        """
        pass

    @classmethod
    def spawn_persistent_process(cls, job, command_data):
        """
        .. note::
            This method is not yet implemented

        Starts one child process using an instance of :class:`CommandData` or
        similiar input.  This process is intended to keep running until the
        last task from this job has been processed, potentially spanning more
        than one assignment.  If the spawned process is still running then
        we'll cleanup the process after :meth:`cleanup_after_job`
        """
        pass

    def node(self):
        """
        Returns live information about this host, the operating system,
        hardware, and several other pieces of global data which is useful
        inside of the job type.  Currently data from this method includes:

            * **master_api** - The base url the agent is using to
              communicate with the master.
            * **hostname** - The hostname as reported to the master.
            * **agent_id** - The unique identifier used to identify.
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
            "hostname": config["agent_hostname"],
            "agent_id": config["agent_id"],
            "id": config["agent_id"],
            "cpus": int(config["agent_cpus"]),
            "ram": int(config["agent_ram"]),
            "total_ram": int(memory.total_ram()),
            "free_ram": int(memory.free_ram()),
            "consumed_ram": int(memory.total_consumption()),
            "admin": is_administrator(),
            "user": username(),
            "case_sensitive_files": system.filesystem_is_case_sensitive(),
            "case_sensitive_env": system.environment_is_case_sensitive(),
            "machine_architecture": machine_architecture,
            "operating_system": system.operating_system()}

    def assignments(self):
        """Short cut method to access tasks"""
        return self.assignment["tasks"]

    def tempdir(self, new=False, remove_on_finish=True):
        """
        Returns a temporary directory to be used within a job type.
        By default once called the directory will be created on disk
        and returned from this method.

        Calling this method multiple times will return the same directory
        instead of creating a new directory unless ``new`` is set to True.

        :param bool new:
            If set to ``True`` then return a new directory when called.  This
            however will not replace the 'default' temp directory.

        :param bool remove_on_finish:
            If ``True`` then keep track of the directory returned so it
            can be removed when the job type finishes.
        """
        if not new and self._tempdir is not None:
            return self._tempdir

        parent_dir = config["jobtype_tempdir_root"].replace(
            "$JOBTYPE_UUID", str(self.uuid))

        try:
            os.makedirs(parent_dir)
        except OSError as e:  # pragma: no cover
            if e.errno != EEXIST:
                logger.error("Failed to create %s: %s", parent_dir, e)
                raise

        self._tempdirs.add(parent_dir)
        tempdir = tempfile.mkdtemp(dir=parent_dir)
        logger.debug(
            "%s.tempdir() created %s", self.__class__.__name__, tempdir)

        # Keep track of the directory so we can cleanup all of them
        # when the job type finishes.
        if remove_on_finish:
            self._tempdirs.add(tempdir)

        if not new and self._tempdir is None:
            self._tempdir = tempdir

        return tempdir

    def get_uid_gid(self, user, group):
        """
        **Overridable**.  This method to convert a named user and group
        into their respective user and group ids.
        """
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

        # All keys and values must be strings.  Normally this is not an issue
        # but it's possible to create an environment using the config which
        # contains non-strings.
        for key in environment:
            if not isinstance(key, STRING_TYPES):
                logger.warning(
                    "Environment key %r is not a string.  It will be converted "
                    "to a string.", key)

                value = environment.pop(key)
                key = str(key)
                environment[key] = value

            if not isinstance(environment[key], STRING_TYPES):
                logger.warning(
                    "Environment value for %r is not a string.  It will be "
                    "converted to a string.", key)
                environment[key] = str(environment[key])

        return environment

    def get_command_list(self, cmdlist):
        """
        Return a list of command to be used when running the process
        as a read-only tuple.
        """
        self._check_command_list_inputs(cmdlist)
        return tuple(map(self.expandvars, cmdlist))

    def get_csvlog_path(self, protocol_uuid, create_time=None):
        """
        Returns the path to the comma separated value (csv) log file.
        The agent stores logs from processes in a csv format so we can store
        additional information such as a timestamp, line number, stdout/stderr
        identification and the the log message itself.

        .. note::

            This method should not attempt to create the parent directories
            of the resulting path.  This is already handled by the logger
            pool in a non-blocking fashion.
        """
        self._check_csvlog_path_inputs(protocol_uuid, create_time)

        if create_time is None:
            create_time = datetime.utcnow()
        assert isinstance(create_time, datetime)

        # Include the agent's time offset in create_time for accuracy.
        if config["agent_time_offset"]:
            create_time += timedelta(seconds=config["agent_time_offset"])

        # The default string template implementation cannot
        # handle cases where you have $VARS$LIKE_$THIS.  So we
        # instead iterate over a dictionary and use .replace()
        template_data = {
            "YEAR": str(create_time.year),
            "MONTH": "%02d" % create_time.month,
            "DAY": "%02d" % create_time.day,
            "HOUR": "%02d" % create_time.hour,
            "MINUTE": "%02d" % create_time.minute,
            "SECOND": "%02d" % create_time.second,
            "JOB": str(self.assignment["job"]["id"]),
            "PROCESS": protocol_uuid.hex}

        filename = config["jobtype_task_log_filename"]
        for key, value in template_data.items():
            filename = filename.replace("$" + key, value)
        filepath = join(config["jobtype_task_logs"], filename)

        return abspath(filepath)

    def get_command_data(self):
        """
        **Overridable**.  This method returns the arguments necessary for
        executing a command.  For job types which execute a single process
        per assignment, this is the most important method to implement.

        .. warning::

            This method should not be used when this jobtype requires more
            than one process for one assignment and may not get called at all
            if :meth:`start` was overridden.

        The default implementation does nothing.  When overriding this method
        you should return an instance of ``COMMAND_DATA_CLASS``:

        .. code-block:: python

            return self.COMMAND_DATA(
                "/usr/bin/python", "-c", "print 'hello world'",
                env={"FOO": "bar"}, user="bob")

        See :class:`CommandData`'s class documentation for a full description
        of possible arguments.

        Please note however the default command data class, :class:`CommandData`
        does not perform path expansion.  So instead you have to handle this
        yourself with :meth:`map_path`.
        """
        raise NotImplementedError("`get_command_data` must be implemented")

    # TODO: finish map_path() implementation
    def map_path(self, path):
        """
        Takes a string argument.  Translates a given path for any OS to
        what it should be on this particular node.  This does not communicate
        with the master.
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

    def start(self):
        """
        This method is called when the job type should start
        working.  Depending on the job type's implementation this will
        prepare and start one more more processes.
        """
        environment = self.get_environment()
        command_data = self.get_command_data()

        if isinstance(command_data, self.COMMAND_DATA):
            command_data = [command_data]

        for command in command_data:
            if not isinstance(command, self.COMMAND_DATA):
                raise TypeError(
                    "Expected `%s` instances from "
                    "get_command_data()" % self.COMMAND_DATA.__name__)

            command.validate()
            command.set_default_environment(environment)
            self._spawn_process(command)

    def stop(self, assignment_failed=False, error=None, signal="KILL"):
        """
        This method is called when the job type should stop
        running.  This will terminate any processes associated with
        this job type and also inform the master of any state changes
        to an associated task or tasks.

        :param boolean assignment_failed:
            Whether this means the assignment has genuinely failed.  By default,
            we assume that stopping this assignment was the result of deliberate
            user action (like stopping the job or shutting down the agent), and
            won't treat it as a failed assignment.

        :param string error:
            If the assignment has failed, this string is upload as last_error
            for the failed tasks.

        :param string signal:
            The signal to send the any running processes.  Valid options
            are KILL, TERM or INT.
        """
        logger.debug("JobType.stop() called, signal: %s", signal)
        assert signal in ("KILL", "TERM", "INT")

        self.stop_called = True

        for _, process in self.processes.iteritems():
            if signal == "KILL":
                process.protocol.kill()
            elif signal == "TERM":
                process.protocol.terminate()
            elif signal == "INT":
                process.protocol.interrupt()
            else:
                raise NotImplementedError(
                    "Don't know how to handle signal %r" % signal)

        if assignment_failed:
            for task in self.assignment["tasks"]:
                if task["id"] not in self.finished_tasks:
                    self.set_task_state(task, WorkState.FAILED, error=error)
                else:
                    logger.info(
                        "Task %r is already in finished tasks, not setting "
                        "state to failed", task["id"])
        else:
            for task in self.assignment["tasks"]:
                if task["id"] not in self.failed_tasks:
                    self.set_task_state(task, None, dissociate_agent=True)
                else:
                    logger.info(
                        "Task %r is already in failed tasks, not setting state "
                        "to queued", task["id"])
        # TODO: chain this callback to the completion of our request to master

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
    def set_task_state(self, task, state, error=None, dissociate_agent=False):
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
        if state not in WorkState and state is not None:
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
            # The task has failed
            if state == WorkState.FAILED:
                error = self.format_error(error)
                logger.error("Task %r failed: %r", task, error)
                if task["id"] not in self.failed_tasks:
                    self.failed_tasks.add(task["id"])

            # `error` shouldn't be set if the state is not a failure
            elif error is not None:
                logger.warning(
                    "`set_state` only allows `error` to be set if `state` is "
                    "'failed'.  Discarding error.")
                error = None

            if (state == WorkState.DONE and
                task["id"] not in self.finished_tasks):
                self.finished_tasks.add(task["id"])

            url = "%s/jobs/%s/tasks/%s" % (
                config["master_api"], self.assignment["job"]["id"], task["id"])
            data = {"state": state or "queued"}

            # If the error has been set then update the data we're
            # going to POST
            if isinstance(error, STRING_TYPES):
                data.update(last_error=error)

            elif error is not None:
                logger.error("Expected a string for `error`")

            if dissociate_agent:
                data.update(agent_id=None)

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

            def error_callback(cburl, cbdata, failure_reason):
                logger.error(
                    "Error while posting state update for task, %s, retrying",
                    failure_reason)
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
        **Overridable**.  This method that determines whether the process
        referred to by a protocol instance has exited successfully.

        The default implementation returns ``True`` if the process's return
        code was 0 and ``False` in all other cases.  If you need to
        modify this behavior please be aware that ``reason`` may be an
        integer or an instance of
        :class:`twisted.internet.error.ProcessTerminated` if the process
        terminated without errors or an instance of
        :class:`twisted.python.failure.Failure` if there were problems.

        :raises NotImplementedError:
            Raised if we encounter a condition that the base implementation
            is unable to handle.
        """
        if reason == 0:
            return True
        elif isinstance(reason, INTEGER_TYPES):
            return False
        elif hasattr(reason, "type"):
            return (
                reason.type is ProcessDone and
                reason.value.exitCode == 0)
        else:
            raise NotImplementedError(
                "Don't know how to handle is_successful(%r)" % reason)

    def before_start(self):
        """
        **Overridable**.  This method called directly before :meth:`start`
        itself is called.

        The default implementation does nothing and values returned from
        this method are ignored.
        """
        pass

    def before_spawn_process(self, command, protocol):
        """
        **Overridable**.  This method called directly before a process is
        spawned.

        By default this method does nothing except log information about
        the command we're about to launch both the the agent's log and to
        the log file on disk.

        :param CommandData command:
            An instance of :class:`CommandData` which contains the
            environment to use, command and arguments.  Modifications to
            this object will be applied to the process being spawned.

        :param ProcessProtocol protocol:
            An instance of :class:`pyfarm.jobtypes.core.process.ProcessProtocol`
            which contains the protocol used to communicate between the
            process and this job type.
        """
        logger.info("Spawning %r", command)
        logpool.log(protocol.uuid, "internal",
                    "Command: %s" % command.command)
        logpool.log(protocol.uuid, "internal",
                    "Arguments: %s" % (command.arguments, ))
        logpool.log(protocol.uuid, "internal", "Work Dir: %s" % command.cwd)
        logpool.log(protocol.uuid, "internal", "User/Group: %s %s" % (
            command.user, command.group))
        logpool.log(protocol.uuid, "internal", "Environment:")
        logpool.log(protocol.uuid, "internal", pformat(command.env, indent=4))

    def process_stopped(self, protocol, reason):
        """
        **Overridable**.  This method called when a child process stopped
        running.

        The default implementation will mark all tasks in the current
        assignment as done or failed of there was at least one failed process.
        """
        if len(self.failed_processes) == 0 and not self.stop_called:
            for task in self.assignment["tasks"]:
                if task["id"] not in self.failed_tasks:
                    self.set_task_state(task, WorkState.DONE)
                else:
                    logger.info(
                        "Task %r is already in failed tasks, not setting state "
                        "to %s", task["id"], WorkState.DONE)
        elif not self.stop_called:
            for task in self.assignment["tasks"]:
                if task["id"] not in self.finished_tasks:
                    self.set_task_state(task, WorkState.FAILED)
                else:
                    logger.info(
                        "Task %r is already in finished tasks, not setting "
                        "state to %s", task["id"], WorkState.FAILED)

    def process_started(self, protocol):
        """
        **Overridable**.  This method is called when a child process started
        running.

        The default implementation will mark all tasks in the current
        assignment as running.
        """
        for task in self.assignment["tasks"]:
            self.set_task_state(task, WorkState.RUNNING)

    def process_output(self, protocol, output, line_fragments, line_handler):
        """
        This is a mid-level method which takes output from a process protocol
        then splits and processes it to ensure we pass complete output lines
        to the other methods.

        Implementors who wish to process the output line by line should
        override :meth:`preprocess_stdout_line`, :meth:`preprocess_stdout_line`,
        :meth:`process_stdout_line` or :meth:`process_stderr_line` instead.
        This method is a glue method between other parts of the job type and
        should only be overridden if there's a problem or you want to change
        how lines are split.

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``output``

        :param string output:
            The blob of text or line produced

        :param dict line_fragments:
            The line fragment dictionary containing individual line
            fragments.  This will be either ``self._stdout_line_fragments`` or
            ``self._stderr_line_fragments``.

        :param callable line_handler:
            The function to handle any lines produced.  This will be either
            :meth:`handle_stdout_line` or :meth:`handle_stderr_line`

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        dangling_fragment = None

        if "\n" in output:
            ends_on_fragment = True

            if output[-1] == "\n":
                ends_on_fragment = False

            lines = output.split("\n")
            if ends_on_fragment:
                dangling_fragment = lines.pop(-1)
            else:
                lines.pop(-1)

            for line in lines:
                if protocol.uuid in line_fragments:
                    line_out = line_fragments.pop(protocol.uuid)
                    line_out += line
                    if line_out and line_out[-1] == "\r":
                        line_out = line_out[:-1]

                    line_handler(protocol, line_out)

                else:
                    if line and line[-1] == "\r":
                        line = line[:-1]

                    line_handler(protocol, line)

            if ends_on_fragment:
                line_fragments[protocol.uuid] = dangling_fragment
        else:
            if protocol.uuid in line_fragments:
                line_fragments[protocol.uuid] += output

            else:
                line_fragments[protocol.uuid] = output

    def handle_stdout_line(self, protocol, stdout):
        """
        Takes a :class:`.ProcessProtocol` instance and ``stdout``
        line produced by :meth:`process_output` and runs it through all
        the steps necessary to preprocess, format, log and handle the line.

        The default implementation will run ``stdout`` through several methods
        in order:

            * :meth:`preprocess_stdout_line`
            * :meth:`format_stdout_line`
            * :meth:`log_stdout_line`
            * :meth:`process_stdout_line`

        .. warning::

            This method is not private however it's advisable to override
            the methods above instead of this one.  Unlike this method,
            which is more generalized and invokes several other methods,
            the above provide more targeted functionality.

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stdout``

        :param string stderr:
            A complete line to ``stderr`` being emitted by the process

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        # Preprocess
        preprocessed = self.preprocess_stdout_line(protocol, stdout)
        if preprocessed is not None:
            stdout = preprocessed
        if preprocessed is False:
            return

        # Format
        formatted = self.format_stdout_line(protocol, stdout)
        if formatted is not None:
            stdout = formatted

        # Log
        self.log_stdout_line(protocol, stdout)

        # Custom Processing
        self.process_stdout_line(protocol, stdout)

    def handle_stderr_line(self, protocol, stderr):
        """
        **Overridable**.  Takes a :class:`.ProcessProtocol` instance and
        ``stderr`` produced by :meth:`process_output` and runs it through all
        the steps necessary to preprocess, format, log and handle the line.

        The default implementation will run ``stderr`` through several methods
        in order:

            * :meth:`preprocess_stderr_line`
            * :meth:`format_stderr_line`
            * :meth:`log_stderr_line`
            * :meth:`process_stderr_line`

        .. warning::

            This method is overridable however it's advisable to override
            the methods above instead.  Unlike this method, which is more
            generalized and invokes several other methods, the above provide
            more targeted functionality.

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stdout``

        :param string stderr:
            A complete line to ``stderr`` being emitted by the process

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        # Preprocess
        preprocessed = self.preprocess_stderr_line(protocol, stderr)
        if preprocessed is not None:
            stderr = preprocessed
        if preprocessed == False:
            return

        # Format
        formatted = self.format_stderr_line(protocol, stderr)
        if formatted is not None:
            stderr = formatted

        # Log
        self.log_stderr_line(protocol, stderr)

        # Custom Processing
        self.process_stderr_line(protocol, stderr)

    def preprocess_stdout_line(self, protocol, stdout):
        """
        **Overridable**.  Provides the ability to manipulate ``stdout`` or
        ``protocol`` before it's passed into any other line handling methods.

        *The default implementation does nothing.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stdout``

        :param string stderr:
            A complete line to ``stdout`` before any formatting or logging
            has occurred.

        :rtype: string
        :return:
            This method returns nothing by default but when overridden should
            return a string which will be used in line handling methods such
            as :meth:`format_stdout_line`, :meth:`log_stdout_line` and
            :meth:`process_stdout_line`.
        """
        pass

    def preprocess_stderr_line(self, protocol, stderr):
        """
        **Overridable**.  Formats a line from ``stdout`` before it's passed onto
        methods such as :meth:`log_stdout_line` and :meth:`process_stdout_line`.

        *The default implementation does nothing.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stderr``

        :param string stderr:
            A complete line to ``stderr`` before any formatting or logging
            has occurred.

        :rtype: string
        :return:
            This method returns nothing by default but when overridden should
            return a string which will be used in line handling methods such
            as :meth:`format_stderr_line`, :meth:`log_stderr_line` and
            :meth:`process_stderr_line`.
        """
        pass

    def format_stdout_line(self, protocol, stdout):
        """
        **Overridable**.  Formats a line from ``stdout`` before it's passed onto
        methods such as :meth:`log_stdout_line` and :meth:`process_stdout_line`.

        *The default implementation does nothing.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stdout``

        :param string stdout:
            A complete line from process to format and return.

        :rtype: string
        :return:
            This method returns nothing by default but when overridden should
            return a string which will be used in :meth:`log_stdout_line` and
            :meth:`process_stdout_line`
        """
        pass

    def format_stderr_line(self, protocol, stderr):
        """
        **Overridable**.  Formats a line from ``stderr`` before it's passed onto
        methods such as :meth:`log_stderr_line` and :meth:`process_stderr_line`.

        *The default implementation does nothing.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stderr``

        :param string stderr:
            A complete line from the process to format and return.

        :rtype: string
        :return:
            This method returns nothing by default but when overridden should
            return a string which will be used in :meth:`log_stderr_line` and
            :meth:`process_stderr_line`
        """
        pass

    def log_stdout_line(self, protocol, stdout):
        """
        **Overridable**.  Called when we receive a complete line on stdout from
        the process.

        The default implementation will use the global logging pool to
        log ``stdout`` to a file.

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stdout``

        :param string stderr:
            A complete line to ``stdout`` that has been formatted and is ready
            to log to a file.

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        if config["jobtype_capture_process_output"]:
            process_stdout.info("task %r: %s", protocol.id, stdout)
        else:
            logpool.log(protocol.uuid, STDOUT, stdout)

    def log_stderr_line(self, protocol, stderr):
        """
        **Overridable**.  Called when we receive a complete line on stderr from
        the process.

        The default implementation will use the global logging pool to
        log ``stderr`` to a file.

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stderr``

        :param string stderr:
            A complete line to ``stderr`` that has been formatted and is ready
            to log to a file.

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        if config["jobtype_capture_process_output"]:
            process_stdout.info("task %r: %s", protocol.id, stderr)
        else:
            logpool.log(protocol.uuid, STDERR, stderr)

    def process_stderr_line(self, protocol, stderr):
        """
        **Overridable**.  This method is called when we receive a complete
        line to ``stderr``.  The line will be preformatted and will already
        have been sent for logging.

        *The default implementation sends ``stderr`` and ``protocol`` to
        :meth:`process_stdout_line`.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stderr``

        :param string stderr:
            A complete line to ``stderr`` after it has been formatted and
            logged.

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        self.process_stdout_line(protocol, stderr)

    def process_stdout_line(self, protocol, stdout):
        """
        **Overridable**.  This method is called when we receive a complete
        line to ``stdout``.  The line will be preformatted and will already
        have been sent for logging.

        *The default implementation does nothing.*

        :type protocol: :class:`.ProcessProtocol`
        :param protocol:
            The protocol instance which produced ``stderr``

        :param string stdout:
            A complete line to ``stdout`` after it has been formatted and
            logged.

        :return:
            This method returns nothing by default and any return value
            produced by this method will not be consumed by other methods.
        """
        pass
