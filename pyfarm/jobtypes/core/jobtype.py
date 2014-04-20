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
import sys
from string import Template
from collections import namedtuple
from os.path import join, dirname, isfile, isdir

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
from twisted.internet.defer import Deferred, DeferredList

from pyfarm.core.config import read_env
from pyfarm.core.enums import INTERGER_TYPES, STRING_TYPES, WorkState
from pyfarm.core.logger import getLogger
from pyfarm.core.sysinfo.user import is_administrator
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.jobtypes.core.process import (
    ProcessProtocol, ProcessInputs, ReplaceEnvironment)

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")


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
Task = namedtuple("Task", ("protocol", "process", "command", "kwargs"))


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
    """
    ignore_uid_gid_mapping_errors = False
    process_protocol = ProcessProtocol
    cache = {}

    def __init__(self, assignment):
        assert isinstance(assignment, dict)
        self.assignment = ImmutableDict(assignment)
        self.running_tasks = {}

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

    def tasks(self):
        """Short cut method to access tasks"""
        return self.assignment["tasks"]

    #
    # Functions related to loading the job type and/or
    # interacting with the cache
    #

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

    #
    # External functions for setting up the job type
    #

    def build_process_protocol(self, jobtype, task):
        """
        Returns the process protocol object used to spawn connect
        a job type to a running process.  By default this instance
        :cvar:`.process_protocol` class variable.

        :raises TypeError:
            raised if the protocol object is not a subclass of
            :class:`.ProcessProtocol`
        """
        instance = self.process_protocol(jobtype, task)

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

    # TODO: add support to get explicit uid/gid for launching processes
    # TODO: this needs to be a deferred in a thread, `pwd` *could* hit netio
    def usrgrp_to_uidgid(self, user, group):
        """Convert the provided username and group into a uid/gid pair"""
        user_name = user
        group_name = group
        if pwd is NotImplemented or grp is NotImplemented:
            logger.warning(
                "This platform does not implement the `pwd` module, cannot "
                "convert username to uid/gid pair")
            return None, None

        elif not is_administrator():
            # Not supported other wise because we need the uid/gid
            # to change the process owner, something we can't do without
            # root/admin powers
            logger.warning(
                "Conversion from username to uid/gid pair is only support "
                "when running as an administrator.")
            return None, None

        else:
            logger.debug("Mapping user and group to uid/gid")

            # Remap user -> uid
            try:
                data = pwd.getpwnam(user)
                user = data.pw_uid
            except KeyError:
                logger.error("Failed to resolve user %r into a uid", user)
                if not self.ignore_uid_gid_mapping_errors:
                    raise
                return None, None

            # Remap group -> gid
            try:
                data = grp.getgrnam(group)
                group = data.gr_gid
            except KeyError:
                logger.error("Failed to resolve group %r into a gid", group)
                if not self.ignore_uid_gid_mapping_errors:
                    raise
                return None, None

            logger.debug(
                "Mapped user/group (%r, %r) -> (%r, %r)",
                user_name, group_name, user, group)
            return user, group

    # TODO: finish required/not required docs
    # TODO: document chdir behavior (None - use PWD of env or agent chroot,
    # string - check if it's a directory or if we use expand vars on it)
    def build_process_inputs(self):
        """
        This method constructs and returns all the arguments necessary
        to execute one or multiple processes on the system.  An example
        implementation may look similar to this:

        >>> from pyfarm.jobtypes.core.jobtype import ProcessInputs
        >>> def build_process_inputs(self):
        ...     for task in self.tasks():
        ...         ProcessInputs(
        ...             task=task,
        ...             command=("/bin/ls", "/tmp/foo%s" % task["frame"]),
        ...             environ={"FOO": "1"},
        ...             usrgrp=("foo", "bar"))

        The above is a generator that will return everything that's needed to
        run the process.  Here's some details on specific attribute you
        can pass into a :class:`ProcessInputs` instance:

            * **task** (required) - The task
        """
        raise NotImplementedError("`build_process_inputs` must be implemented")

    def spawn_process(self, inputs):
        """
        Spawns a process using :func:`.reactor.spawnProcess` and returns
        a deferred object which will fire when t
        """
        assert isinstance(inputs, ProcessInputs)

        # assert `task` is a valid type
        if not isinstance(inputs.task, dict):
            self.set_state(
                inputs.task, WorkState.FAILED,
                "`task` must be a dictionary, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    inputs.task))
            return

        # assert `command` is a valid type
        if not isinstance(inputs.command, (list, tuple)):
            self.set_state(
                inputs.task, WorkState.FAILED,
                "`command` must be a list or tuple, got %s instead.  Check "
                "the output produced by `build_process_inputs`" % type(
                    inputs.command))
            return

        # username/group to uid/gid mapping
        uid, gid = None, None
        if all([pwd is not NotImplemented,
                grp is not NotImplemented,
                inputs.user is not None or inputs.group is not None]):

            try:
                uid, gid = self.usrgrp_to_uidgid(inputs.user, inputs.group)
            except KeyError as e:
                self.set_state(
                    inputs.task, WorkState.FAILED,
                    "Failed to map username/group (%r, %r) to a user id "
                    "and group id: %s" % (inputs.user, inputs.group, e))

        # generate environment and ensure
        if inputs.env is None:
            environment = self.get_default_environment()
        else:
            environment = inputs.env

        if not isinstance(environment, dict):
            self.set_state(
                inputs.task, WorkState.FAILED,
                "`inputs.env` must be a dictionary, got %s instead.  Check "
                "the output produced by "
                "`build_process_inputs`" % type(environment))
            return

        # Prepare the arguments for the spawnProcess call
        protocol = self.build_process_protocol(self, inputs.task)
        kwargs = {"env": None, "args": inputs.command[1:]}

        # update kwargs with uid/gid
        if uid is not None:
            kwargs.update(uid=uid)
        if gid is not None:
            kwargs.update(gid=gid)

        # inputs.chdir may be a file path
        chdir = config["chroot"]
        if isinstance(inputs.chdir, STRING_TYPES):
            # Convert inputs.chdir to a template first so we
            # can resolve any environment variables it may contain
            template = Template(inputs.chdir)
            chdir = template.safe_substitute(**environment)

        if not isdir(chdir):
            self.set_state(
                inputs.task, WorkState.FAILED,
                "Directory provided for `inputs.chdir` does not "
                "exist: %r" % chdir)
            return

        kwargs.update(path=chdir)

        # reactor.spawnProcess does different things with the environment
        # depending on what platform you're on and what you're passing in.
        # To avoid inconsistent behavior, we replace os.environ with
        # our own environment so we can launch the process.  After launching
        # we replace the original environment.
        with ReplaceEnvironment(environment):
            process = reactor.spawnProcess(
                protocol, inputs.command[0], **kwargs)

        return Task(
            protocol=protocol, process=process,
            command=inputs.command[0], kwargs=kwargs)

    def start(self):
        """
        This method is called when the job type should start
        working.  Depending on the job type's implementation this will
        prepare and start one more more processes.
        """
        tasks = []
        for task, command, environment, uidgid in self.build_process_inputs():
            spawned = self.spawn_process(task, command, environment, uidgid)
            if spawned is not None:
                tasks.append(spawned)

        # TODO: verify this does the right thing since `spawned` might not
        # work with this
        return DeferredList(tasks)

    #
    # General method used for internal processing that could
    # be overridden
    #
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

            if state == WorkState.ERROR:
                if isinstance(error, Exception):
                    error = self.format_exception(error)

                logger.error("Task %r failed: %r", task, error)
                running_task = self.running_tasks[task_id]




                # TODO: process_started(task)
                # TODO: process_stopped(task)
                # TODO: stdout_received(task)
                # TODO: stderr_received(task)
                # TODO: post_status_change(task)
