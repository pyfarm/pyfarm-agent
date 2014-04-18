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
from os.path import join, dirname, isfile
from collections import namedtuple

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred, DeferredList

from pyfarm.core.enums import WorkState
from pyfarm.core.files import which
from pyfarm.core.config import read_env
from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.jobtypes.core.protocol import ProcessProtocol

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
Process = namedtuple("Process", ("process", "protocol"))

# Construct the base environment that all job types will use.  We do this
# once per process so a job type can't modify the running environment
# on purpose or by accident.
BASE_ENVIRONMENT_CONFIG = read_env("PYFARM_JOBTYPE_BASE_ENVIRONMENT", "")
if isfile(BASE_ENVIRONMENT_CONFIG):
    logger.info(
        "Attempting to load base environment from %s", BASE_ENVIRONMENT_CONFIG)
    with open(BASE_ENVIRONMENT_CONFIG, "rb") as stream:
        BASE_ENVIRONMENT = json.load(stream)
    assert isinstance(BASE_ENVIRONMENT, dict)

else:
    BASE_ENVIRONMENT = os.environ.copy()


DEFAULT_CACHE_DIRECTORY = read_env(
    "PYFARM_JOBTYPE_CACHE_DIRECTORY", ".jobtypes")


class JobType(object):
    """
    Base class for all other job types.  This class is intended
    to abstract away many of the asynchronous necessary to run
    a job type on an agent.

    :attribute cache:
        Stores the cached job types

    :attribute process_protocol:
        The protocol object used to communicate between the process
        and the job type.
    """
    process_protocol = ProcessProtocol
    cache = {}

    def __init__(self, assignment):
        assert isinstance(assignment, dict)
        self._processes = []
        self.assignment = assignment

        logger.debug("Instanced %r", self)

    def __repr__(self):
        return "JobType(job=%r, tasks=%r, jobtype=%r, version=%r, title=%r)" % (
            self.assignment["job"]["id"],
            tuple(task["id"] for task in self.assignment["tasks"]),
            str(self.assignment["jobtype"]["name"]),
            self.assignment["jobtype"]["version"],
            str(self.assignment["job"]["title"]))

    @property
    def processes(self):
        """Provides access to the ``processes`` attribute"""
        # Provided as a property so you can't accidentally
        # do self.processes = []
        return self._processes

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
    # Functions for setting up the job type
    #

    def build_process_protocol(self, task):
        """
        Returns the process protocol object used to spawn connect
        a job type to a running process.  By default this will instance
        and return :cvar:`.protocol`.

        :raises TypeError:
            raised if the protocol object is not a subclass of
            :class:`._ProcessProtocol`
        """
        instance = self.process_protocol(self, task)

        if not isinstance(instance, ProcessProtocol):
            raise TypeError(
                "Expected of pyfarm.jobtypes.core.protocol.ProcessProtocol")

        return instance

    def get_default_environment(self):
        """
        Constructs a reasonable default environment dictionary.  This method
        is not directly used by the core job type class but could be used
        within subclasses.
        """
        return dict(
            list(BASE_ENVIRONMENT.items()) +
            list(self.assignment["job"].get("environ", {}).items()))

    def get_id_for_user(self, username):
        # TODO: docs including Linux vs. Windows and how we try to
        # find the id
        raise NotImplementedError("TODO: implementation of get_id_for_user")

    def build_commands(self):
        """
        Returns the command to pass along to :func:`.reactor.spawnProcess`.

        .. note::

            This method must be overridden in your job type and it should
            return of lists.  Each list entry should be a list with exactly
            two items containing:

                #. The absolute path to the command.  This is a requirement
                   by one of underlying library used to run the command.
                   You may find :func:`os.path.expandvars` or
                   :func:`pyfarm.core.files.which` useful to achieve this.
                #. The arguments the command should execute.

            It is not required to return multiple commands
        """
        raise NotImplementedError("You must implement `build_command`")

    def build_process_inputs(self):
        """
        This method constructs and returns all the arguments necessary
        to execute one or multiple processes on the system.  This method
        must return a structured list similar to this:

        >>> [(("/bin/ls", "-l", "path1"), os.environ.copy(), "someuser")]

        The above is a list inside of a list, this will cause a single
        process to execute on the agent.  To execute multiple processes
        just add another entry to the list.

        Each entry within the list is three parts:

            #. The absolute path to the command and any command line
               arguments to include.  You may find :func:`os.path.expandvars`
               and :func:`pyfarm.core.files.which` helpful in building the
               absolute path
            #  The environment in which the process should run.  You can either
               use :meth:`get_default_environment` to do this or build your
               own.  Additionally you may want to read the documentation
               on some of the special values for **env**:
                http://twistedmatrix.com/documents/current/api/twisted.internet.interfaces.IReactorProcess.spawnProcess.html
            #. The user the job should execute as.  On Windows this value
               is ignored due to combination of API differences and the
               lack of support from the underlying library.  On Linux the agent
               will try to map the user to a user id, you can use
               :meth:`get_id_for_user` to do this.
        """
        raise NotImplementedError(
            "You must implement `build_process_inputs`")

    def spawn_process(self, command, environment, user):
        """
        Spawns a process using :func:`.reactor.spawnProcess` and returns
        a deferred object which will fire when t
        """
        arguments = command[1:]
        command = command[0]
        protocol = self.build_process_protocol()
        # TODO: create deferred to connect to the protocol's 'finished' signal

    def start(self):
        """This method is called when the job type should start working"""
        # TODO: catch and and properly handle exceptions these could throw
        started = []
        for command, environment, user in self.build_process_inputs():
            deferred = self.spawn_process(command, environment, user)
            started.append(deferred)
        return DeferredList(started)

    # TODO: process_started(task)
    # TODO: process_stopped(task)
    # TODO: stdout_received(task)
    # TODO: stderr_received(task)
    # TODO: post_status_change(task)

if __name__ == "__main__":
    assignment = {
        u'job': {
            u'ram_max': 270, u'title':
            u'test job', u'ram': 256, u'cpus': 1, u'by': 1,
            u'environ':
                {u'POST_ASSIGN_TEST': u'true'},
            u'user': u'foo', u'batch': 1, u'data': {
            u'int': 1, u'bool': True, u'str': u'hello world'},
            u'id': 0, u'ram_warning': 260},
        u'tasks': [
            {u'frame': 1, u'id': 1},
            {u'frame': 2, u'id': 2},
            {u'frame': 3, u'id': 3}],
        u'jobtype':
            {u'version': 1, u'name': u'TestJobType'}}
    jobtype = JobType(assignment)
    jobtype.spawn_process()
