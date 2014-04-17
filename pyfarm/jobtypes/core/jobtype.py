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
import os
import json
import tempfile
import sys
from string import Template
from os.path import join, dirname, isfile

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred
from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol

from pyfarm.core.enums import WorkState
from pyfarm.core.files import which
from pyfarm.core.config import read_env
from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.jobtypes.core.protocol import ProcessProtocol

DEFAULT_CACHE_DIRECTORY = read_env(
    "PYFARM_JOBTYPE_CACHE_DIRECTORY", ".jobtypes")

logcache = getLogger("jobtypes.cache")


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
        self.assignment = assignment

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

        # TODO: if the file is cached on disk, and a command line flag (which
        # does not exist yet) says to use disk cache, do so instead of the below
        # If the job type is not cached, we have to download it
        if cache_key not in cls.cache:
            def download_complete(response):
                if response.code != OK:
                    return response.request.retry()

                # When the download is complete, cache the results
                caching = cls._cache_jobtype(cache_key, response.json())
                caching.addCallback(load_jobtype)

            # Start the download
            download = cls._download_jobtype(assignment)
            download.addCallback(download_complete)
        else:
            load_jobtype((cls.cache[cache_key]))

        return result

    @property
    def running(self):
        """property which returns True if the process has been spawned"""
        if self.process and self.protocol:
            return True
        elif not self.process and not self.protocol:
            return False
        else:
            raise ValueError(
                "`process` and `protocol` must both be True or False")

    def task_state_changed(self, new_state):
        """
        Callback used whenever the state of the task being run is
        changed.
        """
        if new_state == self._last_state:
            return

        # validate the state name
        for state_name, value in WorkState._asdict().iteritems():
            if value == new_state:
                args = (self.assignment["task"], state_name)
                self.log("tasks %s's state is changing to %s" % args)
                break
        else:
            raise ValueError("no such state %s" % new_state)


        # prepare a retry request to POST to the master
        retry_post_agent = RetryDeferred(
            http_post, self._startServiceCallback, self._failureCallback,
            headers={"Content-Type": "application/json"},
            max_retries=self.config["http-max-retries"],
            timeout=None,
            retry_delay=self.config["http-retry-delay"])

        retry_post_agent(
            self.config["http-api"] + "/agents",
            data=json.dumps(get_agent_data()))

        self._last_reported_state = new_state

    def get_process_protocol(self):
        """
        Returns the process protocol object used to spawn connect
        a job type to a running process.  By default this will instance
        and return :cvar:`.protocol`.

        :raises TypeError:
            raised if the protocol object is not a subclass of
            :class:`._ProcessProtocol`
        """
        instance = self.protocol(self.config, self)

        if not isinstance(instance, _ProcessProtocol):
            raise TypeError("expected an instance of Twisted's ProcessProtocol")

        return instance

    def get_environment(self):
        """
        Constructs and returns the environment dictionary.  By default this
        takes the current environment and updates it with the environment
        found in the assignment data.
        """
        return dict(os.environ.items() + self.assignment.get("env", {}).items())

    def get_command(self):
        """
        Returns the command to pass along to :func:`.reactor.spawnProcess`.
        This method will expand the user variable (~) and any environment
        variables in the command.

        :raises OSError:
            raised when the given command couldn't be expanded using
            environment variables into a path or when we cannot find the
            command by name on $PATH
        """
        # construct a template and attempt to sub in values from
        # the environment and user (~)
        template = Template(self.assignment["jobtype"]["command"])
        command = os.path.expanduser(
            template.safe_substitute(self.get_environment()))

        if os.path.isfile(command):
            return command
        else:
            return which(command)

    def get_arguments(self):
        """
        Returns the arguments to pass into :func:`.reactor.spawnProcess`.

        .. note::
            this method is generally overridden in a subclass so things
            like frame number or processor could can be pulled from the
            assignment data
        """
        return " ".join(self.assignment["jobtype"]["args"])

    def spawn_process(self, force=False):
        """
        Spawns a process using :func:`.reactor.spawnProcess` after instancing
        the process protocol from :meth:`get_process_protocol`.  Calling this
        method will populate the ``process`` and ``protocol`` instance
        variables.

        :param bool force:
            If True, disregard if the process is already running.  This should
            be used with great care because it does not try to stop or cleanup
            any existing processes.

        :raises ValueError:
            raised if the process has already been spawned
        """
        if not force and self.running:
            raise ValueError("process has already been spawned")

        try:
            command = self.get_command()
        except OSError:
            self.state_changed(WorkState.NO_SUCH_COMMAND)
            return



        # prepare command and arguments
        command = self.assignment["jobtype"]["args"]
        arguments = self.assignment["jobtype"]["args"]
        if arguments[0] != command:
            arguments.insert(0, command)

        # launch process
        self.protocol = self.get_process_protocol()
        self.process = reactor.spawnProcess(self.protocol, command, arguments)

    def process_started(self):
        """Callback run once the process has started"""
        print "started"

    def process_stopped(self, exit_code):
        print "stopped", exit_code


    # TODO: [GENERAL] - this operates on the agent, abstract away as much of the async work as possible
    # TODO: get_process_protocol() - by default uses self._process_protocol
    # TODO: spawn_process()
    # TODO: process_started()
    # TODO: process_stopped()
    # TODO: stdout_received()
    # TODO: stderr_received()
    # TODO: post_status_change()

if __name__ == "__main__":
    assignment = {
        "jobtype": {
            "cmd": "ping",
            "args": ["-c", "5", "127.0.0.1"]
        }
    }
    jobtype = JobType({}, assignment)
    jobtype.spawn_process()
