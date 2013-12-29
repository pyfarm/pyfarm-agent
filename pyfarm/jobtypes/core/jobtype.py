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

import os
from functools import partial
from string import Template

try:
    import json
except ImportError:
    import simplejson as json

from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol
from twisted.python import log

from pyfarm.core.enums import WorkState
from pyfarm.core.files import which
from pyfarm.jobtypes.core.protocol import ProcessProtocol


class JobType(object):
    """
    Base class for all other job types.  This class is intended
    to abstract away many of the asynchronous necessary to run
    a job type on an agent.
    """
    protocol_class = ProcessProtocol

    def __init__(self, config, assignment_data):
        self.config = config
        self.assignment = assignment_data.copy()
        self.process = None
        self.protocol = None
        self._last_state = None
        self.called = False

        # TODO: replace logger with observer
        # TODO: use observers to prefix certain log messages (ex. stdout)
        self.log = partial(log.msg, system=self.__class__.__name__)

    def __call__(self, *args, **kwargs):
        if self.called:
            raise ValueError("class already called")

        self.called = True
        raise NotImplementedError("not yet implemented")

    @property
    def spawned_process(self):
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
        instance = self.protocol_class(self.config, self)

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
        if not force and self.spawned_process:
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
