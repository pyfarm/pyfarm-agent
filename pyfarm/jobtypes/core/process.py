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
Process
--------

Module responsible for connecting a Twisted process object and
a job type.  Additionally this module contains other classes which
are useful in starting or managing a process.
"""

import os

from psutil import Process, NoSuchProcess
from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol

from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import uuid

logger = getLogger("jobtypes.process")


class ReplaceEnvironment(object):
    """
    A context manager which will replace ``os.environ``'s, or dictionary of
    your choosing, for a short period of time.  After exiting the
    context manager the original environment will be restored.

    This is useful if you have something like a process that's using
    global environment and you want to ensure that global environment is
    always consistent.

    :param dict environment:
        If provided, use this as the environment dictionary instead
        of ``os.environ``
    """
    def __init__(self, frozen_environment, environment=None):
        if environment is None:
            environment = os.environ

        self.environment = environment
        self.original_environment = None
        self.frozen_environment = frozen_environment

    def __enter__(self):
        self.original_environment = self.environment.copy()
        self.environment.clear()
        self.environment.update(self.frozen_environment)
        return self

    def __exit__(self, *_):
        self.environment.clear()
        self.environment.update(self.original_environment)


class ProcessProtocol(_ProcessProtocol):
    """
    Subclass of :class:`.Protocol` which hooks into the various systems
    necessary to run and manage a process.  More specifically, this helps
    to act as plumbing between the process being run and the job type.
    """
    def __init__(
            self, jobtype, command, arguments, environment, working_dir,
            uid, gid):
        self.jobtype = jobtype
        self.command = command
        self.arguments = arguments
        self.environment = environment
        self.working_dir = working_dir
        self.uid = uid
        self.gid = gid
        self.uuid = uuid()
        self._ended = False

    def __repr__(self):
        return \
            "Process(pid=%r, command=%r, args=%r, working_dir=%r, " \
            "uid=%r, gid=%r)" % (
            self.pid, self.command, self.arguments, self.working_dir,
            self.uid, self.gid)

    @property
    def pid(self):
        return self.transport.pid

    @property
    def process(self):
        """The underlying Twisted process object"""
        return self.transport

    @property
    def psutil_process(self):
        """Returns a :class:`psutil.Process` object for the running process"""
        # It's possible that the process could have
        # ended but not died yet.  So we have this
        # check just in case this property is called
        # during this state.
        if self._ended:
            return None

        try:
            return Process(pid=self.pid)
        except NoSuchProcess:  # pragma: no cover
            return None

    def connectionMade(self):
        """
        Called when the process first starts and the file descriptors
        have opened.
        """
        self.jobtype.process_started(self)

    def processEnded(self, reason):
        """
        Called when the process has terminated and all file descriptors
        have been closed.  :meth:`processExited` is called to however we
        only want to notify the parent job type once the process has freed
        up the last bit of resources.
        """
        self._ended = True
        self.jobtype.process_stopped(self, reason)

    def outReceived(self, data):
        """Called when the process emits on stdout"""
        self.jobtype.received_stdout(self, data)

    def errReceived(self, data):
        """Called when the process emits on stderr"""
        self.jobtype.received_stderr(self, data)

    def kill(self):
        """Kills the underlying process, if running."""
        logger.info("Killing %s", self)
        try:
            self.process.signalProcess("KILL")
        except Exception as e:  # pragma: no cover
            logger.warning("Cannot kill %s: %s.", self, e)

    # NOTE: no covered by tests due to flakyness
    # TODO: debug 'flakyness' and find a better solution
    def terminate(self):  # pragma: no cover
        """Terminates the underlying process, if running."""
        logger.info("Terminating %s", self)
        try:
            self.process.signalProcess("TERM")
        except Exception as e:  # pragma: no cover
            logger.warning("Cannot terminate %s: %s.", self, e)

    def interrupt(self):
        """Interrupts the underlying process, if running."""
        logger.info("Interrupt %s", self)
        try:
            self.process.signalProcess("INT")
        except Exception as e:  # pragma: no cover
            logger.warning("Cannot interrupt %s: %s.", self, e)
