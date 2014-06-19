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

from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol

from pyfarm.core.enums import STRING_TYPES, NUMERIC_TYPES


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
    def __init__(self, jobtype, process_inputs, command, arguments, env,
                 path, uid, gid):
        self.jobtype = jobtype
        self.inputs = process_inputs
        self.command = command
        self.args = arguments
        self.env = env
        self.path = path
        self.uid = uid
        self.gid = gid
        self.id = tuple(task["id"] for task in process_inputs.tasks)

    def __repr__(self):
        return "Process(id=%r, pid=%r, command=%r, args=%r, path=%r, " \
               "uid=%r, gid=%r)" % (
                   self.id, self.pid, self.command, self.args,
                   self.path, self.uid, self.gid)

    @property
    def pid(self):
        return self.transport.pid

    @property
    def process(self):
        return self.transport

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
        self.jobtype.process_stopped(self, reason)

    def outReceived(self, data):
        """Called when the process emits on stdout"""
        self.jobtype.received_stdout(self, data)

    def errReceived(self, data):
        """Called when the process emits on stderr"""
        self.jobtype.received_stderr(self, data)
