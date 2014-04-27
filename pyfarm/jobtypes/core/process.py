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


class ProcessInputs(object):
    """
    Simple class used to store inputs for :meth:`JobType.spawn_process`.  This
    class does not handle data so refer to the documentation below to
    ensure your job type does not fail.

    :param dict task:
        The task these process inputs correspond to.

    :param list command:
        The list which contains the absolute path to the command to run
        and the arguments as well.

    :param dict env:
        The environment to pass along to the process.  Any value other than
        ``None`` will fully replace the environment.  A value of ``None`` will
        use the job type's ``get_default_environment``.

    :param string chdir:
        The directory the process should run in.  If no directory is provided
        then this will use the agent's ``--chroot`` flag to determine the
        location change into.  If this flag was not present when the agent
        was launched then the current working directory will be used instead.

    :param string user:
        The user name the process should run as when its launched.

        .. note::

            This value is ignored on Windows.  The underlying library used
            to execute processes does not support changing the user which
            will run the process.

    :param string group:
        The group name the process should run as when its launched.

        .. note::

            This value is ignored on Windows.  The underlying library used
            to execute processes does not support changing the group which
            will run the process.

    :param bool expandvars:
        If ``True`` then environment variables in ``command``, ``chdir`` and
        ``group`` will be expanded before the process is launched.

    :raises TypeError:
        Raised if we got an unexpected type for one of the input arguments or
        if we got a type we couldn't convert in :param:`command` to a string.
    """
    def __init__(
            self, task, command, env=None, chdir=None, user=None, group=None,
            expandvars=True):
        if not isinstance(task, dict):
            raise TypeError("Expected a dictionary for `task`")

        if not isinstance(command, (list, tuple)):
            raise TypeError("Expected a list or tuple for `command`")

        if env is not None and not isinstance(env, dict):
            raise TypeError("Expected `env` to be a dictionary")

        if not isinstance(chdir, STRING_TYPES):
            raise TypeError("Expected `chdir` to be a string")

        if user is not None and not isinstance(user, STRING_TYPES):
            raise TypeError("Expected `user` to be a string")

        if group is not None and not isinstance(group, STRING_TYPES):
            raise TypeError("Expected `group` to be a string")

        # Iterate over all entries in `command` and convert any
        # numeric values to a string.  Anything that's neither
        # string or number will raise TypeError because str(value)
        # will likely be wrong.
        for index, value in enumerate(command[:]):
            if isinstance(value, NUMERIC_TYPES):
                command[index] = str(value)

            # This is neither a string or a number, fail because
            # str() is probably going to return the wrong thing.
            elif not isinstance(value, STRING_TYPES):
                raise TypeError(
                    "Expected a string or number of entry command[%s]" % index)

        self.task = task
        self.command = command
        self.env = env
        self.chdir = chdir
        self.user = user
        self.group = group
        self.expandvars = expandvars

    def __repr__(self):
        return "%s(task=%r, command=%r, env=%r, user=%r, group=%r)" % (
            self.__class__.__name__,
            self.task, self.command, self.env, self.user, self.group)


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.environment.clear()
        self.environment.update(self.original_environment)


class ProcessProtocol(_ProcessProtocol):
    """
    Subclass of :class:`.Protocol` which hooks into the various systems
    necessary to run and manage a process.  More specifically, this helps
    to act as plumbing between the process being run and the job type.
    """
    def __init__(self, jobtype, process_inputs, command, arguments, env,
                 chdir, uid, gid, protocol_id=None):
        self.jobtype = jobtype
        self.inputs = process_inputs
        self.command = command
        self.args = arguments
        self.env = env
        self.chdir = chdir
        self.uid = uid
        self.gid = gid

        if protocol_id is None:
            protocol_id = self.inputs.task["id"]

        self.id = protocol_id

    def __repr__(self):
        return "Process(task=%r, pid=%r, command=%r, args=%r, chdir=%r, " \
               "uid=%r, gid=%r)" % (
            self.id, self.pid, self.command, self.args,
            self.chdir, self.uid, self.gid)

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
        self.jobtype._process_stopped(self, reason)

    def outReceived(self, data):
        """Called when the process emits on stdout"""
        self.jobtype.received_stdout(self, data)

    def errReceived(self, data):
        """Called when the process emits on stderr"""
        self.jobtype.received_stderr(self, data)
