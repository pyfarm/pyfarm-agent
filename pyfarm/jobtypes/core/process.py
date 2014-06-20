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
from threading import Thread
from datetime import datetime
from Queue import Empty, Queue

from psutil import Process, NoSuchProcess
from twisted.internet import reactor
from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import uuid, UnicodeCSVWriter
from pyfarm.jobtypes.core.internals import STREAMS

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
        self.running = False

    def __repr__(self):
        return \
            "Process(pid=%r, command=%r, args=%r, working_dir=%r, " \
            "uid=%r, gid=%r)" % (
            self.pid, self.command, self.args, self.working_dir,
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
        try:
            return Process(pid=self.pid)
        except NoSuchProcess:
            return None

    def connectionMade(self):
        """
        Called when the process first starts and the file descriptors
        have opened.
        """
        self.running = True
        self.jobtype.process_started(self)

    def processEnded(self, reason):
        """
        Called when the process has terminated and all file descriptors
        have been closed.  :meth:`processExited` is called to however we
        only want to notify the parent job type once the process has freed
        up the last bit of resources.
        """
        self.running = False
        self.jobtype.process_stopped(self, reason)

    def outReceived(self, data):
        """Called when the process emits on stdout"""
        self.jobtype.received_stdout(self, data)

    def errReceived(self, data):
        """Called when the process emits on stderr"""
        self.jobtype.received_stderr(self, data)

    def kill(self):
        """Kills the underlying process, if running."""
        if self.running:
            logger.info("Killing %s", self)
            self.process.signalProcess("KILL")
        else:
            logger.warning("Cannot kill %s, it's not running.", self)

    def terminate(self):
        """Terminates the underlying process, if running."""
        if self.running:
            logger.info("Terminating %s", self)
            self.process.signalProcess("TERM")
        else:
            logger.warning("Cannot terminate %s, it's not running.", self)

    def interrupt(self):
        """Interrupts the underlying process, if running."""
        if self.running:
            logger.info("Interrupt %s", self)
            self.process.signalProcess("INT")
        else:
            logger.warning("Cannot interrupt %s, it's not running.", self)



# TODO: if we get fail the task if we have errors
class LoggingThread(Thread):
    """
    This class runs a thread which writes lines in csv format
    to the log file.
    """
    def __init__(self, log_path):
        super(LoggingThread, self).__init__()
        self.queue = Queue()
        self.filepath = log_path
        self.lineno = 1
        self.stopped = False
        self.shutdown_event = \
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)

    def put(self, streamno, message):
        """Put a message in the queue for the thread to pickup"""
        assert streamno in STREAMS

        if self.stopped:
            raise RuntimeError("Cannot put(), thread is stopped")

        if not isinstance(message, STRING_TYPES):
            raise TypeError("Expected string for `message`")

        now = datetime.utcnow()
        self.queue.put_nowait(
            (now.isoformat(), streamno, self.lineno, message))
        self.lineno += 1

    def stop(self):
        self.stopped = True
        reactor.removeSystemEventTrigger(self.shutdown_event)

    def run(self):
        stopping = False
        next_flush = config.get("jobtype_log_flush_after_lines")
        stream = open(self.filepath, "w")
        writer = UnicodeCSVWriter(stream)
        while True:
            # Pull data from the queue or retry again
            try:
                timestamp, streamno, lineno, message = \
                    self.queue.get(
                        timeout=config.get("jobtype_log_queue_timeout"))
            except Empty:
                pass
            else:
                # Write data from the queue to a file
                writer.writerow(
                    [timestamp, str(streamno), str(lineno), message])
                if self.lineno >= next_flush:
                    stream.flush()
                    next_flush += config.get("jobtype_log_flush_after_lines")

            # We're either being told to stop or we
            # need to run one more iteration of the
            # loop to pickup any straggling messages.
            if self.stopped and stopping:
                logger.debug("Closing %s", stream.name)
                stream.close()
                break

            # Go around one more time to pickup remaining messages
            elif self.stopped:
                stopping = True
