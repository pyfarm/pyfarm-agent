# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque
from datetime import datetime
from errno import EEXIST
from threading import RLock
from os import makedirs
from os.path import dirname, isfile

try:
    range_ = xrange
except NameError:  # pragma: no cover
    range_ = range

try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

from twisted.internet import reactor
from twisted.internet.threads import deferToThreadPool
from twisted.python.threadpool import ThreadPool

from pyfarm.agent.config import config
from pyfarm.agent.sysinfo import cpu
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import UnicodeCSVWriter

STDOUT = 0
STDERR = 1
STREAMS = set([STDOUT, STDERR])

logger = getLogger("jobtypes.log")


class CSVLog(object):
    """
    Internal class which represents a log object and handle
    data that's internal to log handling.
    """
    def __init__(self, fileobj):
        # Check the type explicit here because UnicodeCSVWriter will
        # happy accept a non-file object the fail later.
        if not isinstance(fileobj, file):
            raise TypeError("`fileobj` should be a file")

        self.lock = RLock()
        self.file = fileobj
        self.messages = deque()
        self.lines = 0
        self.csv = UnicodeCSVWriter(self.file)

    def write(self, data):
        """Writes the given data to the underlying csv object."""
        date, streamno, lineno, pid, message = data
        self.csv.writerow(
            [date.isoformat(), str(streamno), str(lineno), str(pid), message])


class LoggerPool(ThreadPool):
    """
    This is the thread pool which will handle create of log file
    and storage of messages from processes.  Configuration of
    some of the internals of this class can accomplished using the
    ``jobtype_logging_threadpool`` config value in ``jobtypes.yml``.
    """
    reactor = reactor
    logs = {}

    def __init__(self):
        minthreads = config["jobtype_logging_threadpool"]["min_threads"]
        maxthreads = config["jobtype_logging_threadpool"]["max_threads"]
        self.max_queued_lines = \
            config["jobtype_logging_threadpool"]["max_queue_size"]
        self.flush_lines = \
            config["jobtype_logging_threadpool"]["flush_lines"]
        self.stopped = False

        if minthreads < 1:
            raise ValueError(
                "Config value "
                "jobtype_logging_threadpool.min_threads must be >= 1")

        # Calculate maxthreads if a value was not provided for us
        if maxthreads == "auto":
            auto_maxthreads = min(int(cpu.total_cpus() * 1.5), 20)
            maxthreads = max(auto_maxthreads, minthreads)

        if minthreads > maxthreads:
            raise ValueError(
                "Config value jobtype_logging_threadpool.min_threads cannot "
                "be larger than jobtype_logging_threadpool.max_threads")

        ThreadPool.__init__(
            self,
            minthreads=minthreads, maxthreads=maxthreads,
            name=self.__class__.__name__)

    def defer(self, function, *args, **kwargs):  # pragma: no cover
        """
        Wrapper around :func:`.deferToThreadPool` so we can defer some
        functions to this pool's workers.
        """
        return deferToThreadPool(self.reactor, self, function, *args, **kwargs)

    def open_log(self, uuid, path, ignore_existing=False):
        """
        Opens a log file for the given assignment given by ``uuid`` at ``path``.
        """
        if uuid in self.logs:
            raise KeyError("Log for uuid %s is already logging to %r" %
                           (uuid, self.logs[uuid]))

        if not ignore_existing and isfile(path):
            raise OSError("Log exists: %r" % path)

        # Create the directory and raise any exception
        # produced (except EEXIST)
        parent_dir = dirname(path)
        try:
            makedirs(parent_dir)
            logger.debug("Created directory %r", parent_dir)
        except (OSError, WindowsError) as e:  # pragma: no cover
            if e.errno != EEXIST:
                raise

        logger.debug("Opening log file %r", path)
        file = open(path, "wb")
        logger.info("Created log for assignment %s at %r", uuid, path)
        self.logs[uuid] = CSVLog(file)

    def close_log(self, protocol_uuid):
        """Closes the file handle for the given protocol id."""
        log = self.logs.pop(protocol_uuid, None)
        if log is not None:
            self.flush(log)
            log.file.close()
            logger.debug("Closed %s", log.file.name)

    def log(self, uuid, stream, message, pid=None):
        """
        Places a single message to be handled by the worker threads into
        the queue for processing.
        """
        if self.stopped:
            logger.warning(
                "Rejecting log message, pool is stopped or stopping!!")
            return

        # This operation is atomic so we're safe to keep
        try:
            log = self.logs[uuid]
        except KeyError:
            raise KeyError("No such uuid %s, was open_log() called?" % uuid)

        log.lines += 1
        log.messages.append((datetime.utcnow(), str(pid) or "", stream,
                             log.lines, message))

        if len(log.messages) > self.max_queued_lines:
            self.callInThread(self.flush, log)

    def flush(self, log):
        """
        Takes the given log object and flushes the messages it
        contains to the attached file object.
        """
        # Only one thread at a time may retrieve objects, write
        # to the file, and flush.  This helps to preserve the
        # order of the messages and cuts down on wasted cycles
        # from switching contexts.
        with log.lock:
            processed = False

            # Write all messages in the queue with a single lock.  If
            # we run out of messages the loop stops and waits for the
            # next flush() call.
            while log.messages:
                data = log.messages.popleft()

                try:
                    log.write(data)

                # pragma: no cover
                except (OSError, IOError, WindowsError) as e:
                    # Put the log message back in the queue
                    # so we're not losing data.  It may be lightly
                    # out of order now but we have a date stamp
                    # and it's more important we don't lose data.
                    log.messages.appendleft(data)
                    logger.error(
                        "Failed to write to %s: %s", log.file.name, e)
                else:
                    processed = True

            # Flush to disk but only if we processed some messages.
            if processed:
                try:
                    log.file.flush()

                # pragma: no cover
                except (OSError, IOError, WindowsError) as e:
                    logger.error(
                        "Failed to flush output to %s: %s",
                        log.file.name, e)

    def stop(self):
        """
        Flushes any remaining data, closes the underlying files, then stops
        the thread pool.

        .. warning::

            Because this method is usually called when the reactor is
            stopping all file handling happens in the main thread.
        """
        if not self.started or self.joined:
            return

        logger.debug("Logging thread pool is shutting down.")
        self.stopped = True

        for protocol_id in list(self.logs.keys()):
            self.close_log(protocol_id)

        ThreadPool.stop(self)

    def start(self):
        """
        Starts the logger pool and sets up the required shutdown event
        trigger which will call :meth:`stop` on exit.
        """
        reactor.addSystemEventTrigger("before", "shutdown", self.stop)
        ThreadPool.start(self)
        logger.debug(
            "Started logger thread pool (min=%s, max=%s)", self.min, self.max)

try:
    logpool
    logger.warning(  # pragma: no cover
        "A logger pool was already setup, seems like the log module has "
        "been imported more than once.")
except NameError:
    logpool = LoggerPool()

    # Required so we only start the logpool if the
    # reactor is going to run.  If we don't do this
    # other things will hang.
    reactor.callWhenRunning(logpool.start)
