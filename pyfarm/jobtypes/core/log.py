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
from threading import Lock, RLock
from os import makedirs
from os.path import dirname, isfile

try:
    range_ = xrange
except NameError:  # pragma: no cover
    range_ = range

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
CREATE_LOG_LOG = Lock()

logger = getLogger("jobtypes.log")


def open_log(path, ignore_existing=False):
    """
    Creates a log file on disk as safely as possible and returns
    the file object.  This classmethod is meant to be run from
    within a thread so the disk IO can be placed outside of the
    reactor's event loop.

    :raise OSError:
        Raised if ``path`` already exists or its parent
        directory could not be created for some reason.
    """
    parent_dir = dirname(path)

    with CREATE_LOG_LOG:
        if not ignore_existing and isfile(path):
            raise OSError("Log exists: %r" % path)

        # Create the directory and raise any exception
        # produced (except EEXIST)
        try:
            makedirs(parent_dir)
            logger.debug("Created directory %r", parent_dir)
        except OSError as e:
            if e.errno != EEXIST:
                raise

        logger.debug("Opening log file %r", path)
        return open(path, "w")


class Log(object):
    """
    Internal class which represents a log object and handle
    data that's internal to log handling.
    """
    def __init__(self, fileobj):
        self.lock = RLock()
        self.file = fileobj
        self.messages = deque()
        self.written = 0
        self.csv = UnicodeCSVWriter(self.file)


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
        logger.debug("Created %r", self)
        reactor.addSystemEventTrigger("before", "shutdown", self.stop)

    def defer(self, function, *args, **kwargs):
        """
        Wrapper around :func:`.deferToThreadPool` so we can defer some
        functions to this pool's workers.
        """
        return deferToThreadPool(self.reactor, self, function, *args, **kwargs)

    def open_log(self, protocol, path, ignore_existing=False):
        """
        Opens a log file for the given ``protocol`` object at ``path``.  This
        class method will return a deferred object that will fire its callback
        once the log is open and ready to receive data.
        """
        if protocol.id in self.logs:
            raise KeyError(
                "Protocol %r is already logging to %r" % (
                    protocol.id, self.logs[protocol.id]))

        def log_created(stream, impacted_protocol):
            logger.info(
                "Created log for protocol %r at %r",
                impacted_protocol.id, stream.name)

            self.logs[impacted_protocol.id] = Log(stream)
            return impacted_protocol.id, self.logs[impacted_protocol.id]

        deferred = self.defer(open_log, path, ignore_existing=ignore_existing)
        deferred.addCallback(log_created, protocol)
        return deferred

    def log(self, protocol_id, streamno, message):
        """
        Places a single message to be handled by the worker threads into
        the queue for processing.
        """
        if self.stopped:
            logger.warning(
                "Rejecting log message, pool is stopped or stopping!!")
            return

        # This operation is atomic so we're safe to keep
        log = self.logs[protocol_id]
        log.messages.append((datetime.utcnow(), streamno, message))

        if len(log.messages) > self.max_queued_lines:
            self.callInThread(self.flush, log)

    def flush(self, log):
        """
        Takes the given log object and flushes the messages it
        contains to the attached file object.
        """
        while True:
            # Only one thread at a time may retrieve objects, write
            # to the file, and flush.  This helps to preserve the
            # order of the messages and cuts down on wasted cycles
            # from switching contexts.
            with log.lock:
                try:
                    date, streamno, message = log.messages.popleft()
                except IndexError:
                    break
                else:
                    try:
                        print >> log.file, message

                    except (OSError, IOError) as e:
                        # Put the log message back in the queue
                        # so we're not losing data.  It may be lightly
                        # out of order now but we have a date stamp
                        # and it's more important we don't lose data.
                        log.messages.appendleft((date, streamno, message))
                        logger.error(
                            "Failed to write to %s: %s", log.file.name, e)
                    else:
                        log.written += 1

        # Check if we should flush to disk.  We're doing
        # this outside the above because it ensures we
        # only have to run this logic once instead of
        # once per message.
        with log.lock:
            if log.written >= self.flush_lines:
                try:
                    log.file.flush()
                except (OSError, IOError) as e:
                    logger.error(
                        "Failed to flush output to %s: %s",
                        log.file.name, e)
                else:
                    logger.debug(
                        "%s wrote %s lines to %s",
                        self.currentThread().name, log.written,
                        log.file.name)
                    log.written = 0

        return log

    def close(self, protocol_id):
        """Closes the file handle for the given protocol id."""
        log = self.logs.pop(protocol_id, None)
        if log is not None:
            self.flush(log)
            log.file.close()
            logger.info("Closed %s", log.file.name)

    def stop(self):
        """
        Flushes any remaining data, closes the underlying files, then stops
        the thread pool.

        .. warning::

            Because this method is usually called when the reactor is
            stopping all file handling happens in the main thread.
        """
        logger.info("Logging thread pool is shutting down.")
        self.stopped = True

        for protocol_id in list(self.logs.keys()):
            self.close(protocol_id)

        ThreadPool.stop(self)
