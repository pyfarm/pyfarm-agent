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

"""
Job Type Internals
==================

Contains classes which contain internal methods for
the :class:`pyfarm.jobtypes.core.jobtype.JobType` class.
"""

import os
import tempfile
import threading
from datetime import datetime
from os.path import dirname, isdir, join, isfile
from Queue import Queue, Empty
from string import Template

try:
    import pwd
    import grp
except ImportError:  # pragma: no cover
    pwd = NotImplemented
    grp = NotImplemented

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred

from pyfarm.core.enums import INTEGER_TYPES, STRING_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.http.core.client import get, http_retry_delay
from pyfarm.agent.utility import UnicodeCSVWriter
from pyfarm.jobtypes.core.process import ReplaceEnvironment, ProcessProtocol

STDOUT = 0
STDERR = 1
STREAMS = set([STDOUT, STDERR])

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")
logfile = getLogger("jobtypes.log")


# TODO: if we get fail the task if we have errors
class LoggingThread(threading.Thread):
    """
    This class runs a thread which writes lines in csv format
    to the log file.
    """
    def __init__(self, filepath):
        super(LoggingThread, self).__init__()
        self.queue = Queue()
        self.filepath = filepath
        self.lineno = 1
        self.stopped = False
        self.shutdown_event = \
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)

    def put(self, streamno, message):
        """Put a message in the queue for the thread to pickup"""
        assert self.stopped is False, "Cannot put(), thread is stopped"
        assert streamno in STREAMS
        assert isinstance(message, STRING_TYPES)
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


class Cache(object):
    """Internal methods for caching job types"""
    CACHE_DIRECTORY = Template(
        config.get("jobtype_cache_directory", "")).safe_substitute(
        temp=tempfile.gettempdir())

    if not CACHE_DIRECTORY:
        CACHE_DIRECTORY = None
        logger.warning("Job type cache directory has been disabled.")

    elif not isdir(CACHE_DIRECTORY):
        try:
            os.makedirs(CACHE_DIRECTORY)

        except OSError:
            logger.error(
                "Failed to create %r.  Job type caching is "
                "now disabled.", CACHE_DIRECTORY)
            CACHE_DIRECTORY = None

        else:
            logger.info("Created job type cache directory %r", CACHE_DIRECTORY)

    else:
        logger.debug("Job type cache directory is %r", CACHE_DIRECTORY)

    @classmethod
    def _download_jobtype(cls, name, version):
        """
        Downloads the job type specified in ``assignment``.  This
        method will pass the response it receives to :meth:`_cache_jobtype`
        however failures will be retried.
        """
        url = config["master-api"] + "/jobtypes/" + name + "/" + str(version)
        result = Deferred()
        download = lambda *_: \
            get(url,
                callback=result.callback,
                errback=lambda: reactor.callLater(http_retry_delay(), download))
        download()
        return result

    @classmethod
    def _cache_jobtype(cls, cache_key, jobtype):
        """
        Once the job type is downloaded this classmethod is called
        to store it on disk.  In the rare even that we fail to write it
        to disk, we store it in memory instead.
        """
        filename = str(join(
            cls.CACHE_DIRECTORY,
            "_".join(map(str, cache_key)) + "_" + jobtype["classname"] + ".py"))
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


class Process(object):
    """Methods related to process control and management"""
    logging = {}

    def _start(self):
        return self.start()

    def _stop(self):
        return self.stop()

    def _start_logging(self, protocol, log):
        assert isinstance(protocol, ProcessProtocol)
        assert isinstance(log, STRING_TYPES)
        thread = self.logging[protocol.id] = LoggingThread(logfile)
        thread.start()
        return thread

    def _spawn_process(self, environment, protocol, command, kwargs):
        assert isinstance(environment, dict)
        assert isinstance(protocol, ProcessProtocol)
        assert isinstance(command, STRING_TYPES)
        assert isinstance(kwargs, dict)

        # reactor.spawnProcess does different things with the environment
        # depending on what platform you're on and what you're passing in.
        # To avoid inconsistent behavior, we replace os.environ with
        # our own environment so we can launch the process.  After launching
        # we replace the original environment.
        with ReplaceEnvironment(environment):
            return reactor.spawnProcess(protocol, command, **kwargs)

    def _log_in_thread(self, protocol, stream_type, data):
        """
        Internal implementation called several methods including
        :meth:`_received_stdout`, :meth:`_received_stderr`,
        :meth:`_process_started` and others.

        This method takes the incoming protocol object and retrieves the thread
        which is handling logging for a given process.  Each message will then
        be queued and written to disk at the next opportunity.
        """
        self.logging[protocol.id].put(stream_type, data)

    def _get_uidgid(self, value, value_name, func_name, module, module_name):
        """
        Internal function which handles both user name and group conversion.
        """
        # This platform does not implement the module
        if module is NotImplemented:
            logger.warning(
                "This platform does not implement the %r module, skipping "
                "%s()", module_name, func_name)

        # Convert a user/group string to an integer
        elif isinstance(value, STRING_TYPES):
            try:
                if module_name == "pwd":
                    return pwd.getpwnam(value).pw_uid
                elif module_name == "grp":
                    return grp.getgrnam(value).gr_gid
                else:
                    raise ValueError(
                        "Internal error, failed to get module to use for "
                        "conversion.  Was given %r" % module)
            except KeyError:
                logger.error(
                    "Failed to convert %s to a %s",
                    value, func_name.split("_")[1])

                if not config.get("jobtype_ignore_id_mapping_errors"):
                    raise

        # Verify that the provided user/group string is real
        elif isinstance(value, INTEGER_TYPES):
            try:
                if module_name == "pwd":
                    pass
                elif module_name == "grp":
                    pass
                else:
                    raise ValueError(
                        "Internal error, failed to get module to use for "
                        "conversion.  Was given %r" % module)

                # Seems to check out, return the original value
                return value
            except KeyError:
                logger.error(
                    "%s %s does not seem to exist", value_name, value)

                if not config.get("jobtype_ignore_id_mapping_errors"):
                    raise
        else:
            raise ValueError(
                "Expected an integer or string for `%s`" % value_name)