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
Log
===

Objects and methods used by the job type for logging.
"""

import threading
from datetime import datetime
from Queue import Queue, Empty

from twisted.internet import reactor

from pyfarm.core.enums import STRING_TYPES
from pyfarm.core.config import read_env_int, read_env_float
from pyfarm.core.logger import getLogger
from pyfarm.agent.utility import UnicodeCSVWriter

STDOUT = 0
STDERR = 1
STREAMS = set([STDOUT, STDERR])

logger = getLogger("jobtype.log")


# TODO: if we get fail the task if we have errors
class LoggingThread(threading.Thread):
    """
    This class runs a thread which writes lines in csv format
    to the log file.
    """
    FLUSH_AFTER_LINES = read_env_int(
        "PYFARM_JOBTYPE_LOGGING_FLUSH_AFTER_LINES", 50)
    QUEUE_GET_TIMEOUT = read_env_float(
        "PYFARM_JOBTYPE_LOGGING_QUEUE_TIMEOUT", .25)

    def __init__(self, filepath):
        super(LoggingThread, self).__init__()
        self.queue = Queue()
        self.stream = open(filepath, "wb")
        self.writer = UnicodeCSVWriter(self.stream)
        self.lineno = 1
        self.next_flush = self.FLUSH_AFTER_LINES
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
        while True:
            # Pull data from the queue or retry again
            try:
                timestamp, streamno, lineno, message = self.queue.get(
                    timeout=self.QUEUE_GET_TIMEOUT)
            except Empty:
                pass
            else:
                # Write data from the queue to a file
                self.writer.writerow(
                    [timestamp, str(streamno), str(lineno), message])
                if self.lineno >= self.next_flush:
                    self.stream.flush()
                    self.next_flush += self.FLUSH_AFTER_LINES

            # We're either being told to stop or we
            # need to run one more iteration of the
            # loop to pickup any straggling messages.
            if self.stopped and stopping:
                logger.debug("Closing %s", self.stream.name)
                self.stream.close()
                break

            # Go around one more time to pickup remaining messages
            elif self.stopped:
                stopping = True