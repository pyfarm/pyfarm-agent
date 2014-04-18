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
Protocol
--------

Module responsible for connecting a Twisted process
object and a job type.
"""

from twisted.internet.protocol import ProcessProtocol as _ProcessProtocol


class ProcessProtocol(_ProcessProtocol):
    """
    Subclass of :class:`.Protocol` which hooks into the various systems
    necessary to run and manage a process.  More specifically, this helps
    to act as plumbing between the process being run and the job type.
    """
    def __init__(self, jobtype, task):
        self.jobtype = jobtype
        self.task = task
        # TODO: pull in settings specific to the process
        # TODO: register a manager so we can send events up to a central class

    def connectionMade(self):
        self.jobtype.process_started(self.task)

    def processEnded(self, reason):
        # TODO: **below should all be handled by the jobtype**
        # TODO: post state change to master
        # TODO: shutdown logger(s) and optionally gzip any files on disk
        self.jobtype.process_stopped(reason.value.exitCode)

    def outReceived(self, data):
        # TODO: **below should all be handled by the jobtype**
        # TODO: emit log message (logstash handler too?)
        # TODO: set the stream id using logger.LOGSTREAM
        pass

    def errReceived(self, data):
        if self.combined_output_streams:
            self.outReceived(data)
        else:
            # TODO: **below should all be handled by the jobtype**
            # TODO: emit log message (logstash handler too?)
            # TODO: set the stream id using logger.LOGSTREAM
            pass
