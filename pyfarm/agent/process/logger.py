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
Logger
------


"""

import tempfile
from collections import namedtuple

from twisted.python.log import LogPublisher, FileLogObserver

_LOGSTREAM = namedtuple("LOGSTREAM", ["STDOUT", "STDERR"])
LOGSTREAM = _LOGSTREAM(STDOUT=object(), STDERR=object())


class FileHandler(FileLogObserver):
    # TODO: change format to be something more easily parsed
    # TODO: include the log stream ud in the file
    # TODO: remove newlines from each incoming message
    pass


class ProcessLogger(LogPublisher):
    def __init__(self, config, pid):
        LogPublisher.__init__(self)
        self.config = config
        self.pid = pid
        self.fd, self.filepath = tempfile.mkstemp(
            prefix="pyfarm-process-log-%s" % pid,
            suffix=".log")
        self.addObserver(FileHandler(self.filepath))

    def msg(self, *message, **kwargs):
        kwargs.setdefault("pid", self.pid)
        kwargs.setdefault("config", self.config)
        kwargs.setdefault("stream", LOGSTREAM.STDOUT)
        LogPublisher.msg(self, *message, **kwargs)
