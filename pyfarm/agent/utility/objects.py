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
Objects
-------

General objects within the utility package that don't
fit well into other modules or that serve more than one purpose.
"""


from collections import deque
from functools import partial
from UserDict import IterableUserDict

from twisted.internet import reactor
from twisted.python import log


class LoggingDictionary(IterableUserDict):
    """
    Special configuration object which logs when a key is changed in
    a dictionary.  If the reactor is not running then log messages will
    be queued until they can be emitted so they are not lost.
    """
    queue = deque()

    def __init__(self, dict=None, **kwargs):
        logger_name = kwargs.pop("logger_name", self.__class__.__name__)
        IterableUserDict.__init__(self, dict=dict, **kwargs)
        self.emit = partial(log.msg, system=logger_name)

    def __setitem__(self, key, value):
        # key is not already set or key has changed
        if key not in self.data or key in self.data and self.data[key] != value:
            self.log(key, value)

        IterableUserDict.__setitem__(self, key, value)

    def log(self, k, v):
        """either logs or waits to log depending on the reactor state"""
        if not reactor.running and k is not None:
            self.queue.append((k, v))
        else:
            while self.queue:
                key, value = self.queue.popleft()
                self.emit("%s=%s" % (key, repr(value)))

            if k is not None and v is not None:
                self.emit("%s=%s" % (k, repr(v)))