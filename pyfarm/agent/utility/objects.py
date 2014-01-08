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

import logging
from collections import deque
from functools import partial

try:
    from UserDict import IterableUserDict, UserDict
except ImportError:
    from collections import IterableUserDict, UserDict

from twisted.internet import reactor
from twisted.python import log


class LoggingConfiguration(IterableUserDict):
    """
    Special configuration object which logs when a key is changed in
    a dictionary.  If the reactor is not running then log messages will
    be queued until they can be emitted so they are not lost.
    """
    MODIFIED = "modified"
    CREATED = "created"
    DELETED = "deleted"
    log_queue = deque()
    reactor = reactor
    _log = partial(log.msg, system="config", level=logging.INFO)

    def __setitem__(self, key, value):
        if key not in self:
            self.changed(self.CREATED, key, value)
        elif self[key] != value:
            self.changed(self.MODIFIED, key, value)

        IterableUserDict.__setitem__(self, key, value)

    def __delitem__(self, key):
        IterableUserDict.__delitem__(self, key)
        self.changed(self.DELETED, key, None)

    def pop(self, key, *args):
        IterableUserDict.pop(self, key, *args)
        self.changed(self.DELETED, key, None)

    def clear(self):
        keys = self.keys()
        IterableUserDict.clear(self)

        for key in keys:
            self.changed(self.DELETED, key, None)

    def update(self, data=None, **kwargs):
        if isinstance(data, (dict, UserDict)):
            for key, value in data.items():
                if key not in self:
                    self.changed(self.CREATED, key, value)
                elif self[key] != value:
                    self.changed(self.MODIFIED, key, value)

        for key, value in kwargs.iteritems():
            if key not in self:
                self.changed(self.CREATED, key, value)
            elif self[key] != value:
                self.changed(self.MODIFIED, key, value)

        IterableUserDict.update(self, dict=data, **kwargs)

    @classmethod
    def log(cls, *args, **kwargs):
        if not cls.reactor.running:
            cls.log_queue.append((args, kwargs))
        else:
            while cls.log_queue:
                args, kwargs = cls.log_queue.popleft()
                cls._log(*args, **kwargs)

            cls._log(*args, **kwargs)

    def changed(self, change_type, key, value):
        key = repr(key)
        value = repr(value)

        if change_type == self.MODIFIED:
            self.log("modified %s = %s" % (key, value))

        elif change_type == self.CREATED:
            self.log("set %s = %s" % (key, value))

        elif change_type == self.DELETED:
            self.log("deleted %s" % key)


class ConfigurationWithCallbacks(LoggingConfiguration):
    """
    Subclass of :class:`.LoggingDictionary` that provides the ability to
    run a function when a value is changed.
    """
    callbacks = {}

    @classmethod
    def register_callback(cls, key, callback, append=False):
        """
        Register a function as a callback for ``key``.  When ``key``
        is set the given ``callback`` will be run by :meth:`.changed`

        :param string key:
            the key which when changed in any way will execute
            ``callback``

        :param callable callback:
            the function or method to register

        :param boolean append:
            by default attempting to register a callback which has
            already been registered will do nothing, setting this
            to ``True`` overrides this behavior.
        """
        assert callable(callback)
        callbacks = cls.callbacks.setdefault(key, [])

        if callback in callbacks and not append:
            cls.log(
                "%s is already a registered callback for %s" % (callback, key),
                level=logging.WARNING)
        else:
            callbacks.append(callback)
            cls.log("registered callback %s for %s" % (callback, key),
                    level=logging.DEBUG)

    @classmethod
    def deregister_callback(cls, key, callback):
        if key in cls.callbacks and callback in cls.callbacks[key]:
            # remove all instances of the callback
            while callback in cls.callbacks[key]:
                 cls.callbacks[key].remove(callback)

            # if callbacks no longer exist, remove the key
            if not cls.callbacks[key]:
                cls.callbacks.pop(key)
        else:
            cls.log(
                "%s is not a registered callback for %s" % (callback, key),
                level=logging.WARNING)

    def changed(self, change_type, key, value):
        LoggingConfiguration.changed(self, change_type, key, value)

        if key in self.callbacks:
            for callback in self.callbacks[key]:
                callback(change_type, key, value, self.reactor.running)
                self.log(
                    "key %s was %s, calling callback %s" % (
                        repr(key), change_type, callback),
                    level=logging.DEBUG)
