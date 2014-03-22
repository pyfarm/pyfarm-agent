# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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
Configuration
-------------

Central module for storing and working with a live configuration object.  This
module instances :class:`.ConfigurationWithCallbacks` onto :const:`.config`.
Attempting to reload this module will not reinstance the :const:`.config`
object.

The :const:`.config` object should be directly imported from this
module to be used:

    >>> from pyfarm.agent.config import config
"""

try:
    from UserDict import IterableUserDict, UserDict
except ImportError:  # pragma: no cover
    from collections import IterableUserDict, UserDict

from pyfarm.core.enums import NOTSET
from pyfarm.core.logger import getLogger

logger= getLogger("agent.config")

class LoggingConfiguration(IterableUserDict):
    """
    Special configuration object which logs when a key is changed in
    a dictionary.  If the reactor is not running then log messages will
    be queued until they can be emitted so they are not lost.
    """
    MODIFIED = "modified"
    CREATED = "created"
    DELETED = "deleted"

    def __setitem__(self, key, value):
        if key not in self:
            self.changed(self.CREATED, key, value)
        elif self[key] != value:
            self.changed(self.MODIFIED, key, value)

        IterableUserDict.__setitem__(self, key, value)

    def __delitem__(self, key):
        IterableUserDict.__delitem__(self, key)
        self.changed(self.DELETED, key)

    def pop(self, key, *args):
        IterableUserDict.pop(self, key, *args)
        self.changed(self.DELETED, key)

    def clear(self):
        keys = self.keys()
        IterableUserDict.clear(self)

        for key in keys:
            self.changed(self.DELETED, key)

    def update(self, data=None, **kwargs):
        if isinstance(data, (dict, UserDict)):
            for key, value in data.items():
                if key not in self:
                    self.changed(self.CREATED, key, value)
                elif self[key] != value:  # pragma: no cover
                    self.changed(self.MODIFIED, key, value)

        for key, value in kwargs.iteritems():
            if key not in self:
                self.changed(self.CREATED, key, value)
            elif self[key] != value:  # pragma: no cover
                self.changed(self.MODIFIED, key, value)

        IterableUserDict.update(self, dict=data, **kwargs)

    def changed(self, change_type, key, value=NOTSET):
        assert value is not NOTSET if change_type != self.DELETED else True

        if change_type == self.MODIFIED:
            logger.info("modified %r = %r", key, value)

        elif change_type == self.CREATED:
            logger.info("set %r = %r", key, value)

        elif change_type == self.DELETED:
            logger.warning("deleted %r", key)

        else:
            raise NotImplementedError(
                "Don't know how to handle change_type %r" % change_type)


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
            logger.warning(
                "%r is already a registered callback for %r", callback, key)
            return

        callbacks.append(callback)
        logger.debug("Registered callback %r for %r", callback, key)

    @classmethod
    def deregister_callback(cls, key, callback):
        if key in cls.callbacks and callback in cls.callbacks[key]:
            # remove all instances of the callback
            while callback in cls.callbacks[key]:
                 cls.callbacks[key].remove(callback)

            # if callbacks no longer exist, remove the key
            if not cls.callbacks[key]:
                cls.callbacks.pop(key)
        else:  # pragma: no cover
            logger.warning(
                "%r is not a registered callback for %r", callback, key)

    def changed(self, change_type, key, value=NOTSET):
        LoggingConfiguration.changed(self, change_type, key, value)

        if key in self.callbacks:
            for callback in self.callbacks[key]:
                callback(change_type, key, value)
                logger.debug(
                    "Key %r was %r, calling callback %s",
                    key, change_type, callback)

# prevent a call to reload() from dumping the config object
try:
    config
except NameError:
    config = ConfigurationWithCallbacks()