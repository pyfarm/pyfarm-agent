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

Central module for storing and working with a live configuration objects.  This
module instances :class:`.ConfigurationWithCallbacks` onto :const:`.config`.
Attempting to reload this module will not reinstance the :const:`.config`
object.

The :const:`.config` object should be directly imported from this
module to be used:

    >>> from pyfarm.agent.config import config
"""

import os
from datetime import datetime
from os.path import join, abspath, dirname

from pyfarm.core.enums import NOTSET
from pyfarm.core.config import Configuration
from pyfarm.agent.logger import getLogger
from pyfarm.agent.sysinfo import memory, cpu, network, system

logger = getLogger("agent.config")


class LoggingConfiguration(Configuration):
    """
    Special configuration object which logs when a key is changed in
    a dictionary.  If the reactor is not running then log messages will
    be queued until they can be emitted so they are not lost.

    .. automethod:: _expandvars
    """
    MODIFIED = "modified"
    CREATED = "created"
    DELETED = "deleted"

    def __init__(self, data=None, environment=None, load=True):
        super(LoggingConfiguration, self).__init__("pyfarm.agent")
        assert data is None or isinstance(data, dict)
        assert environment is None or isinstance(environment, dict)

        if environment is None:
            environment = os.environ

        if load:
            self.load(environment=environment)
            self.update(
                # A mapping of UUIDs to job type instances.
                jobtypes={},

                # A mapping of tasks to job type instances.
                current_assignments={},

                # The last time we were in touch with the master,
                # or the last time it was in touch with us.
                last_master_contact=None,

                # The last time we announced ourselves to the master.  This
                # may be longer than --master-reannounce if
                # `last_master_contact` caused us to skip an announcement.
                last_announce=None)

        if data is not None:
            self.update(data)

        # Load configuration file(s) for jobtypes and then
        # update the local instance
        if load:
            jobtypes_config = Configuration(
                "pyfarm.jobtypes", version=self.version)
            jobtypes_config.load(environment=environment)
            self.update(jobtypes_config)

    def _map_value(self, key, value):
        """
        Some configuration values have keywords associated with
        them, this function is responsible for returning the 'fixed'
        value.
        """
        if value == "auto":
            if key == "agent_ram":
                return memory.total_ram()

            if key == "agent_cpus":
                return cpu.total_cpus()

            if key == "agent_hostname":
                return network.hostname()

            if key == "agent_static_root":
                return abspath(
                    join(dirname(__file__), "http", "static"))

        return value

    def __setitem__(self, key, value):
        value = self._map_value(key, value)

        if key not in self:
            self.changed(self.CREATED, key, value, NOTSET)
        elif self[key] != value:
            self.changed(self.MODIFIED, key, value, self[key])

        # Run the base class's method after the above otherwise
        # the value would already be in the data we're comparing
        # against
        super(LoggingConfiguration, self).__setitem__(key, value)

    def __delitem__(self, key):
        """
        Deletes the provided ``key`` and triggers a ``delete`` event
        using :meth:`.changed`.
        """
        old_value = self[key] if key in self else NOTSET
        super(LoggingConfiguration, self).__delitem__(key)
        self.changed(self.DELETED, key, NOTSET, old_value)

    def pop(self, key, *args):
        """
        Deletes the provided ``key`` and triggers a ``delete`` event
        using :meth:`.changed`.
        """
        old_value = self[key] if key in self else NOTSET
        super(LoggingConfiguration, self).pop(key, *args)
        self.changed(self.DELETED, key, NOTSET, old_value)

    def clear(self):
        """
        Deletes all keys in this object and triggers a ``delete`` event
        using :meth:`.changed` for each one.
        """
        keys = list(self.keys())

        # Not quite the same thing as dict.clear() but the effect
        # is the same as the call to changed() is more real time.
        for key in keys:
            old_value = self.pop(key, NOTSET)
            self.changed(self.DELETED, key, NOTSET, old_value)

    def update(self, data=None, **kwargs):
        """
        Updates the data held within this object and triggers the
        appropriate events with :meth:`.changed`.
        """
        def trigger_changed(changed_object):
            try:
                items = changed_object.iteritems()
            except AttributeError:  # pragma: no cover
                items = changed_object.items()

            for key, value in items:
                if key not in self:
                    self.changed(self.CREATED, key, value, NOTSET)

                elif self[key] != value:
                    self.changed(self.MODIFIED, key, value, self[key])

        if isinstance(data, dict):
            for key, value in data.items():
                data[key] = self._map_value(key, value)

            trigger_changed(data)

        elif data is not None:
            raise TypeError("Expected None or dict for `data`")

        elif data is None:
            data = {}

        if kwargs:
            for key, value in kwargs.items():
                kwargs[key] = self._map_value(key, value)

            trigger_changed(kwargs)

        super(LoggingConfiguration, self).update(data, **kwargs)

    def changed(self, change_type, key, new_value=NOTSET, old_value=NOTSET):
        """
        This method is run whenever one of the keys in this object
        changes.
        """
        assert new_value is not NOTSET if change_type != self.DELETED else True
        assert old_value is NOTSET if change_type == self.CREATED else True

        if change_type == self.MODIFIED:
            logger.info("modified %r = %r", key, new_value)

        elif change_type == self.CREATED:
            logger.info("set %r = %r", key, new_value)

        elif change_type == self.DELETED:
            logger.warning("deleted %r", key)

        else:
            raise NotImplementedError(
                "Don't know how to handle change_type %r" % change_type)

    def master_contacted(self, update=True, announcement=False):
        """
        Simple method that will update the ``last_master_contact`` and then
        return the result.

        :param bool update:
            Setting this value to False will just return the current value
            instead of updating the value too.
        """
        if not update and "last_master_contact" not in self:
            return None

        if announcement:
            self["last_announce"] = datetime.utcnow()

        if update:
            self["last_master_contact"] = datetime.utcnow()

        return self["last_master_contact"]


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
        """
        Removes any callback(s) that are registered with the provided ``key``
        """
        results = cls.callbacks.pop(key, None)
        if results is None:  # pragma: no cover
            logger.warning(
                "%r is not a registered callback for %r", callback, key)

    def clear(self, callbacks=False):
        """
        Performs the same operations as :meth:`dict.clear` except
        this method can also clear any registered callbacks if
        requested.
        """
        super(ConfigurationWithCallbacks, self).clear()
        if callbacks:
            self.callbacks.clear()

    def changed(self, change_type, key, new_value=NOTSET, old_value=NOTSET):
        """
        This method is called internally whenever a given ``key``
        changes which in turn will pass off the change to any
        registered callback(s).
        """
        super(ConfigurationWithCallbacks, self).changed(
            change_type, key, new_value=new_value, old_value=old_value)

        if key in self.callbacks:
            for callback in self.callbacks[key]:
                callback(change_type, key, new_value, old_value)
                logger.debug(
                    "Key %r was %r, calling callback %s",
                    key, change_type, callback)

# Prevent a call to reload() from dumping the config object
try:
    config
except NameError:
    config = ConfigurationWithCallbacks()