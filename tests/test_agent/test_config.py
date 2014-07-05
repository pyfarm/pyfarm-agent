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

from datetime import datetime
from os import urandom
from random import randint

from pyfarm.core.enums import NOTSET
from pyfarm.agent.config import LoggingConfiguration, ConfigurationWithCallbacks
from pyfarm.agent.testutil import TestCase


class ChangedLoggingConfiguration(LoggingConfiguration):
    def __init__(self, *args, **kwargs):
        self.created = []
        self.modified = []
        self.deleted = []
        kwargs.update(load=False)
        super(ChangedLoggingConfiguration, self).__init__(*args, **kwargs)

    def changed(self, change_type, key, new_value=NOTSET, old_value=NOTSET):
        if change_type == self.CREATED:
            self.created.append(key)
        elif change_type == self.MODIFIED:
            self.modified.append(key)
        elif change_type == self.DELETED:
            self.deleted.append(key)

        super(ChangedLoggingConfiguration, self).changed(
            change_type, key, new_value=new_value, old_value=old_value)


class TestLoggingConfiguration(TestCase):
    def get_data(self):
        return {
            "int": randint(-1024, 1024),
            urandom(16).encode("hex"): urandom(16).encode("hex"),
            urandom(16).encode("hex"): urandom(16).encode("hex")}

    def test_created(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        self.assertEqual(set(config.created), set(data))

    def test_setitem(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration()

        for k, v in data.items():
            config[k] = v

        self.assertEqual(dict(config), data)
        self.assertEqual(set(config.created), set(data))

        for key in data:
            config[key] = urandom(16)

        self.assertEqual(set(config.modified), set(data))

    def test_delitem(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        keys = []
        for i in config.keys():
            del config[i]
            keys.append(i)

        self.assertEqual(set(config.deleted), set(keys))

    def test_pop(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        for key in config.keys():
            config.pop(key)

        self.assertEqual(dict(config), {})
        self.assertEqual(set(config.deleted), set(data))

    def test_clear(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        config.clear()
        self.assertEqual(dict(config), {})
        self.assertEqual(set(config.deleted), set(data))

    def test_update_dict(self):
        data = self.get_data()
        update_data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        config.update(update_data)
        all_data = dict(data.items() + update_data.items())
        self.assertEqual(dict(config), all_data)
        self.assertEqual(set(config.created), set(all_data))
        for key in all_data.keys():
            config[key] = urandom(16)

        self.assertEqual(set(config.modified), set(all_data.keys()))

    def test_update_kwargs(self):
        data = self.get_data()
        update_data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        config.update(**update_data)
        all_data = dict(data.items() + update_data.items())
        self.assertEqual(dict(config), all_data)
        self.assertEqual(set(config.created), set(all_data))
        for key in all_data.keys():
            config[key] = urandom(16)

        self.assertEqual(set(config.modified), set(all_data.keys()))

    def test_raises_type_error(self):
        with self.assertRaises(TypeError):
            LoggingConfiguration(seq=1)

    def test_update_modified_dict(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration()
        config.update(data)
        for key, value in config.items():
            config.update({key: urandom(16)})

        self.assertEqual(set(config.modified), set(data.keys()))

    def test_update_modified_kwargs(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration()
        config.update(data)
        for key, value in config.items():
            config.update(**{key: True})

        self.assertEqual(set(config.modified), set(data.keys()))

    def test_update_modified_type_error(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration()
        config.update(data)
        for _, _ in config.items():
            with self.assertRaises(TypeError):
                config.update(1)


class TestMasterContacted(TestCase):
    def test_no_update(self):
        config = LoggingConfiguration()
        config.clear()
        self.assertIsNone(config.master_contacted(update=False))
        self.assertNotIn("last_announce", config)
        self.assertNotIn("last_master_contact", config)

    def test_announce(self):
        config = LoggingConfiguration()
        config.clear()
        now = datetime.utcnow()
        config.master_contacted(announcement=True)
        self.assertDateAlmostEqual(now, config["last_announce"])

    def test_update(self):
        config = LoggingConfiguration()
        config.clear()
        now = datetime.utcnow()
        result = config.master_contacted(update=True, announcement=False)
        self.assertDateAlmostEqual(now, result)
        self.assertIs(result, config["last_master_contact"])

    def test_update_default(self):
        config = LoggingConfiguration()
        config.clear()
        now = datetime.utcnow()
        result = config.master_contacted()
        self.assertDateAlmostEqual(now, result)
        self.assertIs(result, config["last_master_contact"])
        self.assertNotIn("last_announce", config)


class TestConfigurationExceptions(TestCase):
    def test_change_type_assert_missing_value(self):
        config = LoggingConfiguration()

        with self.assertRaises(AssertionError):
            config.changed(LoggingConfiguration.MODIFIED, "")

        with self.assertRaises(AssertionError):
            config.changed(LoggingConfiguration.CREATED, "")

    def test_change_unknown_change_type(self):
        config = LoggingConfiguration()
        with self.assertRaises(NotImplementedError):
            config.changed("FOOBAR", "", "")


class TestCallbackConfiguration(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        ConfigurationWithCallbacks.callbacks.clear()

    def test_assert_callable(self):
        config = ConfigurationWithCallbacks()
        with self.assertRaises(AssertionError):
            config.register_callback("", None)

    def test_add_callback(self):
        callback = lambda: None
        config = ConfigurationWithCallbacks()
        config.register_callback("foo", callback)
        self.assertEqual([callback], config.callbacks["foo"])
        config.register_callback("foo", callback)
        self.assertEqual([callback], config.callbacks["foo"])

    def test_add_callback_append(self):
        callback = lambda: None
        config = ConfigurationWithCallbacks()
        config.register_callback("foo", callback)
        self.assertEqual([callback], config.callbacks["foo"])
        config.register_callback("foo", callback, append=True)
        self.assertEqual([callback, callback], config.callbacks["foo"])

    def test_deregister_callback(self):
        callback = lambda: None
        config = ConfigurationWithCallbacks()
        config.register_callback("foo", callback)
        config.deregister_callback("foo", callback)
        self.assertNotIn("foo", config.callbacks)

    def test_deregister_multiple_callback(self):
        callback = lambda: None
        config = ConfigurationWithCallbacks()
        config.register_callback("foo", callback)
        config.register_callback("foo", callback, append=True)
        config.deregister_callback("foo", callback)
        self.assertNotIn("foo", config.callbacks)

    def test_callback_on_change(self):
        results = []

        def callback(change_type, key, value, old_value):
            results.append((change_type, key, value))

        config = ConfigurationWithCallbacks()
        config.register_callback("foo", callback)
        config["foo"] = True
        config["foo"] = False
        del config["foo"]

        self.assertEqual(results, [
            (LoggingConfiguration.CREATED, "foo", True),
            (LoggingConfiguration.MODIFIED, "foo", False),
            (LoggingConfiguration.DELETED, "foo", NOTSET)])
