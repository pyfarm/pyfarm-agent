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

from os import urandom

from pyfarm.core.enums import NOTSET
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.utility.objects import LoggingConfiguration


class ChangedLoggingConfiguration(LoggingConfiguration):
    def __init__(self, *args, **kwargs):
        self.created = []
        self.modified = []
        self.deleted = []
        LoggingConfiguration.__init__(self, *args, **kwargs)

    def changed(self, change_type, key, value=NOTSET):
        if change_type == self.CREATED:
            self.created.append(key)
        elif change_type == self.MODIFIED:
            self.modified.append(key)
        elif change_type == self.DELETED:
            self.deleted.append(key)

        LoggingConfiguration.changed(self, change_type, key, value=value)


class TestLoggingConfiguration(TestCase):
    def get_data(self):
        return {
            urandom(16).encode("hex"): urandom(16).encode("hex"),
            urandom(16).encode("hex"): urandom(16).encode("hex")}

    def test_created(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration(data)
        self.assertEqual(set(config.created), set(data))

    def test_setitem(self):
        data = self.get_data()
        config = ChangedLoggingConfiguration()

        for k,v in data.items():
            config[k] = v

        self.assertEqual(dict(config), data)
        self.assertEqual(set(config.created), set(data))

        for key in data:
            config[key] = None

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
            config[key] = None
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
            config[key] = None
        self.assertEqual(set(config.modified), set(all_data.keys()))


class TestConfigurationExceptions(TestCase):
    def test_change_type_assert_missing_value(self):
        config = LoggingConfiguration()
        self.assertRaises(
            AssertionError,
            lambda: config.changed(LoggingConfiguration.MODIFIED, ""))
        self.assertRaises(
            AssertionError,
            lambda: config.changed(LoggingConfiguration.CREATED, ""))

    def test_change_unknown_change_type(self):
        config = LoggingConfiguration()
        self.assertRaises(
            NotImplementedError,
            lambda: config.changed("FOOBAR", "", ""))

