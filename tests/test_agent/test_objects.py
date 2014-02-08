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

from twisted.internet import reactor

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.utility.objects import (
    LoggingConfiguration, ConfigurationWithCallbacks)


class ChangedLoggingConfiguration(LoggingConfiguration):
    def __init__(self, *args, **kwargs):
        self.created = []
        self.modified = []
        self.deleted = []
        LoggingConfiguration.__init__(self, *args, **kwargs)

    def changed(self, change_type, key, value):
        if change_type == self.CREATED:
            self.created.append(key)
        elif change_type == self.MODIFIED:
            self.modified.append(key)
        elif change_type == self.DELETED:
            self.deleted.append(key)


class TestLoggingConfiguration(TestCase):
    def setUp(self):
        super(TestLoggingConfiguration, self).setUp()
        self.running = False
        self.messages = []

        def capture_logs(_, message):
            self.messages.append(message)

        LoggingConfiguration.reactor = self
        LoggingConfiguration.log_queue.clear()
        LoggingConfiguration._log = capture_logs

    def tearDown(self):
        super(TestLoggingConfiguration, self).tearDown()
        LoggingConfiguration.reactor = reactor

    def test_queues_messages(self):
        config = LoggingConfiguration()
        config.changed(LoggingConfiguration.CREATED, "a", True)
        self.assertEqual(
            [(("set 'a' = True",), {})], list(config.log_queue))

    def test_changed(self):
        self.running = True

        config = ChangedLoggingConfiguration()

        # test created
        config["a"] = True
        config.update(b=True)
        config.update({"c": True})
        config.setdefault("d", True)
        self.assertEqual(config.created, ["a", "b", "c", "d"])
        self.assertEqual(config.modified, [])
        self.assertEqual(config.deleted, [])

        # test modified
        config["a"] = False
        config.update(b=False)
        config.update({"c": False})
        config.setdefault("d", False)
        self.assertEqual(config.created, ["a", "b", "c", "d"])
        self.assertEqual(config.modified, ["a", "b", "c"])
        self.assertEqual(config.deleted, [])

        # test deleted
        del config["a"]
        config.pop("b")
        self.assertEqual(config.created, ["a", "b", "c", "d"])
        self.assertEqual(config.modified, ["a", "b", "c"])
        self.assertEqual(config.deleted, ["a", "b"])


class TestCallbackConfiguration(TestCase):
    def setUp(self):
        self.running = True

        ConfigurationWithCallbacks.reactor = self
        ConfigurationWithCallbacks.log_queue.clear()
        ConfigurationWithCallbacks.callbacks.clear()

    def tearDown(self):
        ConfigurationWithCallbacks.reactor = reactor

    def test_register_callback_once(self):
        config = ConfigurationWithCallbacks()
        callback = lambda: None
        config.register_callback("a", callback)
        config.register_callback("a", callback)
        self.assertEqual({"a": [callback]}, config.callbacks)

    def test_register_callback_multiple(self):
        config = ConfigurationWithCallbacks()
        callback = lambda: None
        config.register_callback("a", callback)
        config.register_callback("a", callback, append=True)
        self.assertEqual({"a": [callback, callback]}, config.callbacks)

    def test_deregister_callback(self):
        config = ConfigurationWithCallbacks()
        callback = lambda: None
        config.register_callback("a", callback)
        config.register_callback("a", callback, append=True)
        config.deregister_callback("a", callback)
        self.assertEqual({}, config.callbacks)

    def test_callback_called(self):
        created = []
        modified = []
        deleted = []

        def callback(change_type, key, value, reactor_running):
            self.assertEqual(reactor_running, self.running)
            if change_type == ConfigurationWithCallbacks.CREATED:
                created.append(key)
            elif change_type == ConfigurationWithCallbacks.MODIFIED:
                modified.append(key)
            elif change_type == ConfigurationWithCallbacks.DELETED:
                deleted.append(key)
            else:
                self.fail("invalid change type %s" % change_type)

        config = ConfigurationWithCallbacks()
        config.register_callback("a", callback)
        config["a"] = True
        config["a"] = False
        del config["a"]
        self.assertEqual(created, ["a"])
        self.assertEqual(modified, ["a"])
        self.assertEqual(deleted, ["a"])
