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

from utcore import TestCase
from pyfarm.agent.utility.objects import LoggingDictionary


class TestObjects(TestCase):
    def setUp(self):
        super(TestObjects, self).setUp()
        self.running = False
        self.emitted = []

        # retrofit/cleanup the class so we can
        # capture things as they happen
        LoggingDictionary.log_queue.clear()
        LoggingDictionary.reactor = self
        self.lg = LoggingDictionary()
        self.emitter = self.lg.emit

        def emitter(value):
            self.emitted.append(value)

        self.lg.emit = emitter

    def tearDown(self):
        super(TestObjects, self).tearDown()
        LoggingDictionary.reactor = reactor

    def test_queues_log_messages(self):
        self.lg["a"] = self.lg["b"] = None
        self.assertEqual([("a", None), ("b", None)], list(self.lg.log_queue))

    def test_emit_queued_messages(self):
        self.lg["a"] = self.lg["b"] = False
        self.running = True
        self.lg["c"] = False
        self.assertEqual(list(self.lg.log_queue), [])
        self.assertEqual(self.emitted, ["a=False", "b=False", "c=False"])

    def test_emit_log_for_changed_value(self):
        self.running = True
        self.lg["a"] = True
        self.lg["a"] = False
        self.assertEqual(list(self.lg.log_queue), [])
        self.assertEqual(self.emitted, ["a=True", "a=False"])