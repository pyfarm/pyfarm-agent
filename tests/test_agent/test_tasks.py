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

from twisted.internet.task import Clock

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.tasks import ScheduledTaskManager


class TestScheduledTasks(TestCase):
    def setUp(self):
        super(TestScheduledTasks, self).setUp()
        self.count = 0

    def test_basic_register(self):
        lc = ScheduledTaskManager()
        func = lambda: None
        lc.register(func, 1)
        self.assertIn(func, lc.tasks)
        lc.register(func, 2)
        self.assertEqual(lc.tasks[func][1], 1)

    def test_assertion_errors(self):
        lc = ScheduledTaskManager()

        with self.assertRaises(AssertionError):
            lc.register(None, 1)

        with self.assertRaises(AssertionError):
            lc.register(lambda: None, 1, func_args=1)

        with self.assertRaises(AssertionError):
            lc.register(lambda: None, 1, func_kwargs=1)

    def test_start_on_register(self):
        lc = ScheduledTaskManager()

        def func():
            self.count += 1

        clock = Clock()
        interval = 1
        lc.register(func, interval, start=True, clock=clock)

        for i in xrange(1, 6):
            self.assertEqual(self.count, i)
            clock.advance(1)

    def test_start_all(self):
        lc = ScheduledTaskManager()

        def func():
            self.count += 1

        clock = Clock()
        interval = 1
        looping_call = lc.register(func, interval, start=False, clock=clock)
        self.assertFalse(looping_call.running)
        lc.start()
        self.assertTrue(looping_call.running)

    def test_stop_all(self):
        lc = ScheduledTaskManager()

        def func():
            self.count += 1

        clock = Clock()
        interval = 1
        looping_call = lc.register(func, interval, start=False, clock=clock)
        self.assertFalse(looping_call.running)
        lc.start()
        self.assertTrue(looping_call.running)
        lc.stop()
        self.assertFalse(looping_call.running)
