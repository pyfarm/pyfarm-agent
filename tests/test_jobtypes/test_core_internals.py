# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from threading import Thread
from Queue import Queue


from pyfarm.agent.testutil import TestCase
from pyfarm.jobtypes.core.internals import LoggingThread


class TestLoggingThread(TestCase):
    def test_instance(self):
        test_path = self.create_test_file()
        lt = LoggingThread(test_path)
        self.assertIsInstance(lt, Thread)
        self.assertIsInstance(lt.queue, Queue)
        self.assertFalse(lt.stopped)
        self.assertEqual(lt.lineno, 1)

    def test_shutdown_event_attribute(self):
        lt = LoggingThread(self.create_test_file())
        self.assertIsInstance(lt.shutdown_event, tuple)
        self.assertEqual(lt.shutdown_event[0], "shutdown")
        self.assertEqual(lt.shutdown_event[1][0], "before")
        self.assertEqual(lt.shutdown_event[1][1], lt.stop)

    def test_put_is_stream(self):
        lt = LoggingThread(self.create_test_file())

        # TODO: add context manager to testutil
        # with self.assertRaises(AssertionError, lambda ):
        #     lt.put("", "")
