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

from utcore import unittest
from pyfarm.core.logger import (
    ROOT_HANDLER, ROOT_FORMAT, DEFAULT_LEVEL, root, getLogger)


class TestLogger(unittest.TestCase):
    def test_root_setup(self):
        self.assertEqual(root.propagate, 0)
        self.assertEqual(root.level, DEFAULT_LEVEL)
        self.assertIn(ROOT_HANDLER, root.handlers)
        self.assertIs(ROOT_HANDLER.formatter, ROOT_FORMAT)

    def test_get_logger(self):
        logger = getLogger("foo")
        self.assertEqual(logger.name, ".".join([root.name, "foo"]))
        self.assertFalse(logger.handlers)
        self.assertEqual(logger.propagate, 1)
        self.assertEqual(logger.level, DEFAULT_LEVEL)
