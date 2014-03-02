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

import os
import json
import tempfile

from pyfarm.core.enums import PY26

if PY26:
    import unittest2 as unittest
else:
    import unittest

from pyfarm.core.logger import getLogger, config
from pyfarm.core.utility import dumps


class TestLogger(unittest.TestCase):
    INITIAL_ENVIRONMENT = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.INITIAL_ENVIRONMENT)

    def remove(self, path):
        try:
            os.remove(path)
        except:
            pass

    def test_get_logger_name(self):
        logger = getLogger("foo")
        self.assertEqual(logger.name, "pf.foo")
        self.assertFalse(logger.handlers)

    def test_config_empty(self):
        os.environ["PYFARM_LOGGING_CONFIG"] = ""
        with self.assertRaises(ValueError):
            config.get()

    def test_config_file(self):
        _, path = tempfile.mkstemp()

        with open(path, "w") as stream:
            stream.write(json.dumps(config.DEFAULT_CONFIGURATION.copy()))

        os.environ["PYFARM_LOGGING_CONFIG"] = path
        self.assertEqual(config.get(), config.DEFAULT_CONFIGURATION)
        self.remove(path)

    def test_corrupt_config_file(self):
        _, path = tempfile.mkstemp()

        with open(path, "w") as stream:
            stream.write('{"foo" "bar"}')

        os.environ["PYFARM_LOGGING_CONFIG"] = path
        with self.assertRaisesRegex(
                ValueError, "Failed to parse json data from %s" % path):
            self.assertEqual(config.get(), config.DEFAULT_CONFIGURATION)
        self.remove(path)

    def test_config_in_environment(self):
        os.environ["PYFARM_LOGGING_CONFIG"] = json.dumps(
            config.DEFAULT_CONFIGURATION)
        self.assertEqual(config.get(), config.DEFAULT_CONFIGURATION)

    def test_corrupt_config_in_environment(self):
        os.environ["PYFARM_LOGGING_CONFIG"] = '{"foo" "bar"}'

        with self.assertRaisesRegex(
                ValueError,
                "Failed to parse json data from \$PYFARM_LOGGING_CONFIG"):
            self.assertEqual(config.get(), config.DEFAULT_CONFIGURATION)


