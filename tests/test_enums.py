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

import sys
import inspect
import warnings

if sys.version_info[0:2] < (2, 7):
    from unittest2 import TestCase
else:
    from unittest import TestCase

from pyfarm.core import enums
from pyfarm.core.warning import NotImplementedWarning


class TestEnums(TestCase):
    def setUp(self):
        warnings.simplefilter("ignore", NotImplementedWarning)

    def tearDown(self):
        warnings.simplefilter("always", NotImplementedWarning)

    def test_duplicate_enum_values(self):
        values = []
        for name, value in vars(enums).iteritems():
            if hasattr(value, "_asdict") and not name.startswith("_"):
                values.extend(value._asdict().values())

        self.assertEqual(
            len(set(values)), len(values), "duplicate enum number(s) found")

    def test_getOs(self):
        self.assertEqual(
            enums._getOS("linux"), enums.OperatingSystem.LINUX)
        self.assertEqual(
            enums._getOS("win"), enums.OperatingSystem.WINDOWS)
        self.assertEqual(
            enums._getOS("darwin"), enums.OperatingSystem.MAC)
        self.assertEqual(
            enums._getOS("FOO"), enums.OperatingSystem.OTHER)