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

from voluptuous import Invalid

from pyfarm.agent.http.api.assign import validate_environment
from pyfarm.agent.testutil import TestCase


class TestValidateEnvironment(TestCase):
    def test_type(self):
        with self.assertRaisesRegexp(Invalid, "Expected a dictionary"):
            validate_environment(None)

    def test_value(self):
        with self.assertRaisesRegexp(Invalid, "Value.*string.*"):
            validate_environment({"foo": None})

    def test_key(self):
        with self.assertRaisesRegexp(Invalid, "Key.*string.*"):
            validate_environment({1: None})


