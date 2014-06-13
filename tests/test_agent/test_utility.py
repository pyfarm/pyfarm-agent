# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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
from decimal import Decimal
from datetime import datetime
from json import dumps as dumps_
from uuid import uuid1

from pyfarm.agent.config import config
from pyfarm.agent.sysinfo.system import system_identifier
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.utility import default_json_encoder, dumps, uuid


class TestDefaultJsonEncoder(TestCase):
    def test_default_json_encoder_decimal(self):
        self.assertAlmostEqual(
            default_json_encoder(Decimal("1.2")), 1.2, places=2)

    def test_default_json_encoder_datetime(self):
        now = datetime.utcnow()
        self.assertEqual(default_json_encoder(now), now.isoformat())

    def test_default_json_encoder_unhandled(self):
        self.assertIsNone(default_json_encoder(1), None)


class TestDumpsJson(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.data = {
            os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}

    def test_dumps_pretty(self):
        config["pretty-json"] = True
        self.assertEqual(dumps(self.data), dumps_(self.data, indent=2))

    def test_dumps_not_pretty(self):
        config["pretty-json"] = False
        self.assertEqual(dumps(self.data), dumps_(self.data))

    def test_dumps_pretty_default(self):
        config.pop("pretty-json", None)
        self.assertEqual(dumps(self.data), dumps_(self.data))

    def test_dumps_single_argument(self):
        config["pretty-json"] = False
        data = self.data.keys()[0]
        self.assertEqual(dumps(data), dumps_(data))

    def test_dumps_datetime(self):
        config["pretty-json"] = False
        data = {"datetime": datetime.utcnow()}
        self.assertEqual(
            dumps(data), dumps_(data, default=default_json_encoder))

    def test_dumps_decimal(self):
        config["pretty-json"] = False
        data = {"decimal": Decimal("1.2")}
        self.assertEqual(
            dumps(data), dumps_(data, default=default_json_encoder))


class TestGeneral(TestCase):
    def test_uuid(self):
        internal_uuid = uuid().hex
        stduuid = uuid1(node=system_identifier()).hex
        self.assertEqual(internal_uuid[8:16], stduuid[8:16])
