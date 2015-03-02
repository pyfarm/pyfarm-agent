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

import atexit
import os
import re
import tempfile
from decimal import Decimal
from datetime import datetime, timedelta
from errno import ENOENT
from json import dumps as dumps_
from uuid import UUID, uuid4
from os.path import isfile

from mock import patch
from voluptuous import Invalid

from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, FakeRequest
from pyfarm.agent.utility import (
    UnicodeCSVWriter, UnicodeCSVReader, default_json_encoder, dumps,
    quote_url, request_from_master, total_seconds, validate_environment,
    AgentUUID, remove_file, logger)

try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError


class TestValidateEnvironment(TestCase):
    def test_type(self):
        with self.assertRaisesRegexp(Invalid,
                                     re.compile("Expected a dictionary")):
            validate_environment(None)

    def test_value(self):
        with self.assertRaisesRegexp(Invalid, re.compile("Value.*string.*")):
            validate_environment({"foo": None})

    def test_key(self):
        with self.assertRaisesRegexp(Invalid, re.compile("Key.*string.*")):
            validate_environment({1: None})


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
        super(TestDumpsJson, self).setUp()
        self.data = {
            os.urandom(16).encode("hex"): os.urandom(16).encode("hex")}

    def test_dumps_pretty(self):
        config["agent_pretty_json"] = True
        self.assertEqual(dumps(self.data), dumps_(self.data, indent=2))

    def test_dumps_not_pretty(self):
        config["agent_pretty_json"] = False
        self.assertEqual(dumps(self.data), dumps_(self.data))

    def test_dumps_single_argument(self):
        config["agent_pretty_json"] = False
        data = self.data.keys()[0]
        self.assertEqual(dumps(data), dumps_(data))

    def test_dumps_datetime(self):
        config["agent_pretty_json"] = False
        data = {"datetime": datetime.utcnow()}
        self.assertEqual(
            dumps(data), dumps_(data, default=default_json_encoder))

    def test_dumps_decimal(self):
        config["agent_pretty_json"] = False
        data = {"decimal": Decimal("1.2")}
        self.assertEqual(
            dumps(data), dumps_(data, default=default_json_encoder))

    def test_dumps_uuid(self):
        data = {"uuid": uuid4()}
        self.assertEqual(dumps(data), dumps({"uuid": str(data["uuid"])}))


class TestGeneral(TestCase):
    def test_request_from_master(self):
        request = FakeRequest(
            self, "GET", "/",
            headers={"User-Agent": config["master_user_agent"]})
        self.assertTrue(request_from_master(request))

    def test_request_not_from_master(self):
        request = FakeRequest(
            self, "GET", "/",
            headers={"User-Agent": "foobar"})
        self.assertFalse(request_from_master(request))

    def test_total_seconds(self):
        self.assertEqual(total_seconds(timedelta(seconds=60)), 60)


class TestCSVBase(TestCase):
    def get_row(self):
        return [os.urandom(16).encode("hex"), os.urandom(16).encode("hex"),
                os.urandom(16).encode("hex"), os.urandom(16).encode("hex")]

    def get_writer(self):
        stream = open(self.create_file(), "w")
        return UnicodeCSVWriter(stream), stream


class TestCSVWriter(TestCSVBase):
    def test_writerow(self):
        writer, stream = self.get_writer()
        path = stream.name
        row = self.get_row()
        writer.writerow(row)
        stream.close()
        with open(path, "r") as stream:
            written_row = stream.read()
        self.assertEqual(written_row.strip(), ",".join(row))

    def test_writerows(self):
        writer, stream = self.get_writer()
        path = stream.name
        rows = [self.get_row() for _ in range(5)]
        writer.writerows(rows)
        stream.close()
        with open(path, "r") as stream:
            written_rows = stream.read()

        self.assertEqual(
            written_rows.strip(), "\r\n".join([",".join(row) for row in rows]))


class TestCSVReader(TestCSVBase):
    def test_iter(self):
        writer, stream = self.get_writer()
        path = stream.name
        rows = [self.get_row() for _ in range(5)]
        writer.writerows(rows)
        stream.close()
        reader = UnicodeCSVReader(open(path))

        written_rows = []
        for written_row in reader:
            written_rows.append(written_row)
        self.assertEqual(written_rows, rows)

    def test_next(self):
        writer, stream = self.get_writer()
        path = stream.name
        rows = [self.get_row() for _ in range(5)]
        writer.writerows(rows)
        stream.close()
        reader = UnicodeCSVReader(open(path))

        written_rows = []
        while True:
            try:
                written_rows.append(reader.next())
            except StopIteration:
                break
        self.assertEqual(written_rows, rows)


class TestQuoteURL(TestCase):
    def test_simple_with_scheme(self):
        self.assertEqual(quote_url("http://foobar"), "http://foobar")

    def test_simple_without_scheme(self):
        self.assertEqual(quote_url("/foobar"), "/foobar")

    def test_parameters(self):
        self.assertEqual(
            quote_url("/foobar?first=1&second=2"),
            "/foobar?first=1&second=2")

    def test_fragment(self):
        self.assertEqual(
            quote_url("/foobar?first=1&second=2#abcd"),
            "/foobar?first=1&second=2#abcd")


class TestAgentUUID(TestCase):
    def test_generate(self):
        result = AgentUUID.generate()
        self.assertIsInstance(result, UUID)

    def test_save(self):
        data = uuid4()
        saved_path = self.create_file()
        AgentUUID.save(data, saved_path)
        with open(saved_path, "r") as saved_file:
            saved_data = saved_file.read()

        self.assertEqual(saved_data, str(data))

    def test_load(self):
        data = uuid4()
        path = self.create_file(str(data))
        self.assertEqual(AgentUUID.load(path), data)

    def test_load_from_missing_path(self):
        path = self.create_file()
        os.remove(path)
        self.assertIsNone(AgentUUID.load(path))


class TestRemoveFile(TestCase):
    def setUp(self):
        super(TestRemoveFile, self).setUp()
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.path = path
        del atexit._exithandlers[:]

    def test_removes_file(self):
        messages = []

        def capture_messages(message, *args, **kwargs):
            messages.append((message, args, kwargs))

        with patch.object(logger, "info", capture_messages):
            remove_file(self.path)

        for message, args, kwargs in messages:
            if "Removed %s" in message:
                break
        else:
            self.fail("Never found log message.")

        self.assertFalse(isfile(self.path))

    def test_ignored_error(self):
        os.remove(self.path)

        messages = []

        def capture_messages(message, *args, **kwargs):
            messages.append((message, args, kwargs))

        with patch.object(logger, "debug", capture_messages):
            remove_file(self.path, ignored_errnos=(ENOENT, ))

        for message, args, kwargs in messages:
            if "Failed to remove %s (%s)" in message:
                break
        else:
            self.fail("Never found log message.")

        self.assertFalse(isfile(self.path))

    def test_retry_on_shutdown_no_raise(self):
        os.remove(self.path)
        remove_file(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        self.assertEqual(
            atexit._exithandlers,
            [(remove_file, (self.path, ),
              {"raise_": False, "retry_on_exit": False})])

    def test_retry_on_shutdown_raise(self):
        os.remove(self.path)

        with self.assertRaises((WindowsError, OSError, IOError)):
            remove_file(
                self.path, ignored_errnos=(), retry_on_exit=True, raise_=True)

        self.assertEqual(
            atexit._exithandlers,
            [(remove_file, (self.path, ),
              {"raise_": False, "retry_on_exit": False})])
