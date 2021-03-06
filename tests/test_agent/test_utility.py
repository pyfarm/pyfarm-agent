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
import shutil
import os
import re
import tempfile
from contextlib import nested
from decimal import Decimal
from datetime import datetime, timedelta
from errno import ENOENT
from json import dumps as dumps_
from uuid import UUID, uuid4
from os.path import isfile, isdir

from mock import Mock, patch
from twisted.internet import reactor
from twisted.internet.defer import (
    AlreadyCalledError, DeferredLock, Deferred, inlineCallbacks)
from twisted.internet.error import AlreadyCalled
from voluptuous import Invalid

from pyfarm.agent.config import config
from pyfarm.agent.testutil import TestCase, DummyRequest
from pyfarm.agent.utility import (
    UnicodeCSVWriter, UnicodeCSVReader, default_json_encoder, dumps,
    quote_url, request_from_master, total_seconds, validate_environment,
    AgentUUID, remove_file, remove_directory, validate_uuid, TimedDeferredLock,
    LockTimeoutError)

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


class TestValidateUUID(TestCase):
    def test_uuid(self):
        uid = uuid4()
        self.assertIs(validate_uuid(uid), uid)

    def test_hex(self):
        uid = uuid4()
        self.assertEqual(validate_uuid(uid.hex).hex, uid.hex)

    def test_invalid_hex(self):
        with self.assertRaises(Invalid):
            validate_uuid("hello world")

    def test_other_invalid(self):
        for value in (None, True, lambda: None, 1):
            with self.assertRaises(Invalid):
                validate_uuid(value)


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
        request = DummyRequest("")
        request.requestHeaders.setRawHeaders(
            "User-Agent", [config["master_user_agent"]])
        self.assertTrue(request_from_master(request))

    def test_request_not_from_master(self):
        request = DummyRequest("")
        request.requestHeaders.setRawHeaders(
            "User-Agent", ["foobar"])
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

    def test_raises_unhandled_error(self):
        path = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, path)

        with self.assertRaises(IOError):
            AgentUUID.load(path)


class TestRemoveFile(TestCase):
    def setUp(self):
        super(TestRemoveFile, self).setUp()
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.path = path
        del atexit._exithandlers[:]
        self.atexit_signature = [
            (remove_file, (self.path, ),
             {"raise_": False, "retry_on_exit": False})]

    def test_removes_file(self):
        remove_file(self.path)
        self.assertFalse(isfile(self.path))

    def test_ignored_error(self):
        os.remove(self.path)
        remove_file(self.path, ignored_errnos=(ENOENT, ), raise_=True)
        self.assertFalse(isfile(self.path))

    def test_retry_on_shutdown_no_raise(self):
        os.remove(self.path)
        remove_file(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        self.assertEqual(atexit._exithandlers, self.atexit_signature)

    def test_retry_on_shutdown_raise(self):
        os.remove(self.path)

        with self.assertRaises((WindowsError, OSError, IOError)):
            remove_file(
                self.path, ignored_errnos=(), retry_on_exit=True, raise_=True)

        self.assertEqual(atexit._exithandlers, self.atexit_signature)

    def test_retry_only_once(self):
        os.remove(self.path)

        # If remove_file is wrapped in a while loop and is being
        # passed the same arguments, it should only be one call for atexit.
        remove_file(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        remove_file(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        self.assertEqual(atexit._exithandlers, self.atexit_signature)


class TestRemoveDirectory(TestCase):
    def setUp(self):
        super(TestRemoveDirectory, self).setUp()
        path = tempfile.mkdtemp()
        self.path = path
        del atexit._exithandlers[:]
        self.atexit_signature = [
            (remove_directory, (self.path, ),
             {"raise_": False, "retry_on_exit": False})]

    def test_removes_directory(self):
        remove_directory(self.path)
        self.assertFalse(isdir(self.path))

    def test_ignored_error(self):
        os.rmdir(self.path)
        remove_directory(self.path, ignored_errnos=(ENOENT, ), raise_=True)
        self.assertFalse(isdir(self.path))

    def test_retry_on_shutdown_no_raise(self):
        os.rmdir(self.path)
        remove_directory(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        self.assertEqual(atexit._exithandlers, self.atexit_signature)

    def test_retry_on_shutdown_raise(self):
        os.rmdir(self.path)

        with self.assertRaises((WindowsError, OSError, IOError)):
            remove_directory(
                self.path, ignored_errnos=(), retry_on_exit=True, raise_=True)

        self.assertEqual(atexit._exithandlers, self.atexit_signature)

    def test_retry_only_once(self):
        os.rmdir(self.path)

        # If remove_file is wrapped in a while loop and is being
        # passed the same arguments, it should only be one call for atexit.
        remove_directory(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        remove_directory(
            self.path, ignored_errnos=(), retry_on_exit=True, raise_=False)
        self.assertEqual(atexit._exithandlers, self.atexit_signature)


class TestTimedDeferredLock(TestCase):
    def test_parent_class(self):
        deferred = TimedDeferredLock()
        self.assertIsInstance(deferred, DeferredLock)

    def test_exception_parent_class(self):
        self.assertIsInstance(LockTimeoutError(), Exception)

    def test_acquire_assertion(self):
        deferred = TimedDeferredLock()

        for value in (-1, 0, "", datetime.utcnow()):
            with self.assertRaises(AssertionError):
                deferred.acquire(value)

    def test_private_call_callback_ignores_already_called_error(self):
        deferred = TimedDeferredLock()

        def raise_(_):
            raise AlreadyCalledError

        mock = Mock(callback=Mock(side_effect=raise_))
        deferred._call_callback(None, mock)
        mock.callback.assert_called_with(None)

    def test_private_cancel_timeout_ignores_already_called(self):
        deferred = TimedDeferredLock()

        def raise_():
            raise AlreadyCalled

        mock = Mock(cancel=Mock(side_effect=raise_))
        deferred._cancel_timeout(None, mock)
        self.assertEqual(mock.cancel.call_count, 1)

    def test_private_timeout(self):
        deferred = TimedDeferredLock()
        mock = Mock(errback=Mock())
        deferred._timeout(mock)
        self.assertEqual(mock.errback.call_count, 1)
        keywords = mock.errback.mock_calls[0][2]
        self.assertIn("fail", keywords)
        self.assertIsInstance(keywords["fail"], LockTimeoutError)

    @inlineCallbacks
    def test_standard_behavior(self):
        deferred = TimedDeferredLock()
        yield deferred.acquire()
        self.assertTrue(deferred.locked)
        self.assertEqual(len(deferred.waiting), 0)
        second_acquire = deferred.acquire()
        self.assertTrue(deferred.locked)
        self.assertEqual(len(deferred.waiting), 1)
        self.assertFalse(second_acquire.called)
        yield deferred.release()
        self.assertTrue(second_acquire.called)
        self.assertTrue(deferred.locked)
        self.assertEqual(len(deferred.waiting), 0)
        yield deferred.release()
        self.assertFalse(deferred.locked)
        self.assertEqual(len(deferred.waiting), 0)

    @inlineCallbacks
    def test_acquire_timeout(self):
        deferred = TimedDeferredLock()
        yield deferred.acquire()

        self.assertTrue(deferred.locked)
        self.assertEqual(len(deferred.waiting), 0)

        start = datetime.utcnow()
        with self.assertRaises(LockTimeoutError):
            yield deferred.acquire(1)
        stop = datetime.utcnow()

        self.assertTrue(1.1 > total_seconds(stop - start) > .9)

    @inlineCallbacks
    def test_cancel_timeout(self):
        deferred = TimedDeferredLock()

        # Mock out the parts that will leak a Deferred object (which
        # will cause the tests to fail)
        with nested(
            patch.object(reactor, "callLater"),
            patch.object(deferred, "_cancel_timeout"),
        ) as (callLater, cancel_timeout):
            yield deferred.acquire()
            deferred.acquire(10)
            yield deferred.release()

        self.assertEqual(callLater.call_count, 1)
        timeout, timeout_method, finished = callLater.mock_calls[0][1]
        self.assertEqual(timeout, 10)
        self.assertIs(
            timeout_method.im_func.func_name,
            deferred._timeout.im_func.func_name)
        self.assertIsInstance(finished, Deferred)
