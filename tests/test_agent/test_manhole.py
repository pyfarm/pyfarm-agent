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

import os
from collections import namedtuple
from pprint import pprint
from random import randint
from StringIO import StringIO
from textwrap import dedent

try:
    from unittest.mock import patch
except ImportError:  # pragma: no cover
    from mock import patch

from twisted.internet.protocol import ServerFactory
from twisted.cred.portal import Portal
from twisted.conch.telnet import (
    ITelnetProtocol, TelnetBootstrapProtocol, TelnetTransport)

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.manhole import (
    LoggingManhole, TransportProtocolFactory, TelnetRealm,
    manhole_factory, show)


Peer = namedtuple("Peer", ("host", "port"))


class FakeLoggingManhole(LoggingManhole):
    QUIT = False
    GET_PEER_CALLS = 0

    class terminal(object):
        RIGHT_ARROW, LEFT_ARROW = None, None

        class transport(object):
            @classmethod
            def getPeer(cls):
                FakeLoggingManhole.GET_PEER_CALLS += 1
                return Peer(os.urandom(12).encode("hex"), randint(1024, 65535))

    def handle_QUIT(self):
        self.QUIT = True


class TestManholeBase(TestCase):
    def setUp(self):
        TelnetRealm.NAMESPACE = None
        FakeLoggingManhole.GET_PEER_CALLS = 0
        FakeLoggingManhole.QUIT = False


class TestManholeFactory(TestManholeBase):
    def test_assertions(self):
        with self.assertRaises(AssertionError):
            manhole_factory(None, "", "")

        with self.assertRaises(AssertionError):
            manhole_factory({}, None, "")

        with self.assertRaises(AssertionError):
            manhole_factory({}, "", None)

    def test_instance_one(self):
        namespace = {"bob": None}
        username = os.urandom(32).encode("hex")
        password = os.urandom(32).encode("hex")
        manhole_factory(namespace, username, password)

        with self.assertRaises(AssertionError):
            manhole_factory(namespace, username, password)

    def test_instance(self):
        namespace = {"bob": None}
        username = os.urandom(32).encode("hex")
        password = os.urandom(32).encode("hex")
        manhole = manhole_factory(namespace, username, password)
        self.assertEqual(namespace, {"bob": None})
        self.assertEqual(
            TelnetRealm.NAMESPACE,
            {"bob": None, "pp": pprint, "show": show})
        self.assertIsInstance(manhole, ServerFactory)
        self.assertIsInstance(manhole.protocol, TransportProtocolFactory)
        self.assertIsInstance(manhole.protocol.portal, Portal)

        # There could be multiple password checkers, check for the one
        # we know we should have added.
        for _, instance in manhole.protocol.portal.checkers.items():
            found = False
            for user, passwd in instance.users.items():
                if user == username and passwd == password:
                    found = True
            if found:
                break
        else:
            self.fail("Failed to find correct username and password.")

    def test_request_avatar(self):
        realm = TelnetRealm()
        avatar = realm.requestAvatar(None, ITelnetProtocol)
        self.assertEqual(len(avatar), 3)
        self.assertIs(avatar[0], ITelnetProtocol)
        self.assertIsInstance(avatar[1], TelnetBootstrapProtocol)
        self.assertTrue(callable(avatar[2]))

    def test_request_avatar_error(self):
        realm = TelnetRealm()
        with self.assertRaises(NotImplementedError):
            realm.requestAvatar(None, None)

    def test_protocol_factory(self):
        factory = TransportProtocolFactory(None)
        transport = factory()
        self.assertIsInstance(transport, TelnetTransport)


class TestManholeShow(TestManholeBase):
    def test_uses_namespace(self):
        namespace = {"bob": None}
        username = os.urandom(32).encode("hex")
        password = os.urandom(32).encode("hex")
        manhole_factory(namespace, username, password)
        output = StringIO()
        with patch("sys.stdout", output):
            show()

        output.seek(0)
        output = output.getvalue().strip()
        self.assertEqual(output, "objects: ['bob', 'pp', 'show']")

    def test_custom_object(self):
        class Foobar(object):
            a, b, c, d, e = True, 1, "yes", {}, 0.0

        output = StringIO()
        with patch("sys.stdout", output):
            show(Foobar)

        output.seek(0)
        output = output.getvalue().strip()
        self.assertEqual(
            output,
            dedent("""
            data attributes of <class 'tests.test_agent.test_manhole.Foobar'>
                           a : True
                           b : 1
                           c : yes
                           d : {} (0 elements)
                           e : 0.0
            """).strip())

    def test_wrap_long_line(self):
        class Foobar(object):
            a = " " * 90

        output = StringIO()
        with patch("sys.stdout", output):
            show(Foobar)

        output.seek(0)
        output = output.getvalue().strip()
        self.assertEqual(
            output,
            dedent("""
            data attributes of <class 'tests.test_agent.test_manhole.Foobar'>
                           a : '                 """ +
                   """                                          '...
            """).strip())


class TestLoggingManhole(TestManholeBase):
    def test_line_received(self):
        f = FakeLoggingManhole()
        f.lineReceived("exit")
        self.assertTrue(f.QUIT)
