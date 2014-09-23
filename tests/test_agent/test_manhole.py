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
from textwrap import dedent
from pprint import pprint
from StringIO import StringIO

try:
    from unittest.mock import patch
except ImportError:  # pragma: no cover
    from mock import patch

from twisted.internet.protocol import ServerFactory
from twisted.cred.portal import Portal

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.manhole import (
    TransportProtocolFactory, TelnetRealm, manhole_factory, show)


class TestManhole(TestCase):
    def setUp(self):
        TelnetRealm.NAMESPACE = None

    def test_factory_assertions(self):
        with self.assertRaises(AssertionError):
            manhole_factory(None, "", "")

        with self.assertRaises(AssertionError):
            manhole_factory({}, None, "")

        with self.assertRaises(AssertionError):
            manhole_factory({}, "", None)

    def test_factory_once_only(self):
        namespace = {"bob": None}
        username = os.urandom(32).encode("hex")
        password = os.urandom(32).encode("hex")
        manhole_factory(namespace, username, password)

        with self.assertRaises(AssertionError):
            manhole_factory(namespace, username, password)

    def test_factory(self):
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

    def test_show_uses_namespace(self):
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

    def test_show_custom_object(self):
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

    def test_show_wrap_long_line(self):
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
