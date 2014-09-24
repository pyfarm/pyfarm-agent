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

"""
Manhole
=======

Provides a way to access the internals of the agent via
the telnet protocol.
"""

# NOTE: This module is based on buildbot's manhole implementation
#   https://github.com/buildbot/buildbot/blob/
#   1d35e1cb25dfcdd3d30263bdfce8910a56874e60/master/buildbot/manhole.py

from __future__ import print_function

from pprint import pprint
from inspect import ismethod

from twisted.conch.insults.insults import ServerProtocol
from twisted.conch.manhole import ColoredManhole
from twisted.conch.telnet import (
    ITelnetProtocol, TelnetBootstrapProtocol, AuthenticatingTelnetProtocol,
    TelnetTransport)
from twisted.cred.checkers import InMemoryUsernamePasswordDatabaseDontUse
from twisted.cred.portal import IRealm, Portal
from twisted.internet.protocol import ServerFactory
from zope.interface import implements

from pyfarm.core.enums import STRING_TYPES, INTEGER_TYPES, NOTSET
from pyfarm.agent.logger import getLogger

ITERABLE_TYPES = (list, tuple, dict)

logger = getLogger("agent.manhole")


class LoggingManhole(ColoredManhole):
    """
    A slightly modified implementation of :class:`ColoredManhole`
    which logs information to the logger so we can track activity in
    the agent's log.
    """
    def connectionMade(self):  # pragma: no cover
        peer = self.terminal.transport.getPeer()
        logger.info("Connection made from %s@%s", peer.host, peer.port)
        super(LoggingManhole, self).connectionMade()

    def connectionLost(self, reason):  # pragma: no cover
        peer = self.terminal.transport.getPeer()
        logger.info("Connection lost from %s@%s", peer.host, peer.port)
        super(LoggingManhole, self).connectionLost(reason)

    def lineReceived(self, line):
        peer = self.terminal.transport.getPeer()
        logger.info("%s@%s - %s", peer.host, peer.port, line)

        if line.strip() in ("exit", "exit()", "quit", "quit()", "\q"):
            self.handle_QUIT()
        else:  # pragma: no cover
            super(LoggingManhole, self).lineReceived(line)


class TransportProtocolFactory(object):
    """
    Glues together a portal along with the :class:`TelnetTransport` and
    :class:`AuthenticatingTelnetProtocol` objects.  This class is instanced
    onto the ``protocol`` attribute of the :class:`ServerFactory` class
    in :func:`build_manhole`.
    """
    def __init__(self, portal):
        self.portal = portal

    def __call__(self):
        return TelnetTransport(AuthenticatingTelnetProtocol, self.portal)


class TelnetRealm(object):
    """Wraps together :class:`ITelnetProtocol`,
    :class:`TelnetBootstrapProtocol`, :class:`ServerProtocol` and
    :class:`ColoredManhole` in :meth:`requestAvatar` which will provide
    the interface to the manhole.
    """
    implements(IRealm)
    NAMESPACE = None

    def requestAvatar(self, _, *interfaces):
        if ITelnetProtocol in interfaces:
            return (
                ITelnetProtocol,
                TelnetBootstrapProtocol(
                    ServerProtocol, LoggingManhole,
                    self.NAMESPACE),
                lambda: None)
        raise NotImplementedError


def show(x=NOTSET):
    """Display the data attributes of an object in a readable format"""
    if x is NOTSET:
        print("objects: %s" % TelnetRealm.NAMESPACE.keys())
        return

    print("data attributes of %r" % (x,))

    names = dir(x)
    maxlen = max([0] + [len(n) for n in names])
    for k in names:
        v = getattr(x, k)

        if ismethod(v) or k[:2] == "__" and k[-2:] == "__":
            continue

        if isinstance(v, STRING_TYPES):
            if len(v) > 80 - maxlen - 5:
                v = repr(v[:80 - maxlen - 5]) + "..."

        elif isinstance(v, INTEGER_TYPES) or v is None:
            v = str(v)

        elif isinstance(v, ITERABLE_TYPES):
            v = "%s (%d elements)" % (v, len(v))

        print("%*s : %s" % (maxlen, k, v))
    return x


def manhole_factory(namespace, username, password):
    """
    Produces a factory object which can be used to listen for telnet
    connections to the manhole.
    """
    assert isinstance(namespace, dict)
    assert isinstance(username, STRING_TYPES)
    assert isinstance(password, STRING_TYPES)
    assert TelnetRealm.NAMESPACE is None, "namespace already set"

    # TODO: we should try to use the system to authorize users instead
    checker = InMemoryUsernamePasswordDatabaseDontUse()
    checker.addUser(username, password)

    # Setup the namespace
    namespace = namespace.copy()
    namespace.setdefault("pp", pprint)
    namespace.setdefault("show", show)

    realm = TelnetRealm()
    TelnetRealm.NAMESPACE = namespace
    portal = Portal(realm, [checker])
    factory = ServerFactory()
    factory.protocol = TransportProtocolFactory(portal)
    return factory
