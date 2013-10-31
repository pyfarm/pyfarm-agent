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

"""
HTTP Client
===========

The client library the manager uses to communicate with
the master server.
"""

from httplib import NO_CONTENT
import logging
from urllib import getproxies
from urlparse import urlparse

try:
    import json
except ImportError:
    import simplejson as json

try:
    from collections import OrderedDict
except ImportError:
    from pyfarm.core.backports import OrderedDict

from zope.interface import implements
from twisted.python import log
from twisted.internet import reactor, defer, protocol
from twisted.web.iweb import IBodyProducer
from twisted.web.client import (
    HTTPConnectionPool as _HTTPConnectionPool, Agent, ProxyAgent)
from twisted.web.http_headers import Headers
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.ssl import ClientContextFactory as _ClientContextFactory


# TODO: add documentation
class StringReceiver(protocol.Protocol):
    def __init__(self, deferred):
        self.buffer = ""
        self.deferred = deferred

    def dataReceived(self, data):
        self.buf += data

    def connectionLost(self, reason):
        self.deferred.callback(self.buffer)


class StringProducer(object):
    """
    Implementation of :class:`.IBodyProducer` which
    is used to produce data to send to the server
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class ClientContextFactory(_ClientContextFactory):
    """Context factory used for supporting SSL connections"""
    def getContext(self, hostname=None, port=None):
        return ClientContextFactory.getContext(self)


class HTTPConnectionPool(_HTTPConnectionPool):
    """:class:`._HTTPConnectionPool` object without retries"""
    retryAutomatically = False  # this will be handled internally
    maxPersistentPerHost = 1  # more than one could cause problems at scale


class Client(object):
    """
    Basic HTTP web client which should handle SSL, proxies, and normal
    requests.

    :param string base_uri:
        the base uri will be prepended to all requests

    :param int connect_timeout:
        how longer we should wait on the initial connection to timeout

        .. note::
            this value only applies to non-proxied connections.
    """
    SUPPORTED_METHODS = set(("GET", "POST", "DELETE", "PUT"))
    connection_pool = HTTPConnectionPool()

    def __init__(self, base_uri=None, connect_timeout=30):
        self.base_uri = base_uri
        self.requests = OrderedDict()  # currently active requests

        # setup the agent
        proxies = getproxies()
        if proxies:
            parsed_proxy = urlparse(proxies.get("http") or proxies.get("https"))

            # possible SSL interception, warn about this because
            # it may or may not break the request
            if parsed_proxy.scheme == "https":
                log.msg("proxy scheme is https!", level=logging.WARNING)

            try:
                proxy_server, proxy_port = parsed_proxy.netloc.split(":")
                proxy_port = int(proxy_port)

            except ValueError:
                raise ValueError(
                    "failed to parse server and and port from proxy settings")

            self.construct_agent = lambda: ProxyAgent(
                TCP4ClientEndpoint(reactor, proxy_server, proxy_port),
                reactor=reactor,
                pool=self.connection_pool)

        # non-proxy agent
        else:
            self.construct_agent = lambda: Agent(
                reactor,
                connectTimeout=connect_timeout,
                pool=self.connection_pool)

        # Instance the agent from the lambda function.  This will allow
        # us to 'reconstruct' an agent later on if needed (such as for retries)
        self.agent = self.construct_agent()

    def request(self, method, uri, data=None, headers=None,
                data_dumps=json.dumps):
        """
        Base method which constructs data to pass along to the ``request``
        method of either the :class:`.ProxyAgent` or :class:`.Agent` class

        :param string method:
            The HTTP method to call.

        :param string uri:
            Where ``method`` should be performed.  If a ``base_uri`` was
            provided in :meth:`.__init__` then it will be prepended to
            this value

        :type data: string or list or tuple or dict
        :param data:
            the data to POST or PUT to ``uri``

        :param dict headers:
            The headers to send along with the request

        :param data_dumps:
            the function to use to dump ``data``

        :exception AssertionError:
            Raised if there's some issue with input data.  This can
            occur if either the ``method`` provided is unsupported or
            if the data provided for a value, such as ``headers`` is of
            the wrong type.  TypeError will not be raised here because
            :meth:`.request` is an internal method which usually is not
            called outside of :class:`Client`

        :return:
            returns an instance of :class:`.defer.Deferred`
        """
        assert method in self.SUPPORTED_METHODS, \
            "unsupported method %s" % repr(method)

        # if data was provided dump it t
        if data is not None:
            body_producer = StringProducer(data_dumps(data))
        else:
            body_producer = None

        # prepend the base uri if one was provided
        if self.base_uri is not None:
            uri = self.base_uri + uri

        # create headers
        if headers is not None:
            assert isinstance(headers, dict), "expected dictionary for headers"

            output_headers = Headers()
            for key, value in headers.iteritems():
                if isinstance(value, basestring):
                    value = [value]

                output_headers.addRawHeader(key, value)

            headers = output_headers
        else:
            headers = Headers()

        # TODO: store the request being made so we can retry/check status/etc
        self.requests[(method, uri, headers, data, body_producer)] = None

        # TODO: add deferreds and remaining bits necessary for POST, see example
        deferred = self.agent.request(
            method, uri, headers=headers, bodyProducer=body_producer)

        def handle_resonse(response):
            if response.code == NO_CONTENT:
                d = defer.succeed("")
            else:
                d = defer.Deferred()
                response.delieverBody(StringReceiver(d))

            return d

        # TODO: add errback
        deferred.addCallback(handle_resonse)
        return deferred


