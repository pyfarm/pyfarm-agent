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

import logging
from httplib import NO_CONTENT, RESET_CONTENT
from functools import partial
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
    HTTPConnectionPool as _HTTPConnectionPool, Agent, ProxyAgent, ResponseDone)
from twisted.web.http_headers import Headers
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.ssl import ClientContextFactory as _ClientContextFactory


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


class WebClient(object):
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

    def __init__(self, base_uri=None, connect_timeout=10):
        self.base_uri = base_uri
        self.requests = OrderedDict()  # currently active requests
        self.log = partial(log.msg, system=self.__class__.__name__)

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
                pool=HTTPConnectionPool(reactor))

        # non-proxy agent
        else:
            self.construct_agent = lambda: Agent(
                reactor,
                connectTimeout=connect_timeout,
                pool=HTTPConnectionPool(reactor))

        # Instance the agent from the lambda function.  This will allow
        # us to 'reconstruct' an agent later on if needed (such as for retries)
        self.agent = self.construct_agent()

    def handle_response(self, response):
        if response.code in (NO_CONTENT, RESET_CONTENT):
            return defer.succeed("")

        elif response.code >= 400:
            # TODO: add statsd codes
            return defer.fail(response)

        else:
            class SimpleReceiver(protocol.Protocol):
                def __init__(self, deferred):
                    self.buffer = ""
                    self.deferred = deferred

                def dataReceived(self, data):
                    self.buffer += data

                def connectionLost(self, reason):
                    # TODO: add statsd for response

                    # if the headers specify json content, convert
                    # it before returning the data
                    content_types = [] or response.headers.getRawHeaders(
                        "content-type")
                    for content_type in content_types:
                        if "application/json" in content_type:
                            self.buffer = json.loads(self.buffer)
                            break

                    # check the reason type 'the twisted way'...
                    if reason.type is ResponseDone:
                        self.deferred.callback(self.buffer)
                    else:
                        self.deferred.errback(reason)

            d = defer.Deferred()
            response.deliverBody(SimpleReceiver(d))
            return d

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
            called outside of :class:`WebClient`

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
        log_msg = "%s %s" % (method, uri)
        if data is not None:
            log_msg += " data: %s" % data

        if headers is not None:
            log_msg += " headers: %s" % headers

        self.log(log_msg)
        deferred = self.agent.request(
            method, uri, headers=headers, bodyProducer=body_producer)

        # add in our response handler which will help
        # to fire the proper callback/errback
        deferred.addCallback(self.handle_response)

        return deferred

    # TODO: documentation
    def post(self, uri, data=None, headers=None):
        return self.request("POST", uri, data=data, headers=headers)

    # TODO: documentation
    def get(self, uri, headers=None):
        return self.request("GET", uri, headers=headers)

    # TODO: documentation
    def put(self, uri, data=None, headers=None):
        return self.request("PUT", uri, data=data, headers=headers)

    # TODO: documentation
    def delete(self, uri, headers=None):
        return self.request("DELETE", uri, headers=headers)


# TODO: documentation
def post(uri, data=None, headers=None):
    return WebClient().post(uri, data=data, headers=headers)


# TODO: documentation
def get(uri, headers=None):
    return WebClient().get(uri, headers=headers)


# TODO: documentation
def put(uri, data=None, headers=None):
    return WebClient().put(uri, data=data, headers=headers)


# TODO: documentation
def delete(uri, headers=None):
    return WebClient().delete(uri, headers=headers)
