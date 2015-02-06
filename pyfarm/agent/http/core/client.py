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

import os
import json
from collections import namedtuple
from functools import partial
from random import random
from urlparse import urlparse
from uuid import UUID, uuid4

try:
    from httplib import (
        responses, INTERNAL_SERVER_ERROR, BAD_REQUEST, MULTIPLE_CHOICES
    )

except ImportError:  # pragma: no cover
    from http.client import (
        responses, INTERNAL_SERVER_ERROR, BAD_REQUEST, MULTIPLE_CHOICES
    )

try:
    from UserDict import UserDict
except ImportError:  # pragma: no cover
    from collections import UserDict

try:
    import ssl
except ImportError:  # pragma: no cover
    ssl = NotImplemented

try:
    import PyOpenSSL
except ImportError:  # pragma: no cover
    PyOpenSSL = NotImplemented


try:
    import service_identity
except ImportError:  # pragma: no cover
    service_identity = NotImplemented

import treq
try:
    from treq.response import _Response as TQResponse
except ImportError:  # pragma: no cover
    TQResponse = NotImplemented

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol, connectionDone
from twisted.python import log
from twisted.python.failure import Failure
from twisted.web.client import (
    Response as TWResponse, GzipDecoder as TWGzipDecoder, ResponseDone)

from pyfarm.core.enums import STRING_TYPES, NOTSET, INTEGER_TYPES
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import quote_url, json_safe

logger = getLogger("agent.http.client")

# response classes which are allowed to the `response` argument
# to Response.__init__
if TQResponse is not NotImplemented:
    RESPONSE_CLASSES = (TWResponse, TWGzipDecoder, TQResponse)
else:  # pragma: no cover
    RESPONSE_CLASSES = (TWResponse, TWGzipDecoder)

USERAGENT = "PyFarm/1.0 (agent)"
DELAY_NUMBER_TYPES = tuple(list(INTEGER_TYPES) + [float])
HTTP_METHODS = frozenset(("HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"))
HTTP_SCHEMES = frozenset(("http", "https"))


class HTTPLog(object):
    """
    Provides a wrapper around the http logger so requests
    and responses can be logged in a standardized fashion.
    """
    @staticmethod
    def queue(method, url, uid=None):
        """Logs the request we're asking treq to queue"""
        assert isinstance(uid, UUID)
        logger.debug("Queue %s %s (uid: %s)", method, url, uid.hex[20:])

    @staticmethod
    def response(response, uid=None):
        """Logs the return code of a request that treq completed"""
        assert isinstance(response, TQResponse)
        assert isinstance(uid, UUID)

        message = "%s %s %s %s (uid: %s)"
        args = (
            response.code, responses.get(response.code, "UNKNOWN"),
            response.request.method, response.request.absoluteURI,
            uid.hex[20:]
        )

        if (response.code >= INTERNAL_SERVER_ERROR
                or response.code >= BAD_REQUEST):
            logger.error(message, *args)

        else:
            logger.info(message, *args)

        # Return so response can be handled by another callback
        return response

    @staticmethod
    def error(failure, uid=None, method=None, url=None):
        """
        Called when the treq request experiences an error and
        calls the ``errback`` method.
        """
        assert isinstance(uid, UUID)
        assert isinstance(failure, Failure)
        logger.error(
            "%s %s has failed (uid: %s):%s%s",
            method, url, uid.hex[20:], os.linesep, failure.getTraceback()
        )

        # Reraise the error so other code can handle the error
        raise failure


def build_url(url, params=None):
    """
    Builds the full url when provided the base ``url`` and some
    url parameters:

    >>> build_url("/foobar", {"first": "foo", "second": "bar"})
    '/foobar?first=foo&second=bar'
    >>> build_url("/foobar bar/")
    ''/foobar%20bar/'

    :param str url:
        The url to build off of.

    :param dict params:
        A dictionary of parameters that should be added on to ``url``.  If
        this value is not provided ``url`` will be returned by itself.
        Arguments to a url are unordered by default however they will be
        sorted alphabetically so the results are repeatable from call to call.
    """
    assert isinstance(url, STRING_TYPES)

    # append url arguments
    if isinstance(params, (dict, ImmutableDict, UserDict)) and params:
        url += "?" + "&".join([
            "%s=%s" % (key, value)for key, value in sorted(params.items())])

    return quote_url(url)


def http_retry_delay(initial=None, uniform=False, get_delay=random, minimum=1):
    """
    Returns a floating point value that can be used to delay
    an http request.  The main purpose of this is to ensure that not all
    requests are run with the same interval between then.  This helps to
    ensure that if the same request, such as agents coming online, is being
    run on multiple systems they should be staggered a little more than
    they would be without the non-uniform delay.

    :param int initial:
        The initial delay value to start off with before any extra
        calculations are done.  If this value is not provided the value
        provided to ``--http-retry-delay`` at startup will be used.

    :param bool uniform:
        If True then use the value produced by ``get_delay`` as
        a multiplier.

    :param callable get_delay:
        A function which should produce a number to multiply ``delay``
        by.  By default this uses :func:`random.random`

    :param minimum:
        Ensures that the value returned from this function is greater than
        or equal to a minimum value.
    """
    delay = initial
    if initial is None:
        # TODO: provide command line flags for jitter
        delay = config["agent_http_retry_delay"]

    assert isinstance(delay, DELAY_NUMBER_TYPES)  # expect integer or float
    assert isinstance(minimum, DELAY_NUMBER_TYPES)  # expect integer or float
    assert minimum >= 0

    if not uniform:
        delay *= get_delay()

    return max(minimum, delay)


class Request(namedtuple("Request", ("method", "url", "kwargs"))):
    """
    Contains all the information used to perform a request such as the
    ``method``, ``url``, and original keyword arguments (``kwargs``).  These
    values contain the basic information necessary in order to :meth:`retry`
    a request.
    """
    def retry(self, **kwargs):
        """
        When called this will rerun the original request with all
        of the original arguments to :func:`request`

        :param kwargs:
            Additional keyword arguments which should override the original
            keyword argument(s).
        """
        # first take the original keyword arguments
        # and provide overrides
        request_kwargs = self.kwargs.copy()
        request_kwargs.update(kwargs)

        # log and retry the request
        debug_kwargs = request_kwargs.copy()
        url = build_url(self.url, debug_kwargs.pop("params", None))
        logger.debug(
            "Retrying %s %s, kwargs: %r", self.method, url, debug_kwargs)
        return request(self.method, self.url, **request_kwargs)


class Response(Protocol):
    """
    This class receives the incoming response body from a request
    constructs some convenience methods and attributes around the data.

    :param Deferred deferred:
        The deferred object which contains the target callback
        and errback.

    :param response:
        The initial response object which will be passed along
        to the target deferred.

    :param Request request:
        Named tuple object containing the method name, url, headers, and data.
    """
    def __init__(self, deferred, response, request):
        assert isinstance(deferred, Deferred)
        assert isinstance(response, RESPONSE_CLASSES)
        assert isinstance(request, Request)

        # internal attributes
        self._done = False
        self._body = ""
        self._deferred = deferred

        # main public attributes
        self.request = request
        self.response = response

        # convenience attributes constructed
        # from the public attributes
        self.method = self.request.method
        self.url = self.request.url
        self.code = self.response.code
        self.content_type = None

        # consume the response headers
        self.headers = {}
        for header_key, header_value in response.headers.getAllRawHeaders():
            if len(header_value) == 1:
                header_value = header_value[0]
            self.headers[header_key] = header_value

        # determine the content type
        if "Content-Type" in self.headers:
            self.content_type = self.headers["Content-Type"]

    def data(self):
        """
        Returns the data currently contained in the buffer.

        :raises RuntimeError:
            Raised if this method id called before all data
            has been received.
        """
        if not self._done:
            raise RuntimeError("Response not yet received.")
        return self._body

    def json(self, loader=json.loads):
        """
        Returns the json data from the incoming request

        :raises RuntimeError:
            Raised if this method id called before all data
            has been received.

        :raises ValueError:
            Raised if the content type for this request is not
            application/json.
        """
        if not self._done:
            raise RuntimeError("Response not yet received.")
        elif self.content_type != "application/json":
            raise ValueError("Not an application/json response.")
        else:
            return loader(self._body)

    def dataReceived(self, data):
        """
        Overrides :meth:`.Protocol.dataReceived` and appends data
        to ``_body``.
        """
        self._body += data

    def connectionLost(self, reason=connectionDone):
        """
        Overrides :meth:`.Protocol.connectionLost` and sets the ``_done``
        when complete.  When called with :class:`.ResponseDone` for ``reason``
        this method will call the callback on ``_deferred``
        """
        if reason.type is ResponseDone:
            self._done = True
            url = build_url(self.request.url, self.request.kwargs.get("params"))
            code_text = responses.get(self.code, "UNKNOWN")
            logger.debug(
                "%s %s %s %s, body: %s",
                self.code, code_text, self.request.method, url, self._body)
            self._deferred.callback(self)
        else:
            self._deferred.errback(reason)


def request(method, url, **kwargs):
    """
    Wrapper around :func:`treq.request` with some added arguments
    and validation.

    :param str method:
        The HTTP method to use when making the request.

    :param str url:
        The url this request will be made to.

    :type data: str, list, tuple, set, dict
    :keyword data:
        The data to send along with some types of requests
        such as ``POST`` or ``PUT``

    :keyword dict headers:
        The headers to send along with the request to
        ``url``.  Currently only single values per header
        are supported.

    :keyword function callback:
        The function to deliver an instance of :class:`Response`
        once we receive and unpack a response.

    :keyword function errback:
        The function to deliver an error message to.  By default
        this will use :func:`.log.err`.

    :keyword class response_class:
        The class to use to unpack the internal response.  This is mainly
        used by the unittests but could be used elsewhere to add some
        custom behavior to the unpack process for the incoming response.

    :raises NotImplementedError:
        Raised whenever a request is made of this function that we can't
        implement such as an invalid http scheme, request method or a problem
        constructing data to an api.
    """
    assert isinstance(url, STRING_TYPES)
    direct = kwargs.pop("direct", False)

    # We only support http[s]
    parsed_url = urlparse(url)
    if not parsed_url.hostname:
        raise NotImplementedError("No hostname present in url")

    if not parsed_url.path:
        raise NotImplementedError("No path provided in url")

    if not direct:
        original_request = Request(
            method=method, url=url, kwargs=ImmutableDict(kwargs.copy()))

    # Headers
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", ["application/json"])
    headers.setdefault("User-Agent", [USERAGENT])

    # Twisted requires lists for header values
    for header, value in headers.items():
        if isinstance(value, STRING_TYPES):
            headers[header] = [value]

        elif isinstance(value, (list, tuple, set)):
            continue

        else:
            raise NotImplementedError(
                "Cannot handle header values with type %s" % type(value))

    # Handle request data
    data = kwargs.pop("data", NOTSET)
    if isinstance(data, dict):
        data = json_safe(data)

    if (data is not NOTSET and
            headers["Content-Type"] == ["application/json"]):
        data = json.dumps(data)

    elif data is not NOTSET:
        raise NotImplementedError(
            "Don't know how to dump data for %s" % headers["Content-Type"])

    # prepare keyword arguments
    kwargs.update(
        headers=headers,
        persistent=config["agent_http_persistent_connections"])

    if data is not NOTSET:
        kwargs.update(data=data)

    if direct:
        # We don't support these with direct request
        # types.
        assert "callback" not in kwargs
        assert "errback" not in kwargs
        assert "response_class" not in kwargs

        # Construct the request and attach some loggers
        # to callback/errback.
        uid = uuid4()
        treq_request = treq.request(method, url, **kwargs)
        treq_request.addCallback(HTTPLog.response, uid=uid)
        treq_request.addErrback(HTTPLog.error, uid=uid, method=method, url=url)
        return treq_request
    else:
        callback = kwargs.pop("callback", None)
        errback = kwargs.pop("errback", log.err)
        response_class = kwargs.pop("response_class", Response)

        # check assumptions for keywords
        assert callback is not None, "callback not provided"
        assert callable(callback) and callable(errback)
        assert data is NOTSET or \
               isinstance(data, tuple(list(STRING_TYPES) + [dict, list]))

        def unpack_response(response):
            deferred = Deferred()
            deferred.addCallback(callback)

            # Deliver the body onto an instance of the response
            # object along with the original request.  Finally
            # the request and response via an instance of `Response`
            # to the outer scope's callback function.
            response.deliverBody(
                response_class(deferred, response, original_request))

            return deferred

        debug_kwargs = kwargs.copy()
        debug_url = build_url(url, debug_kwargs.pop("params", None))
        logger.debug(
            "Queued %s %s, kwargs: %r", method, debug_url, debug_kwargs)

        try:
            deferred = treq.request(method, quote_url(url), **kwargs)
        except NotImplementedError:  # pragma: no cover
            logger.error(
                "Attempting to access a url over SSL but you don't have the "
                "proper libraries installed.  Please install the PyOpenSSL and "
                "service_identity Python packages.")
            raise

        deferred.addCallback(unpack_response)
        deferred.addErrback(errback)

        return deferred


# Old style requests.  These are in place so we can
# improve on the agent without having to rewrite
# everything at once.
# TODO: remove these once everything is converted to *_direct
head = partial(request, "HEAD")
get = partial(request, "GET")
post = partial(request, "POST")
put = partial(request, "PUT")
patch = partial(request, "PATCH")
delete = partial(request, "DELETE")

# New style requests which we can utilize on a case
# by case basis.
# TODO: rename these to the above functions once everything is using them
head_direct = partial(request, "HEAD", direct=True)
get_direct = partial(request, "GET", direct=True)
post_direct = partial(request, "POST", direct=True)
put_direct = partial(request, "PUT", direct=True)
patch_direct = partial(request, "PATCH", direct=True)
delete_direct = partial(request, "DELETE", direct=True)
