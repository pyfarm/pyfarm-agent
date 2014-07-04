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

import json
from collections import namedtuple
from functools import partial
from random import random

try:
    from httplib import responses
except ImportError:  # pragma: no cover
    from http.client import responses

try:
    from UserDict import UserDict
except ImportError:  # pragma: no cover
    from collections import UserDict

import treq
try:
    from treq.response import _Response as TQResponse
except ImportError:  # pragma: no cover
    TQResponse = NotImplemented

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol, connectionDone
from twisted.python import log
from twisted.web.client import (
    Response as TWResponse, GzipDecoder as TWGzipDecoder, ResponseDone)

from pyfarm.core.enums import STRING_TYPES, NOTSET, INTEGER_TYPES
from pyfarm.core.utility import ImmutableDict
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import quote_url

logger = getLogger("agent.http.client")

# response classes which are allowed to the `response` argument
# to Response.__init__
if TQResponse is not NotImplemented:
    RESPONSE_CLASSES = (TWResponse, TWGzipDecoder, TQResponse)
else:  # pragma: no cover
    RESPONSE_CLASSES = (TWResponse, TWGzipDecoder)

USERAGENT = "PyFarm/1.0 (agent)"
DELAY_NUMBER_TYPES = tuple(list(INTEGER_TYPES) + [float])


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
        If True then use the value produced by :param:`get_delay` as
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
    """
    # check assumptions for arguments
    assert method in ("HEAD", "GET", "POST", "PUT", "PATCH", "DELETE")
    assert isinstance(url, STRING_TYPES) and url

    original_request = Request(
        method=method, url=url, kwargs=ImmutableDict(kwargs.copy()))
    data = kwargs.pop("data", NOTSET)
    headers = kwargs.pop("headers", {})
    callback = kwargs.pop("callback", None)
    errback = kwargs.pop("errback", log.err)
    response_class = kwargs.pop("response_class", Response)

    # check assumptions for keywords
    assert callback is not None, "callback not provided"
    assert callable(callback) and callable(errback)
    assert data is NOTSET or \
           isinstance(data, tuple(list(STRING_TYPES) + [dict, list]))

    # add our default headers
    headers.setdefault("Content-Type", ["application/json"])
    headers.setdefault("User-Agent", [USERAGENT])

    # ensure all values in the headers are lists (needed by Twisted)
    for header, value in headers.items():
        if isinstance(value, STRING_TYPES):
            headers[header] = [value]

        # for our purposes we should not expect headers with more
        # than one value for now
        elif isinstance(value, (list, tuple, set)):
            assert len(value) == 1

        else:
            raise NotImplementedError(
                "cannot handle header values with type %s" % type(value))

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

    # prepare to send the data
    request_data = data  # so we don't modify the original data
    if request_data is not NOTSET and \
                    headers["Content-Type"] == ["application/json"]:
        request_data = json.dumps(data)

    elif data is not NOTSET:
        raise NotImplementedError(
            "Don't know how to dump data for %s" % headers["Content-Type"])

    # prepare keyword arguments
    kwargs.update(
        headers=headers,

        # Controls if the http connection should be persistent or
        # not.  Generally this should always be True because the connection
        # self-terminates after a short period of time anyway.  We
        # have setting for it however because the tests need this value
        # to be False.
        persistent=config.get("persistent-http-connections", False))

    if request_data is not NOTSET:
        kwargs.update(data=request_data)

    debug_kwargs = kwargs.copy()
    debug_url = build_url(url, debug_kwargs.pop("params", None))
    logger.debug(
        "Queued %s %s, kwargs: %r", method, debug_url, debug_kwargs)

    try:
        deferred = treq.request(method, quote_url(url), **kwargs)
    except NotImplementedError:
        logger.error(
            "Attempting to access a url over SSL but you don't have the "
            "proper libraries installed.  Please install the PyOpenSSL and "
            "service_identity Python packages.")
        raise

    deferred.addCallback(unpack_response)
    deferred.addErrback(errback)

    return deferred


head = partial(request, "HEAD")
get = partial(request, "GET")
post = partial(request, "POST")
put = partial(request, "PUT")
patch = partial(request, "PATCH")
delete = partial(request, "DELETE")
