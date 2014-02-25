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

import treq
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol, connectionDone
from twisted.python import log
from twisted.web.client import ResponseDone

from pyfarm.core.enums import STRING_TYPES, NOTSET
from pyfarm.core.logger import getLogger

Request = namedtuple(
    "Request",
    ["method", "uri", "headers", "data"])

logger = getLogger("agent.http")


class Response(Protocol):
    """
    This class receives the incoming response body from a request
    constructs some convenience methods and attributes around the data.

    :param deferred:
        The deferred object which contains the target callback
        and errback.

    :param response:
        The initial response object which will be passed along
        to the target deferred.

    :param request:
        Named tuple object containing the method name, uri, headers, and data.
    """
    def __init__(self, deferred, response, request):
        # internal attributes
        self._done = False
        self._body = ""
        self._deferred = deferred

        # main public attributes
        self.request = request
        self.response = response

        # convenience attributes constructed
        # from the public attributes
        self.uri = self.request.uri
        self.headers = self.request.headers
        self.code = self.response.code

        if "Content-Type" not in self.headers:
            self.content_type = None
        else:
            content_types = self.headers["Content-Type"]
            self.content_type = \
                content_types[0] if len(content_types) == 1 else content_types

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
            logger.debug(
                "%s %s (code: %s: body: %r)",
                self.request.method, self.request.uri, self.code, self._body)
            self._deferred.callback(self)
        else:
            self._deferred.errback(reason)


def request(method, uri, **kwargs):
    """
    Wrapper around :func:`treq.request` with some added arguments
    and validation.

    :param str method:
        The HTTP method to use when making the request.

    :param str uri:
        The URI this request will be made to.

    :type data: str, list, tuple, set, dict
    :keyword data:
        The data to send along with some types of requests
        such as ``POST`` or ``PUT``

    :keyword dict headers:
        The headers to send along with the request to
        ``uri``.  Currently only single values per header
        are supported.

    :keyword function callback:
        The function to deliver an instance of :class:`Response`
        once we receive and unpack a response.

    :keyword function errback:
        The function to deliver an error message to.  By default
        this will use :func:`.log.err`.
    """
    # check assumptions for arguments
    assert method in ("HEAD", "GET", "POST", "PUT", "PATCH", "DELETE")
    assert isinstance(uri, STRING_TYPES) and uri

    data = kwargs.pop("data", NOTSET)
    headers = kwargs.pop("headers", {})
    callback = kwargs.pop("callback", None)
    errback = kwargs.pop("errback", log.err)

    # check assumptions for keywords
    assert callable(callback) and callable(errback)
    assert data is NOTSET or \
           isinstance(data, tuple(list(STRING_TYPES) + [dict, list]))

    # add our default headers
    headers.setdefault("Content-Type", ["application/json"])
    headers.setdefault("User-Agent", ["PyFarm (agent) 1.0"])

    # ensure all values in the headers are lists (needed by Twisted)
    for header, value in headers.items():
        if isinstance(value, STRING_TYPES):
            headers[header] = [value]

        # for our purposes we should not expect headers with more
        # than one value for now
        assert len(headers[header]) == 1

    def unpack_response(response):
        deferred = Deferred()
        deferred.addCallback(callback)

        # Deliver the body onto an instance of the response
        # object along with the original request.  Finally
        # the request and response via an instance of `Response`
        # to the outer scope's callback function.
        response.deliverBody(
            Response(
                deferred, response,
                Request(method=method, uri=uri, data=data, headers=headers)))

        return deferred

    # prepare to send the data
    request_data = data  # so we don't modify the original data
    if request_data is not NOTSET and \
                    headers["Content-Type"] == ["application/json"]:
        request_data = json.dumps(data)

    elif data is not NOTSET:
        raise NotImplementedError(
            "Don't know how to dump data for %s" % headers["Content-Type"])

    logmsg = "Queued request `%s %s`" % (method, uri)

    # prepare keyword arguments
    kwargs.update(headers=headers)
    if request_data is not NOTSET:
        kwargs.update(data=request_data)
        logmsg += " data: %r" % data

    # setup the request from treq
    logger.debug(logmsg)
    deferred = treq.request(method, uri, **kwargs)
    deferred.addCallback(unpack_response)
    deferred.addErrback(errback)

    return deferred


head = partial(request, "GET")
get = partial(request, "GET")
post = partial(request, "POST")
put = partial(request, "PUT")
patch = partial(request, "PATCH")
delete = partial(request, "DELETE")
