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
from httplib import BAD_REQUEST, OK, MULTIPLE_CHOICES

from collections import namedtuple

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

import treq
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol, connectionDone
from twisted.web.client import ResponseDone, ResponseFailed

from pyfarm.core.enums import STRING_TYPES, NOTSET
from pyfarm.core.logger import getLogger

RequestData = namedtuple(
    "RequestData",
    ["method", "uri", "headers", "data"])

logger = getLogger("agent.http")


class ResponseReceiver(Protocol):
    """Simple class used to receive and process response objects"""
    def __init__(self, deferred, response):
        self.buffer = ""
        self.deferred = deferred
        self.response = response

    def dataReceived(self, data):
        self.buffer += data

    def connectionLost(self, reason=connectionDone):
        # TODO: add statsd for response

        # TODO: look at response for code
        if reason.type is ResponseDone:
            self.deferred.callback(self.buffer)

        # if the headers specify json content, convert
        # # it before returning the data
        # content_types = \
        #     [] or self.response.headers.getRawHeaders("Content-Type")
        #
        # for content_type in content_types:
        #     if "application/json" in content_type:
        #         self.buffer = json.loads(self.buffer)
        #         break
        #
        # # there's a problem with the incoming response, the buffer
        # # should contain the error so pass it to the errback
        # if self.response.code >= BAD_REQUEST:
        #     self.deferred.errback(reason)
        #
        # elif OK <= self.response.code < MULTIPLE_CHOICES:
        #     self.deferred.callback(self.buffer)
        #
        # # nothing left to do, call the callback (success)
        # elif reason.type is ResponseDone:
        #     self.deferred.callback(self.buffer)
        #
        # # we're not done and we don't have a specific error code
        # else:
        #     self.deferred.errback(reason)

def request(method, uri, **kwargs):
    """
    Wrapper around :func:`treq.request` with some added arguments
    and validation.
    """
    # pop off our arguments
    data = kwargs.pop("data", NOTSET)
    headers = kwargs.pop("headers", {})
    response_success = kwargs.pop("response_success", None)
    request_failure = kwargs.pop("request_failure", None)

    assert method in ("HEAD", "GET", "POST", "PUT", "PATCH", "DELETE")
    assert isinstance(uri, STRING_TYPES) and uri
    assert callable(response_success)
    assert request_failure is None or callable(request_failure)
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

    # encode the data (if any)
    if data is not NOTSET and headers["Content-Type"] == ["application/json"]:
        data = json.dump(data)

    elif data is not NOTSET:
        raise NotImplementedError(
            "don't know how to dump data for %s" % headers["Content-Type"])

    # prepare keyword arguments for treq
    kwargs.update(headers=headers)
    if data is not NOTSET:
        kwargs.update(data=data)

    def unpack_response(response):
        request = RequestData(
            method=method, uri=uri, headers=headers, data=data)
        deferred = Deferred()
        deferred.addCallback(response_success, request, response)
        response.deliverBody(ResponseReceiver(deferred, response))
        return deferred

    treq_deferred = treq.request(method, uri, **kwargs)
    treq_deferred.addCallback(unpack_response)
    if request_failure is not None:
        treq_deferred.addErrback(request_failure)

    return treq_deferred


def get(uri, **kwargs):
    return request("GET", uri, **kwargs)