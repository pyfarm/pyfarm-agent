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
Assign
------

Main module for handling incoming assignments.  This code does
not start new work but knows how to pass it along to the next
component and respond to requests.
"""

import logging
from functools import partial
from httplib import UNSUPPORTED_MEDIA_TYPE, BAD_REQUEST

try:
    import json
except ImportError:
    import simplejson as json

from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.python import log
from twisted.python.compat import intToBytes
from twisted.web.server import NOT_DONE_YET
from voluptuous import (
    Error, Invalid, Schema, Required, Optional, Any, All, Range)

from pyfarm.core.enums import JobTypeLoadMode
from pyfarm.agent.http.resource import Resource, JSONError


class PostProcessedSchema(Schema):
    """
    Subclass of :class:`.Schema` which does some additional
    processing on the dictionary
    """
    @staticmethod
    def string_keys_and_values(data):
        if not isinstance(data, dict):
            raise Invalid("invalid type")

        for key, value in data.iteritems():
            if not isinstance(key, basestring):
                raise Invalid("expected string for env key '%s'" % key)

            if not isinstance(value, basestring):
                raise Invalid("expected string for env value '%s'" % value)

        return data

    def __call__(self, data):
        data = super(PostProcessedSchema, self).__call__(data)

        # set default frames
        frame_data = data["frame"]
        frame_data.setdefault("end", data["frame"]["start"])
        frame_data.setdefault("by_frame", 1)

        return data


class Assign(Resource):
    """
    Provides public access so work can be assigned to the agent.  This
    resource only supports ``GET`` and ``POST``.  Using ``GET`` on this
    resource will describe what should be used for a ``POST`` request.

    .. note::
        Results from a ``GET`` request are intended to be used as a guide
        for building input to ``POST``.  Do not use ``GET`` for non-human
        consumption.
    """
    TEMPLATE = "pyfarm/assign.html"
    NUMBER_TYPES = Any(int, float, long)

    try:
        STRING_TYPES = basestring
    except NameError:
        STRING_TYPES = unicode

    SCHEMA = PostProcessedSchema({
        Required("project"): int,
        Required("job"): int,
        Required("task"): int,
        Required("jobtype"): {
            Required("load_type"): All(
                STRING_TYPES, Any(*list(JobTypeLoadMode))),
            Required("load_from"): STRING_TYPES,
            Required("cmd"): STRING_TYPES,
            Required("args"): STRING_TYPES},
        Required("frame"): {
            Required("start"): NUMBER_TYPES,
            Optional("end"): NUMBER_TYPES,
            Optional("by"): NUMBER_TYPES},
        Optional("resources"): {
            Optional("cpus"): All(int, Range(min=1)),
            Optional("ram_warning"): All(int, Range(min=1)),
            Optional("ram_max"): All(int, Range(min=1))},
        Optional("user"): STRING_TYPES,
        Optional("data"): Any(
            dict, list, STRING_TYPES, int, float, long, type(None)),
        Optional("env"): PostProcessedSchema.string_keys_and_values})

    def __init__(self, config):
        Resource.__init__(self, config)
        self.log = partial(log.msg, system=self.__class__.__name__)
        self.info = partial(self.log, level=logging.INFO)
        self.debug = partial(self.log, level=logging.DEBUG)
    
    def get(self, request):
        # write out the results from the template back
        # to the original request
        def cb(content):
            request.write(content)
            request.setResponseCode(200)
            request.finish()

        deferred = self.template.render(uri=request.prePathURL())
        deferred.addCallback(cb)

        return NOT_DONE_YET

    def error(self, request, error, code=BAD_REQUEST):
        """writes an error to the incoming request"""
        body = json.dumps((code, error))
        request.setResponseCode(code)
        request.setHeader(b"content-type", b"application/json")
        request.setHeader(b"content-length", intToBytes(len(body)))
        request.write(body)
        request.finish()

    def validate_post_data(self, args):
        request, content = args

        try:
            data = self.SCHEMA(content)
        except Error, e:
            self.error(request, str(e))
            return
        else:
            request.finish()

        # TODO: start internal assignment
        deferred = deferLater(reactor, )

    def decode_post_data(self, args):
        """ensures the data is real json"""
        request, content = args

        if not content:
            self.info("no data provided in assignment POST")
            self.error(request, "no data provided")
            return

        try:
            content = json.loads(content)
        except ValueError:
            self.info("failed to decode incoming assignment data")
            self.error(request, "json decode failed")
        else:
            self.info("incoming assignment data: %s" % repr(content))
            deferred = deferLater(reactor, 0, lambda: [request, content])
            deferred.addCallback(self.validate_post_data)

    def unpack_post_data(self, request):
        """read in all data from the request"""
        content = request.content.read()
        deferred = deferLater(reactor, 0, lambda: [request, content])
        deferred.addCallback(self.decode_post_data)

    def post(self, request):
        # check content type before we do anything else
        content_type = request.requestHeaders.getRawHeaders("Content-Type")
        if "application/json" not in content_type:
            raise JSONError(
                UNSUPPORTED_MEDIA_TYPE, "only application/json is supported")

        # handle the request with a series of deferred objects
        # so we block for shorter periods of time
        deferred = deferLater(reactor, 0, lambda: request)
        deferred.addCallback(self.unpack_post_data)

        return NOT_DONE_YET