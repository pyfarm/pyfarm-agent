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

from httplib import UNSUPPORTED_MEDIA_TYPE, BAD_REQUEST

try:
    import json
except ImportError:
    import simplejson as json


from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.python.compat import intToBytes

from pyfarm.agent.http.resource import Resource, JSONError

number = (int, float, long)


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

        if not isinstance(content, dict):
            self.error(
                request, "invalid type for post data, expected dictionary")
            return

        errors = []

        # basic top level keys that are required
        for key in ("project", "job", "task"):
            if key not in content or not isinstance(content[key], int):
                errors.append("%s must be an integer" % repr(key))

        # top level dictionaries that are required
        for key in ("jobtype", "frame"):
            if key not in content or not isinstance(content[key], dict):
                errors.append("%s must be a dictionary" % repr(key))

            else:
                if key == "jobtype":
                    for subkey in ("import", "cmd", "args"):
                        if subkey not in content[key] or not \
                        isinstance(content[key][subkey], basestring):
                            errors.append(
                                "%s in %s must be a string" % (
                                    repr(subkey), repr(key)))

                elif key == "frame":
                    for subkey in ("start_frame", "end_frame", "by_frame"):
                        if subkey not in content[key] or not \
                        isinstance(content[key][subkey], (int, long, float)):
                            errors.append(
                                "%s in %s must be an int, long, or float" % (
                                    repr(subkey), repr(key)))

        if "user" in content and not isinstance(content["user"], basestring):
            errors.append("'user' must be a string")

        if "env" in content and not isinstance(content["env"], dict):
            errors.append("'env' must be a dictionary")

        elif "env" in content:
            for key, value in content["env"].iteritems():
                if not isinstance(key, basestring) or not \
                    isinstance(value, basestring):
                    errors.append(
                        "'env' only supports string key value pairs")
                    break

        if errors:
            self.error(request, errors)
            return

        # TODO: schedule
        # TODO: populate missing data and their defaults
        request.finish()

    def decode_post_data(self, args):
        """ensures the data is real json"""
        request, content = args
        try:
            content = json.loads(content)
        except ValueError:
            self.error(request, "json decode failed")
        else:
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