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
Resource
--------

Base resources which can be used to build top leve
documents, pages, or other types of data for the web.
"""

from functools import partial
from httplib import responses, BAD_REQUEST, INTERNAL_SERVER_ERROR
from os.path import isdir

try:
    import json
except ImportError:
    import simplejson as json

from twisted.web.server import Request as _Request, NOT_DONE_YET
from twisted.python import log
from twisted.python.compat import nativeString, intToBytes
from twisted.web.error import Error, UnsupportedMethod
from twisted.web.resource import Resource as _Resource

import txtemplate


class Request(_Request):
    """
    Overrides the default :class:`._Request` so we can produce
    custom errors
    """
    def processingFailed(self, reason):
        if reason.type is JSONError:
            log.err(reason)
            body = json.dumps((reason.value.status, reason.value.message))
            self.setResponseCode(reason.value.status)
            self.setHeader(b"content-type", b"application/json")
            self.setHeader(b"content-length", intToBytes(len(body)))
            self.write(body)
            self.finish()
        else:
            return _Request.processingFailed(self, reason)


class Resource(_Resource):
    """
    basic subclass of :class:`._Resource` for passing requests to
    specific methods
    """
    CONTENT_TYPE = None
    CONTENT_TYPE_SETS_RESPONSE_HEADER = True
    TEMPLATE = NotImplemented
    templates = None

    def __init__(self, config):
        _Resource.__init__(self)
        self.config = config

        # create the template loader at the class level so all
        # children can share it
        if Resource.templates is None:
            if "html-templates" not in self.config:
                raise KeyError("`html-templates` not present in config")

            elif not isdir(self.config["html-templates"]):
                raise OSError(
                    "`html-templates` %s does not exist"
                    % self.config["html-templates"])

            else:
                Resource.templates = txtemplate.Jinja2TemplateLoader(
                    self.config["html-templates"])

        # CONTENT_TYPE if provided should normally be a set object
        # so misordered headers won't cause problems.  Of course
        # if we need ordered headers, __init__ can be replaced.
        if self.CONTENT_TYPE is not None and not \
                isinstance(self.CONTENT_TYPE, set):
            raise TypeError("expected a set object for `CONTENT_TYPE")

    @property
    def template(self):
        """
        loads the template provided by ``TEMPLATE`` using the loader object
        """
        if self.TEMPLATE is NotImplemented:
            raise NotImplementedError("you must set `TEMPLATE` first")

        return self.templates.load(self.TEMPLATE)

    def render(self, request):
        """
        Provides similar behavior to :class:`flask.view.MethodView`.
        while processing like :meth:`._Resource.render`
        """
        # Didn't find the method?  Find what methods are supported
        # and tell the client about them.
        request_method = nativeString(request.method).lower()
        instance_method = getattr(self, request_method, None)
        if instance_method is None:
            raise UnsupportedMethod(
                [name for name in ("get", "post", "put", "delete", "head")
                 if hasattr(self, name)])

        # CONTENT_TYPE was specified so the incoming request should
        # both specify the header and it should match our expected content type
        if self.CONTENT_TYPE is not None:
            if not request.requestHeaders.hasHeader("content-type"):
                raise Error(BAD_REQUEST, "missing content-type header")
            else:
                headers = set(
                    request.requestHeaders.getRawHeaders("content-type"))

                # check the headers against the requested headers
                if headers != self.CONTENT_TYPE:
                    raise Error(
                        BAD_REQUEST, "invalid headers for this resource")

            if self.CONTENT_TYPE_SETS_RESPONSE_HEADER:
                request.responseHeaders.setRawHeaders(
                    "content-type", list(self.CONTENT_TYPE))

        return self.handle(instance_method, request)

    def handle(self, instance_method, request):
        """
        Method which receives an instance method from :meth:`render` as well
        as the incoming request.  This is provided so a subclass can change
        both the calling behavior as well as the results.
        """
        return instance_method(request)


class JSONResource(Resource):
    """
    Subclass of :class:`.Resource` but tailored for sending and
    responding with json
    """
    CONTENT_TYPE = set(["application/json"])

    def __init__(self, config=None):
        Resource.__init__(self, config=config)

        if self.config.get("pretty-json", False):
            self.dumps = partial(json.dumps, indent=4)
        else:
            self.dumps = json.dumps

    def handle(self, instance_method, request):
        """
        Run :func:`json.loads` on ``request`` before passing it along to
        ``instance_method``.  :func:`json.dumps` will also be run on the result
        from ``instance_method`` before returning.
        """
        content = request.content.read()
        try:
            data = json.loads(content)
        except ValueError, e:
            raise Error(BAD_REQUEST, str(e))
        else:
            result = instance_method(data, request)

            if result == NOT_DONE_YET:
                return result

            try:
                return self.dumps(result)
            except ValueError:
                raise Error(INTERNAL_SERVER_ERROR, "failed to dump response")


class JSONError(Error):
    """
    Replacement for Twisted's :class:`.Error` class that's specifically
    geared towards to providing an error in a json style format.
    """
    def __init__(self, code, message, response=None):
        assert code in responses, "invalid code for a response"
        self.status = code
        self.message = message
        self.response = response
