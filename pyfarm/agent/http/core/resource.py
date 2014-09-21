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

from json import loads

try:
    from httplib import (
        responses, NOT_FOUND, BAD_REQUEST, UNSUPPORTED_MEDIA_TYPE,
        METHOD_NOT_ALLOWED)
except ImportError:  # pragma: no cover
    from http.client import (
        responses, NOT_FOUND, BAD_REQUEST, UNSUPPORTED_MEDIA_TYPE,
        METHOD_NOT_ALLOWED)

try:
    from itertools import ifilter as filter_
except ImportError:  # pragma: no cover
    filter_ = filter

try:
    from itertools import imap as map_
except ImportError:  # pragma: no cover
    map_ = map

from twisted.web.server import NOT_DONE_YET
from twisted.web.resource import Resource as _Resource
from voluptuous import Invalid, Schema

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.http.core import template
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.http.resource")


class Resource(_Resource):
    """
    Basic subclass of :class:`._Resource` for passing requests to
    specific methods.  Unlike :class:`._Resource` however this will
    will also handle:

        * rewriting of request objects
        * templating
        * content type discovery and validation
        * unpacking of request data
        * rerouting of request to specific internal methods
    """
    TEMPLATE = NotImplemented
    CONTENT_TYPES = set(["text/html", "application/json"])
    LOAD_DATA_FOR_METHODS = set(["POST", "PUT"])

    # Used by APIResource
    SCHEMAS = {}

    def __init__(self):
        _Resource.__init__(self)
        assert isinstance(self.CONTENT_TYPES, set)

    @property
    def template(self):
        """
        Loads the template provided but the partial path in ``TEMPLATE`` on
        the class.
        """
        if self.TEMPLATE is NotImplemented:
            raise NotImplementedError("You must set `TEMPLATE` first")
        return template.load(self.TEMPLATE)

    @property
    def methods(self):
        """A set containing all the methods this resource implements."""
        methods = set()
        for method in ("get", "post", "put", "delete", "head"):
            attribute_count = 0
            for attribute_name in (method, "render_%s" % method.upper()):
                attribute = getattr(self, attribute_name, None)
                if attribute is not None:
                    attribute_count += 1
                    methods.add(method)

            if attribute_count == 2:
                raise ValueError(
                    "%s has both `%s` and `%s` methods" % (
                        self.__class__.__name__,
                        method, "render_%s" % method.upper()))

        return methods

    def content_types(self, request, default=None):
        """Returns the content type(s) present in the request"""
        headers = request.requestHeaders.getRawHeaders("content-type")
        if isinstance(default, STRING_TYPES):
            default = [default]
        elif default is None:
            default = []
        return set(headers) if headers is not None else set(default)

    def putChild(self, path, child):
        """
        Overrides the builtin putChild() so we can return the results for
        each call and use them externally
        """
        _Resource.putChild(self, path, child)
        return child

    def error(self, request, code, message):
        """
        Writes the proper out an error response message depending on the
        content type in the request
        """
        content_types = self.content_types(request, default="text/html")
        logger.error(message)

        if "text/html" in content_types:
            request.setResponseCode(code)
            html_error = template.load("error.html")
            deferred = html_error.render(
                code=code, code_msg=responses[code], message=message)
            deferred.addCallback(request.write).addCallback(
                lambda _: request.finish())

        elif "application/json" in content_types:
            request.setResponseCode(code)
            request.write({"error": message})
            request.finish()

        else:
            request.setResponseCode(UNSUPPORTED_MEDIA_TYPE)
            request.write(
                {"error": "Can only handle text/html or application/json here"})
            request.finish()

    def render(self, request):
        # make sure that the requested content type is supported
        content_type = self.content_types(request, default=["text/html",
                                                            "application/json"])
        if not self.CONTENT_TYPES & content_type:
            self.error(
                request, UNSUPPORTED_MEDIA_TYPE,
                "%s is not a support content type for this url" % content_type)
            return NOT_DONE_YET

        # Try to find the method this web request is making by first trying
        # our usual convention then the 'standard' convention.
        request_methods = (request.method.lower(), "render_%s" % request.method)
        for method_name in request_methods:
            if hasattr(self, method_name):
                kwargs = {"request": request}

                # Unpack the incoming data for the request
                if "application/json" in content_type \
                        and request.method in self.LOAD_DATA_FOR_METHODS:
                    request_content = request.content.read()

                    # Check to see if we have any incoming data at all
                    if not request_content.strip():
                        self.error(request, BAD_REQUEST, "No data provided")
                        return NOT_DONE_YET

                    # Either load the data or handle the error, don't call
                    # the method unless we're successful.
                    try:
                        data = loads(request_content)

                    except ValueError as e:
                        self.error(
                            request, BAD_REQUEST,
                            "Failed to decode json data: %r" % e)
                        return NOT_DONE_YET

                    # We have data, check to see if we have a schema
                    # and if we do does it validate.
                    schema = self.SCHEMAS.get(request.method)
                    if isinstance(schema, Schema):
                        try:
                            schema(data)
                        except Invalid as e:
                            self.error(
                                request, BAD_REQUEST,
                                "Failed to validate the request data "
                                "against the schema: %s" % e)
                            return NOT_DONE_YET

                    kwargs.update(data=data)

                return getattr(self, method_name)(**kwargs)

        # If we could not find function to call for the given method
        # produce an error.
        else:
            supported_methods = self.methods
            message = "%r only supports the %s method(s)" % (
                request.uri,
                ", ".join(list(map_(str.upper, supported_methods))))

            self.error(request, METHOD_NOT_ALLOWED, message)
            return NOT_DONE_YET
