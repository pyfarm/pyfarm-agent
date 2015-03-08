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

from json import loads, dumps

try:
    from httplib import (
        responses, NOT_FOUND, BAD_REQUEST, UNSUPPORTED_MEDIA_TYPE,
        METHOD_NOT_ALLOWED, INTERNAL_SERVER_ERROR)
except ImportError:  # pragma: no cover
    from http.client import (
        responses, NOT_FOUND, BAD_REQUEST, UNSUPPORTED_MEDIA_TYPE,
        METHOD_NOT_ALLOWED, INTERNAL_SERVER_ERROR)

from twisted.internet.defer import Deferred, inlineCallbacks
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

        * Templates
        * Content type discovery and validation
        * Handling of deferred responses
        * Validation of POST/PUT data against a schema
    """
    TEMPLATE = NotImplemented
    SUPPORTED_CONTENT_TYPES = set(["text/html", "application/json"])

    # Used by API endpoints for data validation
    # format of this attri
    SCHEMAS = {}

    def __init__(self):
        _Resource.__init__(self)
        assert isinstance(self.SUPPORTED_CONTENT_TYPES, set)

    @property
    def template(self):
        """
        Loads the template provided but the partial path in ``TEMPLATE`` on
        the class.
        """
        if self.TEMPLATE is NotImplemented:
            raise NotImplementedError("You must set `TEMPLATE` first")
        return template.load(self.TEMPLATE)

    def response_types(self, request, default=None):
        """
        Returns an instance of :class:`frozenset` which contains the type
        or types of responses we can send.  We look at the ``Accept`` and
        ``Content-Type`` headers to determine this information.  If values
        are present in the ``Accept`` header these will be returned instead
        of values in ``Content-Type``.  If neither of these headers are present
        we'll return what was provided to ``default`` instead.

        .. note::

            This method's purpose is only to return what kinds of responses
            are acceptable based on the request.  Ultimately, it's up to the
            calling method to make the final determination about what kind of
            response to send.
        """
        assert default is None or isinstance(default, (list, tuple, set))
        accepts = request.requestHeaders.getRawHeaders("Accept")
        content_types = request.requestHeaders.getRawHeaders("Content-Type")

        if accepts is not None:
            return frozenset(accepts)
        elif content_types is not None:
            return frozenset(content_types)
        elif default is None:
            return frozenset()
        else:
            return frozenset(default)

    def putChild(self, path, child):
        """
        Overrides the builtin putChild() so we can return the results for
        each call and use them externally.
        """
        assert isinstance(path, STRING_TYPES)
        assert isinstance(child, Resource)
        _Resource.putChild(self, path, child)
        return child

    def error(self, request, code, message):
        """
        Writes the proper out an error response message depending on the
        content type in the request
        """
        content_types = self.response_types(request, default=["text/html"])
        logger.error(message)

        if "text/html" in content_types:
            request.setResponseCode(code)
            html_error = template.load("error.html")
            result = html_error.render(
                code=code, code_msg=responses[code], message=message)
            request.write(result)

        elif "application/json" in content_types:
            request.setResponseCode(code)
            error = dumps({"error": message})
            request.write(error.encode("ascii", "replace"))

        else:
            request.setResponseCode(UNSUPPORTED_MEDIA_TYPE)
            error = dumps(
                {"error": "Can only handle text/html or application/json here"})
            request.write(error.encode("ascii", "replace"))

        request.finish()

    def render_tuple(self, request, response):
        """
        Takes a response tuple of ``(body, code, headers)`` or
        ``(body, code)`` and renders the resulting data onto
        the request.
        """
        if len(response) == 3:
            body, code, headers = response

            if isinstance(headers, dict):
                # Set the response headers
                for header, value in headers.items():
                    # Response header values in Twisted are supposed
                    # to be strings, unlike request headers, according
                    # to the documentation.  Internally it seems to set
                    # it as a list however that's not something the
                    # setHeader() api exposes.
                    if not isinstance(value, STRING_TYPES):
                        self.error(
                            request, INTERNAL_SERVER_ERROR,
                            "Expected string for header values"
                        )
                        return

                    request.setHeader(header, value)

            request.setResponseCode(code)
            request.write(body)
            request.finish()

        elif len(response) == 2:
            body, code = response
            request.setResponseCode(code)
            request.write(body)
            request.finish()

        else:
            self.error(
                request, INTERNAL_SERVER_ERROR,
                "Expected two or three length tuple for response"
            )

    @inlineCallbacks
    def render_deferred(self, request, deferred):
        """
        An inline callback used to unpack a deferred
        response object.
        """
        response = yield deferred
        if isinstance(response, tuple):
            self.render_tuple(request, response)
        else:
            self.error(
                request, INTERNAL_SERVER_ERROR,
                "Expected a tuple to be returned from the deferred response.")

    def render(self, request):
        # Check to ensure that the content type being requested
        content_types = self.response_types(
            request, default=["text/html", "application/json"])

        if not self.SUPPORTED_CONTENT_TYPES & content_types:
            self.error(
                request, UNSUPPORTED_MEDIA_TYPE,
                "%s is not a support content type for this url" % content_types)
            return NOT_DONE_YET

        kwargs = {"request": request}

        try:
            handler_method = getattr(self, request.method.lower())
        except AttributeError:
            self.error(
                request, METHOD_NOT_ALLOWED,
                "Method %s is not supported" % request.method)
            return NOT_DONE_YET

        # Attempt to load the data for the incoming request if appropriate
        if ("application/json" in content_types
                and request.method in ("POST", "PUT")):
            data = request.content.read().strip()
            if data:
                try:
                    data = loads(data)
                except ValueError as e:
                    self.error(
                        request, BAD_REQUEST,
                        "Failed to decode json data: %r" % e)
                    return NOT_DONE_YET

                # We have data, check to see if we have a schema
                # and if we do does it validate.
                schema = self.SCHEMAS.get(request)
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

        try:
            response = handler_method(**kwargs)
        except Exception as error:
            self.error(
                request, INTERNAL_SERVER_ERROR,
                "Unhandled error while rendering response: %s" % error
            )
            return NOT_DONE_YET

        # The handler_method is going to handle everything
        if response == NOT_DONE_YET:
            return NOT_DONE_YET

        # Flask style response
        elif isinstance(response, tuple):
            self.render_tuple(request, response)
            return NOT_DONE_YET

        # handler_method() is returns a Deferred which means
        # we have to handle writing the response ourselves
        elif isinstance(response, Deferred):
            self.render_deferred(request, response)
            return NOT_DONE_YET

        else:
            self.error(
                request, INTERNAL_SERVER_ERROR,
                "Unhandled type %s in response" % response
            )
            return NOT_DONE_YET
