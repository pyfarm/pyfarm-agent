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
        METHOD_NOT_ALLOWED, INTERNAL_SERVER_ERROR, OK, NOT_ACCEPTABLE)
except ImportError:  # pragma: no cover
    from http.client import (
        responses, NOT_FOUND, BAD_REQUEST, UNSUPPORTED_MEDIA_TYPE,
        METHOD_NOT_ALLOWED, INTERNAL_SERVER_ERROR, OK, NOT_ACCEPTABLE)

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.web.server import NOT_DONE_YET
from twisted.web.resource import Resource as _Resource
from twisted.web.static import File
from voluptuous import Invalid, Schema

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.http.core import template
from pyfarm.agent.logger import getLogger
from pyfarm.agent.utility import dumps

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

    :cvar string TEMPLATE:
        The name of the template this class will use when rendering
        an html view.

    :type SCHEMAS: dict
    :cvar SCHEMAS:
        A dictionary of schemas to validate the data of an incoming request
        against.  The structure of this dictionary is::

            {http method: <instance of voluptuous.Schema>}

        If the schema validation fails the request will be rejected with
        ``400 BAD REQUEST``.

    :type ALLOWED_CONTENT_TYPE: frozenset
    :cvar ALLOWED_CONTENT_TYPE:
        An instance of :class:`frozenset` which describes what this resource
        is going to allow in the ``Content-Type`` header.  The request
        and this instance must share at least on entry in common.  If not,
        the request will be rejected with ``415 UNSUPPORTED MEDIA TYPE``.

        **This must be defined in subclass**

    :type ALLOWED_ACCEPT: frozenset
    :cvar ALLOWED_ACCEPT:
        An instance of :class:`frozenset` which describes what this
        resource is going to allow in the ``Accept`` header.  The request
        and this instance must share at least one entry in common.  If not,
        the request will be rejected with ``406 NOT ACCEPTABLE``.

        **This must be defined in subclass**

    :type DEFAULT_ACCEPT: frozenset
    :cvar DEFAULT_ACCEPT:
        If ``Accept`` header is not present in the request, use this as the
        value instead.  This defaults to ``frozenset(["*/*"])``

    :type DEFAULT_CONTENT_TYPE: frozenset
    :cvar DEFAULT_CONTENT_TYPE:
        If ``Content-Type`` header is not present in the request, use this as
        the value instead.  This defaults to ``frozenset([""])``
    """
    TEMPLATE = NotImplemented
    SCHEMAS = {}

    # These must be set in a subclass and
    # should contain the full range of headers
    # allowed for Accept and Content-Type.
    ALLOWED_ACCEPT = NotImplemented
    ALLOWED_CONTENT_TYPE = NotImplemented

    # Default values if certain headers
    # are not present.
    DEFAULT_ACCEPT = frozenset(["*/*"])
    DEFAULT_CONTENT_TYPE = frozenset([None])

    @property
    def template(self):
        """
        Loads the template provided but the partial path in ``TEMPLATE`` on
        the class.
        """
        if self.TEMPLATE is NotImplemented:
            raise NotImplementedError("You must set `TEMPLATE` first")
        return template.load(self.TEMPLATE)

    def methods(self):
        """
        Returns a tuple of methods which an instance of this class implements
        """
        methods = []
        for method_name in ("get", "put", "post", "delete", "head"):
            method = getattr(self, method_name, None)
            if method is not None and callable(method):
                methods.append(method_name)

        return tuple(methods)

    def get_content_type(self, request):
        """
        Return the ``Content-Type`` header(s) in the request or
        ``DEFAULT_CONTENT_TYPE`` if the header is not set.
        """
        header = request.requestHeaders.getRawHeaders("Content-Type")
        if not header:
            return self.DEFAULT_CONTENT_TYPE

        content_type = set()
        for value in header:
            # Split out the various parts of the header and return them.  We
            # ignore the q parameter here for the moment.
            content_type.update(
                entry.split(";")[0] for entry in value.split(","))

        return content_type

    def get_accept(self, request):
        """
        Return the ``Accept`` header(s) in the request or
        ``DEFAULT_ACCEPT`` if the header is not set.
        """
        header = request.requestHeaders.getRawHeaders("Accept")
        if not header:
            return self.DEFAULT_ACCEPT

        accept = set()
        for value in header:
            # Split out the various parts of the header and return them.  We
            # ignore the q parameter here for the moment.
            accept.update(entry.split(";")[0] for entry in value.split(","))

        return frozenset(accept)

    def putChild(self, path, child):
        """
        Overrides the builtin putChild() so we can return the results for
        each call and use them externally.
        """
        assert isinstance(path, STRING_TYPES)
        assert isinstance(child, (Resource, File))
        _Resource.putChild(self, path, child)
        return child

    def error(self, request, code, message):
        """
        Writes the proper out an error response message depending on the
        content type in the request
        """
        response_types = self.get_accept(request)
        logger.error(message)

        if "text/html" in response_types:
            request.setResponseCode(code)
            html_error = template.load("error.html")
            result = html_error.render(
                code=code, code_msg=responses[code], message=message)
            request.write(result)

        elif "application/json" in response_types:
            request.setResponseCode(code)
            request.write(dumps({"error": message}))

        else:
            request.setResponseCode(UNSUPPORTED_MEDIA_TYPE)
            error = dumps(
                {"error":
                     "Can only handle one of %s here" % self.ALLOWED_ACCEPT})
            request.write(error)

        request.finish()

    def render_tuple(self, request, response):
        """
        Takes a response tuple of ``(body, code, headers)`` or
        ``(body, code)`` and renders the resulting data onto
        the request.
        """
        assert isinstance(response, (list, tuple)), type(response)
        if len(response) == 3:
            body, code, headers = response

            if isinstance(headers, dict):
                for header, value in headers.items():
                    if isinstance(value, STRING_TYPES):
                        value = [value]
                    request.responseHeaders.setRawHeaders(header, value)

            if not request.responseHeaders.hasHeader("Content-Type"):
                request.responseHeaders.setRawHeaders(
                    "Content-Type",
                    list(self.DEFAULT_CONTENT_TYPE)
                )

            request.setResponseCode(code)

            # Cast to str, otherwise Twisted responds
            #   TypeError: Data must not be unicode
            request.write(str(body))
            request.finish()

        elif len(response) == 2:
            body, code = response

            # Set Content-Type if it has not already been set
            if not request.responseHeaders.hasHeader("Content-Type"):
                request.responseHeaders.setRawHeaders(
                    "Content-Type",
                    list(self.DEFAULT_CONTENT_TYPE)
                )

            request.setResponseCode(code)

            # Cast to str, otherwise Twisted responds
            #   TypeError: Data must not be unicode
            request.write(str(body))
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
        assert isinstance(deferred, Deferred)
        response = yield deferred
        self.render_tuple(request, response)

    def render(self, request):
        try:
            handler_method = getattr(self, request.method.lower())
        except AttributeError:
            self.error(
                request, METHOD_NOT_ALLOWED,
                "Method %s is not supported" % request.method)
            return NOT_DONE_YET

        assert isinstance(self.ALLOWED_CONTENT_TYPE, (set, frozenset))
        content = request.content.read().strip()
        shared_content_types = \
            self.get_content_type(request) & self.ALLOWED_CONTENT_TYPE

        # Ensure we can handle the content of the request
        if content and not shared_content_types:
            self.error(
                request, UNSUPPORTED_MEDIA_TYPE,
                "Can only support content "
                "type(s) %s" % self.ALLOWED_CONTENT_TYPE)
            return NOT_DONE_YET

        # Determine if we'll be able to produce a response for the request
        assert isinstance(self.ALLOWED_ACCEPT, (set, frozenset))
        response_types = self.get_accept(request) & self.ALLOWED_ACCEPT
        if not response_types:
            self.error(
                request, NOT_ACCEPTABLE,
                "Can only respond with %s" % self.ALLOWED_ACCEPT)
            return NOT_DONE_YET

        # Keywords to pass into `handler_method` below
        kwargs = dict(request=request)

        # Attempt to load the data for the incoming request if appropriate
        if content and "application/json" in shared_content_types:
            try:
                data = loads(content)
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

        elif isinstance(response, STRING_TYPES):
            # Set Content-Type if it has not already been set
            if not request.responseHeaders.hasHeader("Content-Type"):
                request.responseHeaders.setRawHeaders(
                    "Content-Type",
                    list(self.DEFAULT_CONTENT_TYPE)
                )

            request.setResponseCode(OK)
            request.write(response)
            request.finish()
            return NOT_DONE_YET

        else:
            self.error(
                request, INTERNAL_SERVER_ERROR,
                "Unhandled %r in response" % response
            )
            return NOT_DONE_YET
