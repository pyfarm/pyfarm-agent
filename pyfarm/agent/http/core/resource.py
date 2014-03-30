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

from httplib import (
    responses, INTERNAL_SERVER_ERROR, NOT_FOUND,
    UNSUPPORTED_MEDIA_TYPE, METHOD_NOT_ALLOWED)


from twisted.python import log
from twisted.python.compat import intToBytes
from twisted.web.error import Error
from twisted.web.server import Request as _Request, NOT_DONE_YET
from twisted.web.resource import Resource as _Resource

from pyfarm.agent.http.core import template
from pyfarm.agent.utility import dumps


class Request(_Request):
    """
    Overrides the default :class:`._Request` so we can produce
    custom errors
    """
    def processingFailed(self, reason):
        if reason.type is JSONError:
            log.err(reason)
            body = dumps((reason.value.status, reason.value.message))
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
    # set by child classes
    TEMPLATE = NotImplemented
    CONTENT_TYPES = set(["text/html", "application/json"])

    # Twisted is not very explicit about trailing slash/no trailing slash
    # so you can often access the resource 'foo' with either '/foo' or
    # '/foo/'.  We typically do not want this behavior
    IGNORE_TRAILING_SLASH = False
    REDIRECT_IF_MISSING_TRAILING_SLASH = True

    # setup in __init__ because we don't want to
    # pull the config values until we instance the class
    template_loader = None

    isLeaf = True

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
            attribute = getattr(self, method, None)
            if attribute is not None and callable(attribute):
                methods.add(method)
        return methods

    def content_types(self, request, default=None):
        """Returns the content type(s) present in the request"""
        default = default or []
        return set(
            request.requestHeaders.getRawHeaders(
                "content-type", default=default))

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
        content_types = self.content_types(request)

        if "text/html" in content_types:
            request.setResponseCode(code)
            html_error = template.load("error.html")
            deferred = html_error.render(
                code=code, code_msg=responses[code], message=message)
            deferred.addCallback(request.write).addCallback(
                lambda _: request.finish())

        elif "application/json" in content_types:
            request.setResponseCode(code)
            request.write(dumps(error=message))
            request.finish()
        else:
            request.setResponseCode(INTERNAL_SERVER_ERROR)
            request.write(
                dumps(
                    error="Can only handle text/html or application/json here"))
            request.finish()

    def render(self, request):
        """
        Provides similar behavior to :class:`flask.view.MethodView`.
        while processing like :meth:`._Resource.render`
        """
        # no such url
        if request.uri not in self.children:
            self.error(
                request, NOT_FOUND,
                "The requested uri, %r, does not exist" % request.uri)

            return NOT_DONE_YET

        # unsupported content type
        if not self.CONTENT_TYPES & self.content_types(request):
            self.error(
                request, UNSUPPORTED_MEDIA_TYPE,
                "%r only only accepts requests with content type(s) %s" % (
                    request.uri, ", ".join(self.CONTENT_TYPES)))
            return NOT_DONE_YET

        resource = self.children[request.uri]

        # unsupported http method
        request_method = request.method.lower()
        supported_methods = resource.methods
        if request_method not in supported_methods:
            if not supported_methods:
                message = "%r does not support any methods" % request.uri
            else:
                message = "%r only supports the %s method(s)" % (
                    request.uri,
                    ", ".join(map(str.upper, supported_methods)))

            self.error(request, METHOD_NOT_ALLOWED, message)

        method = getattr(resource, request_method)

    # this is required so we can write to the request
    # object ourselves
    # return NOT_DONE_YET




        #     #
            #
            # # Didn't find the method?  Find what methods are supported
            # # and tell the client about them.
            # request_method = nativeString(request.method).lower()
            # instance_method = getattr(self, request_method, None)
            # if instance_method is None:
            #     # print request.uri
            #     print
            #     print [name for name in ("get", "post", "put", "delete", "head")
            #          if hasattr(self, name)]
            #     raise UnsupportedMethod(
            #         [name for name in ("get", "post", "put", "delete", "head")
            #          if hasattr(self, name)])
            #
            # # CONTENT_TYPE was specified so the incoming request should
            # # both specify the header and it should match our expected content type
            # if self.CONTENT_TYPE is not NotImplemented:
            #     if not request.requestHeaders.hasHeader("content-type"):
            #         raise Error(BAD_REQUEST, "missing content-type header")
            #     else:
            #         headers = set(
            #             request.requestHeaders.getRawHeaders("content-type"))
            #
            #         # check the headers against the requested headers
            #         if headers != self.CONTENT_TYPE:
            #             raise Error(
            #                 BAD_REQUEST, "invalid headers for this resource")
            #
            #     if self.SET_RESPONSE_CONTENT_TYPE:
            #         request.responseHeaders.setRawHeaders(
            #             "content-type", list(self.CONTENT_TYPE))

            # return self.handle(instance_method, request)
    #
    def handle(self, instance_method, request):
        pass
    #     """
    #     Method which receives an instance method from :meth:`render` as well
    #     as the incoming request.  This is provided so a subclass can change
    #     both the calling behavior as well as the results.
    #     """
    #     try:
    #         return instance_method(request)
    #
    #     # known error case
    #     except Error as e:
    #         raise e
    #


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
