# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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

import json
from contextlib import nested
from StringIO import StringIO

try:
    from httplib import (
        responses, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND,
        UNSUPPORTED_MEDIA_TYPE, OK, METHOD_NOT_ALLOWED)
except ImportError:  # pragma: no cover
    from httplib.client import (
        responses, BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND,
        UNSUPPORTED_MEDIA_TYPE, OK, METHOD_NOT_ALLOWED)

from mock import Mock, patch

from twisted.internet.defer import (
    Deferred, inlineCallbacks, returnValue, succeed)
from twisted.web.resource import Resource as _Resource
from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Required

from pyfarm.agent.testutil import TestCase, DummyRequest
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.http.core import template


class TestTemplate(TestCase):
    def test_template_not_implemented(self):
        resource = Resource()

        with self.assertRaises(NotImplementedError):
            resource.template

    def test_loads_template(self):
        class Foo(Resource):
            TEMPLATE = "foobar.html"

        foo = Foo()

        with patch.object(template, "load", return_value=Foo.TEMPLATE) as load:
            self.assertEqual(foo.template, Foo.TEMPLATE)

        load.assert_called_with(Foo.TEMPLATE)


class TestMethods(TestCase):
    def test_methods(self):
        resource = Resource()
        resource.get = lambda self: None
        resource.put = lambda self: None
        self.assertEqual(set(resource.methods()), set(["get", "put"]))

    def test_non_callable_method(self):
        resource = Resource()
        resource.get = lambda self: None
        resource.put = lambda self: None
        resource.delete = None
        self.assertEqual(set(resource.methods()), set(["get", "put"]))

    def test_ignores_twisted_named_methods(self):
        resource = Resource()
        resource.render_GET = lambda self: None
        resource.render_PUT = lambda self: None
        resource.delete = lambda self: None
        self.assertEqual(set(resource.methods()), set(["delete"]))


class TestHeaders(TestCase):
    HEADER = NotImplemented
    DEFAULT_ATTRIBUTE = NotImplemented
    METHOD = NotImplemented

    # Only run the real _run if we're inside a child
    # class.
    def _run(self, methodName, result):
        if self.HEADER is NotImplemented:
            return succeed(True)

        return super(TestHeaders, self)._run(methodName, result)

    def test_get_correct_header(self):
        resource = Resource()
        request = DummyRequest()

        with patch.object(
                request.requestHeaders, "getRawHeaders",
                return_value=False) as get_headers:
            getattr(resource, self.METHOD)(request)

        get_headers.assert_called_with(self.HEADER)

    def test_returns_default_if_header_not_set(self):
        resource = Resource()
        request = DummyRequest()

        with patch.object(
                request.requestHeaders, "getRawHeaders", return_value=False):
            result = getattr(resource, self.METHOD)(request)

        self.assertIs(result, getattr(resource, self.DEFAULT_ATTRIBUTE))

    def test_split_header_single_entry(self):
        resource = Resource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders(
            self.HEADER,
            ["text/html,application/xhtml+xml,"
             "application/xml;q=0.9,image/webp,*/*;q=0.8"])
        result = getattr(resource, self.METHOD)(request)
        self.assertEqual(
            result,
            frozenset([
                "*/*", "application/xhtml+xml", "application/xml",
                "image/webp", "text/html"])
        )

    def test_split_header_multiple_entries(self):
        resource = Resource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders(
            self.HEADER,
            ["text/html,application/xhtml+xml,"
             "application/xml;q=0.9,image/webp,*/*;q=0.8",
             "application/json", "text/plain;q=.1"])
        result = getattr(resource, self.METHOD)(request)
        self.assertEqual(
            result,
            frozenset([
                "*/*", "application/xhtml+xml", "application/xml",
                "image/webp", "text/html", "application/json", "text/plain"])
        )


class TestAccept(TestHeaders):
    HEADER = "Accept"
    DEFAULT_ATTRIBUTE = "DEFAULT_ACCEPT"
    METHOD = "get_accept"


class TestContentTypes(TestHeaders):
    HEADER = "Content-Type"
    DEFAULT_ATTRIBUTE = "DEFAULT_CONTENT_TYPE"
    METHOD = "get_content_type"


class TestPutChild(TestCase):
    def test_assertion_invalid_path(self):
        resource = Resource()

        with self.assertRaises(AssertionError):
            resource.putChild(None, Resource())

    def test_assertion_invalid_child(self):
        resource = Resource()

        with self.assertRaises(AssertionError):
            resource.putChild("/", None)

    def test_returns_child(self):
        resource = Resource()
        child = Resource()
        self.assertIs(resource.putChild("/", child), child)

    def test_call(self):
        resource = Resource()
        child = Resource()

        with patch.object(_Resource, "putChild") as putChild:
            resource.putChild("/", child)

        putChild.assert_called_with(resource, "/", child)

    def test_adds_child(self):
        resource = Resource()
        child = Resource()
        resource.putChild("/", child)
        self.assertIn("/", resource.children)
        self.assertIs(resource.children["/"], child)


class FakeErrorResource(Resource):
    def setup(self, request, code, message):
        self.test_request = request
        self.test_code = code
        self.test_message = message

    def render(self, request):
        self.error(self.test_request, self.test_code, self.test_message)
        return NOT_DONE_YET


class TestError(TestCase):
    def test_html(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders("Accept", ["text/html"])
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(
            request.responseCode, INTERNAL_SERVER_ERROR, request.written)
        self.assertEqual(
            request.written, [
                template.load("error.html").render(
                    code=INTERNAL_SERVER_ERROR,
                    code_msg=responses[INTERNAL_SERVER_ERROR],
                    message="Test Error")])

    def test_json(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.written, [json.dumps({"error": "Test Error"})])

    def test_unknown_type(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders("Accept", ["foobar"])
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, UNSUPPORTED_MEDIA_TYPE)
        self.assertEqual(
            request.written,
            [json.dumps({
                # Expect NotImplemented because it's not set on the base class
                "error": "Can only handle one of NotImplemented here"})])


class TestRenderTuple(TestCase):
    def test_assertion(self):
        resource = FakeErrorResource()
        request = DummyRequest()

        for value in ("", None, 1, set()):
            with self.assertRaises(AssertionError):
                resource.render_tuple(request, value)

    def test_body_code(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        resource.render_tuple(
            request, ("body", OK)
        )
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, ["body"])

    def test_less_than_one_length(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.render_tuple(request, ())
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.written, [json.dumps(
                {"error": "Expected two or three length tuple for response"})])

    def test_default_content_type(self):
        """
        If the Content-Type response header is not already set, set it based
        on the defaults
        """
        request = DummyRequest()
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/pyfarm"])
        resource.render_tuple(request, ("body", OK))
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )

    def test_content_type_already_set(self):
        """
        If the Content-Type response header is already set, do not set
        it again.
        """
        request = DummyRequest()
        request.responseHeaders.setRawHeaders(
            "Content-Type", ["application/pyfarm"])
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/foobar"])
        resource.render_tuple(request, ("body", OK))
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )


class TestRenderThreeTuple(TestCase):
    def test_body_code_headers(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        resource.render_tuple(
            request, ("body", OK, {"Foo": "a", "Bar": ["c", "d"]})
        )
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, ["body"])
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Foo"), ["a"])
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Bar"), ["c", "d"])

    def test_default_content_type(self):
        """
        If the Content-Type response header is not already set, set it based
        on the defaults
        """
        request = DummyRequest()
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/pyfarm"])
        resource.render_tuple(request, ("body", OK, {}))
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )

    def test_content_type_already_set(self):
        """
        If the Content-Type response header is already set, do not set
        it again.
        """
        request = DummyRequest()
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/foobar"])
        resource.render_tuple(
            request, ("body", OK, {"Content-Type": ["application/pyfarm"]})
        )
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )



class TestRenderDeferred(TestCase):
    @inlineCallbacks
    def test_assertion(self):
        resource = FakeErrorResource()
        request = DummyRequest()
        with self.assertRaises(AssertionError):
            yield resource.render_deferred(request, None)

    @inlineCallbacks
    def test_deferred_tuple_two(self):
        resource = FakeErrorResource()
        request = DummyRequest()

        @inlineCallbacks
        def render():
            yield succeed(None)
            returnValue(("body", OK))

        yield resource.render_deferred(request, render())
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, ["body"])

    @inlineCallbacks
    def test_deferred_tuple_three(self):
        resource = FakeErrorResource()
        request = DummyRequest()

        @inlineCallbacks
        def render():
            yield succeed(None)
            returnValue(("body", OK, {"Foo": "a", "Bar": ["c", "d"]}))

        yield resource.render_deferred(request, render())
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, ["body"])
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Foo"), ["a"])
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Bar"), ["c", "d"])

    @inlineCallbacks
    def test_default_content_type(self):
        """
        If the Content-Type response header is not already set, set it based
        on the defaults
        """
        request = DummyRequest()
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/pyfarm"])

        @inlineCallbacks
        def render():
            yield succeed(None)
            returnValue(("body", OK))

        yield resource.render_deferred(request, render())
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )

    @inlineCallbacks
    def test_content_type_already_set(self):
        """
        If the Content-Type response header is already set, do not set
        it again.
        """
        request = DummyRequest()
        resource = Resource()
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/foobar"])

        @inlineCallbacks
        def render():
            yield succeed(None)
            request.responseHeaders.setRawHeaders(
                "Content-Type", ["application/pyfarm"]
            )
            returnValue(("body", OK))

        yield resource.render_deferred(request, render())
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )

class TestRenderString(TestCase):
    def test_default_content_type(self):
        """
        If the Content-Type response header is not already set, set it based
        on the defaults
        """
        request = DummyRequest()
        resource = Resource()
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.ALLOWED_ACCEPT = frozenset(["application/json"])
        resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/pyfarm"])
        resource.get = lambda *_, **__: ""
        resource.render(request)

        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )

    def test_content_type_already_set(self):
        """
        If the Content-Type response header is already set, do not set
        it again.
        """
        request = DummyRequest()
        resource = Resource()
        request.responseHeaders.setRawHeaders(
            "Content-Type", ["application/pyfarm"])
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.ALLOWED_ACCEPT = frozenset(["application/json"])
        resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
        resource.DEFAULT_CONTENT_TYPE = frozenset(["application/foobar"])
        resource.get = lambda *_, **__: ""
        resource.render(request)
        self.assertEqual(
            request.responseHeaders.getRawHeaders("Content-Type"),
            ["application/pyfarm"]
        )



class TestRender(TestCase):
    def test_unsupported_media_type(self):
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset("")
        resource.ALLOWED_ACCEPT = frozenset("")
        resource.get = lambda **_: ""
        request = DummyRequest()
        request.requestHeaders.setRawHeaders("Accept", ["foobar"])
        request.set_content("hello")

        with patch.object(resource, "error") as error:
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, UNSUPPORTED_MEDIA_TYPE,
            "Can only support content type(s) frozenset([])"
        )

    def test_method_not_allowed(self):
        resource = Resource()
        request = DummyRequest()
        request.method = "foobar"

        with patch.object(resource, "error") as error:
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, METHOD_NOT_ALLOWED,
            "Method %s is not supported" % request.method
        )

    def test_data_is_not_json(self):
        for method in ("POST", "PUT"):
            request = DummyRequest()
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.requestHeaders.setRawHeaders(
                "Accept", ["application/json"])
            request.set_content("/")
            resource = Resource()
            resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
            resource.ALLOWED_ACCEPT = frozenset(["application/json"])
            method_impl = Mock()
            setattr(resource, method.lower(), method_impl)

            with patch.object(resource, "error") as error:
                response = resource.render(request)

            self.assertEqual(response, NOT_DONE_YET)
            error.assert_called_once_with(
                request, BAD_REQUEST,
                "Failed to decode json data: "
                "ValueError('No JSON object could be decoded',)"
            )
            self.assertFalse(method_impl.called)

    def test_data_schema_validation_failed(self):
        for method in ("POST", "PUT"):
            request = DummyRequest()
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.requestHeaders.setRawHeaders(
                "Accept", ["application/json"])
            request.set_content(json.dumps({"bar": ""}))
            resource = Resource()
            resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
            resource.ALLOWED_ACCEPT = frozenset(["application/json"])
            resource.SCHEMAS = {
                method: Schema({Required("foo"): str})
            }
            method_impl = Mock(return_value=("", OK))
            setattr(resource, method.lower(), method_impl)

            with patch.object(resource, "error") as error:
                response = resource.render(request)

            self.assertEqual(response, NOT_DONE_YET)
            error.assert_called_once_with(
                request, BAD_REQUEST,
                "Failed to validate the request data against the schema: extra "
                "keys not allowed @ data[u'bar']"
            )
            self.assertFalse(method_impl.called)

    def test_data_empty(self):
        for method in ("POST", "PUT"):
            request = DummyRequest()
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.requestHeaders.setRawHeaders(
                "Accept", ["application/json"])
            resource = Resource()
            resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
            resource.ALLOWED_ACCEPT = frozenset(["application/json"])
            resource.SCHEMAS = {
                method: Schema({Required("foo"): str})
            }
            method_impl = Mock(return_value=("", OK))
            setattr(resource, method.lower(), method_impl)
            response = resource.render(request)
            self.assertEqual(response, NOT_DONE_YET)
            method_impl.assert_called_once_with(request=request)

    def test_exception_in_handler_method(self):
        def get(**_):
            raise ValueError("error")

        request = DummyRequest()
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset(["application/json"])
        resource.ALLOWED_ACCEPT = frozenset(["*/*"])
        resource.get = get

        with patch.object(resource, "error") as error:
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, INTERNAL_SERVER_ERROR,
            "Unhandled error while rendering response: error"
        )

    def test_handle_not_done_yet(self):
        request = DummyRequest()
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset([""])
        resource.ALLOWED_ACCEPT = frozenset(["*/*"])
        resource.get = lambda **_: NOT_DONE_YET

        with nested(
            patch.object(resource, "error"),
            patch.object(resource, "render_tuple"),
            patch.object(resource, "render_deferred"),
        ) as (error, render_tuple, render_deferred):
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        self.assertFalse(error.called)
        self.assertFalse(render_tuple.called)
        self.assertFalse(render_deferred.called)

    def test_handle_tuple(self):
        request = DummyRequest()
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset([""])
        resource.ALLOWED_ACCEPT = frozenset(["*/*"])
        resource.get = lambda **_: ("body", OK)

        with nested(
            patch.object(resource, "error"),
            patch.object(resource, "render_tuple"),
            patch.object(resource, "render_deferred"),
        ) as (error, render_tuple, render_deferred):
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        self.assertFalse(error.called)
        render_tuple.assert_called_once_with(request, ("body", OK))
        self.assertFalse(render_deferred.called)

    def test_handle_deferred(self):
        @inlineCallbacks
        def get(**_):
            yield succeed(None)
            returnValue(("body", OK))

        request = DummyRequest()
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset([""])
        resource.ALLOWED_ACCEPT = frozenset(["*/*"])
        resource.get = get

        with nested(
            patch.object(resource, "error"),
            patch.object(resource, "render_tuple"),
            patch.object(resource, "render_deferred"),
        ) as (error, render_tuple, render_deferred):
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        self.assertFalse(error.called)
        self.assertFalse(render_tuple.called)
        self.assertEqual(render_deferred.call_count, 1)
        self.assertIs(render_deferred.call_args[0][0], request)
        self.assertIsInstance(render_deferred.call_args[0][1], Deferred)

    def test_handle_unknown_return_value(self):
        request = DummyRequest()
        resource = Resource()
        resource.ALLOWED_CONTENT_TYPE = frozenset([""])
        resource.ALLOWED_ACCEPT = frozenset(["*/*"])
        resource.get = lambda **_: None

        with nested(
            patch.object(resource, "error"),
            patch.object(resource, "render_tuple"),
            patch.object(resource, "render_deferred"),
        ) as (error, render_tuple, render_deferred):
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, INTERNAL_SERVER_ERROR,
            "Unhandled %r in response" % None
        )
        self.assertFalse(render_tuple.called)
        self.assertFalse(render_deferred.called)
