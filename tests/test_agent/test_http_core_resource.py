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
from twisted.web.test.requesthelper import DummyRequest
from twisted.web.resource import Resource as _Resource
from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Required

from pyfarm.agent.testutil import TestCase
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


class TestResponseTypes(TestCase):
    accept = ["a", "b", "c"]
    content_types = ["d", "e", "f"]
    default = ["g", "h", "i"]

    def test_default_assertion(self):
        resource = Resource()
        request = DummyRequest("/")
        for value in ("", 1, 1.0, dict()):
            with self.assertRaises(AssertionError):
                resource.response_types(request, default=value)

    def test_accept(self):
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", self.accept)
        response_types = resource.response_types(request)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.accept))

    def test_content_type(self):
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Content-Type", self.content_types)
        response_types = resource.response_types(request)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.content_types))

    def test_accept_overrides_content_type(self):
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Content-Type", self.content_types)
        request.requestHeaders.setRawHeaders("Accept", self.accept)
        response_types = resource.response_types(request)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.accept))

    def test_default_empty(self):
        resource = Resource()
        request = DummyRequest("/")
        response_types = resource.response_types(request)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset())

    def test_default_provided(self):
        resource = Resource()
        request = DummyRequest("/")
        response_types = resource.response_types(request, default=self.default)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.default))

    def test_default_ignored(self):
        # Accept
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", self.accept)
        response_types = resource.response_types(request, default=self.default)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.accept))

        # Content-Type
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Content-Type", self.content_types)
        response_types = resource.response_types(request, default=self.default)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.content_types))

        # Accept and Content-Type
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Content-Type", self.content_types)
        request.requestHeaders.setRawHeaders("Accept", self.accept)
        response_types = resource.response_types(request, default=self.default)
        self.assertIsInstance(response_types, frozenset)
        self.assertEqual(response_types, frozenset(self.accept))


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
        request = DummyRequest("/")
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.written, [
                template.load("error.html").render(
                    code=INTERNAL_SERVER_ERROR,
                    code_msg=responses[INTERNAL_SERVER_ERROR],
                    message="Test Error")])

    def test_json(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.written, [json.dumps({"error": "Test Error"})])

    def test_unknown_type(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", ["foobar"])
        resource.setup(request, INTERNAL_SERVER_ERROR, "Test Error")
        resource.render(request)
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, UNSUPPORTED_MEDIA_TYPE)
        self.assertEqual(
            request.written,
            [json.dumps({
                "error": "Can only handle text/html "
                         "or application/json here"})])


class TestRenderTuple(TestCase):
    def test_assertion(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")

        for value in ("", None, 1, set()):
            with self.assertRaises(AssertionError):
                resource.render_tuple(request, value)

    def test_body_code_headers(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
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

    def test_body_code(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
        resource.render_tuple(
            request, ("body", OK)
        )
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, OK)
        self.assertEqual(request.written, ["body"])

    def test_less_than_one_length(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", ["application/json"])
        resource.render_tuple(request, ())
        self.assertTrue(request.finished)
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertEqual(
            request.written, [json.dumps(
                {"error": "Expected two or three length tuple for response"})])


class TestRenderDeferred(TestCase):
    @inlineCallbacks
    def test_assertion(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")
        with self.assertRaises(AssertionError):
            yield resource.render_deferred(request, None)

    @inlineCallbacks
    def test_deferred_tuple_two(self):
        resource = FakeErrorResource()
        request = DummyRequest("/")

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
        request = DummyRequest("/")

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


class TestRender(TestCase):
    def test_unsupported_content_type(self):
        resource = Resource()
        request = DummyRequest("/")
        request.requestHeaders.setRawHeaders("Accept", ["foobar"])
        content_types = resource.response_types(
            request, default=["text/html", "application/json"])

        with patch.object(resource, "error") as error:
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, UNSUPPORTED_MEDIA_TYPE,
            "%s is not a support content type for this url" % content_types
        )

    def test_method_not_allowed(self):
        resource = Resource()
        request = DummyRequest("/")
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
            request = DummyRequest("/")
            request.method = "POST"
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.content = StringIO()
            request.content.write("invalid")
            request.content.seek(0)
            resource = Resource()
            method_impl = Mock()
            setattr(resource, method.lower(), method_impl)

            with patch.object(resource, "error") as error:
                response = resource.render(request)

            self.assertEqual(response, NOT_DONE_YET)
            error.assert_called_once_with(
                request, BAD_REQUEST,
                "Failed to decode json data: ValueError('No JSON object could "
                "be decoded',)"
            )
            self.assertFalse(method_impl.called)

    def test_data_schema_validation_failed(self):
        for method in ("POST", "PUT"):
            request = DummyRequest("/")
            request.method = "POST"
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.content = StringIO()
            request.content.write(json.dumps({"bar": ""}))
            request.content.seek(0)
            resource = Resource()
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
            request = DummyRequest("/")
            request.method = "POST"
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.method = method
            request.requestHeaders.setRawHeaders(
                "Content-Type", ["application/json"])
            request.content = StringIO()
            request.content.write("")
            request.content.seek(0)
            resource = Resource()
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

        request = DummyRequest("/")
        resource = Resource()
        resource.get = get

        with patch.object(resource, "error") as error:
            response = resource.render(request)

        self.assertEqual(response, NOT_DONE_YET)
        error.assert_called_once_with(
            request, INTERNAL_SERVER_ERROR,
            "Unhandled error while rendering response: error"
        )

    def test_handle_not_done_yet(self):
        def get(**_):
            return NOT_DONE_YET

        request = DummyRequest("/")
        resource = Resource()
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
        self.assertFalse(render_deferred.called)

    def test_handle_tuple(self):
        def get(**_):
            return ("body", OK)

        request = DummyRequest("/")
        resource = Resource()
        resource.get = get

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

        request = DummyRequest("/")
        resource = Resource()
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
        def get(**_):
            return None

        request = DummyRequest("/")
        resource = Resource()
        resource.get = get

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
