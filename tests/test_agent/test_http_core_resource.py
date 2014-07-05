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

import os
from StringIO import StringIO

try:
    from httplib import (
        BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND, UNSUPPORTED_MEDIA_TYPE)
except ImportError:  # pragma: no cover
    from httplib.client import (
        BAD_REQUEST, INTERNAL_SERVER_ERROR, NOT_FOUND, UNSUPPORTED_MEDIA_TYPE)

from twisted.web.test.requesthelper import DummyRequest as _DummyRequest
from twisted.web.http import Headers
from twisted.web.server import NOT_DONE_YET
from voluptuous import Schema, Required

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.http.core.template import DeferredTemplate
from pyfarm.agent.utility import dumps


class DummyContent(StringIO):
    def read(self, n=-1):
        return self.getvalue()


class DummyRequest(_DummyRequest):
    def __init__(self, uri=None, headers=None, data=None, method="GET"):
        _DummyRequest.__init__(self, uri or [""])
        self.data = ""
        self.requestHeaders = Headers()
        self.content = DummyContent()
        self.method = method.upper()

        if headers is not None:
            for key, value in headers.items():
                if isinstance(value, STRING_TYPES):
                    value = [value]
                self.requestHeaders.setRawHeaders(key, value)

        if uri is not None:
            self.path = self.uri = uri

        if data is not None:
            if not isinstance(data, STRING_TYPES):
                data = dumps(data)
            self.content.write(data)

    def write(self, data):
        if not isinstance(data, STRING_TYPES):
            data = dumps(data)

        self.data += data


class ResourceWithMethods(Resource):
    delete = lambda *args, **kwargs: None
    render_PUT = lambda *args, **kwargs: None


class ResourceWithDuplicateMethods(Resource):
    get = lambda *args, **kwargs: None
    render_GET = lambda *args, **kwargs: None


class PostResource(Resource):
    def __init__(self):
        Resource.__init__(self)
        self.kwargs = {}

    def post(self, **kwargs):
        self.kwargs.update(kwargs)
        return self.kwargs


class PostResourceWithSchema(PostResource):
    SCHEMAS = {
        "POST": Schema({Required("foo"): int})}


class TestResourceBase(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self._template = Resource.TEMPLATE
        self._content_types = Resource.CONTENT_TYPES

    def tearDown(self):
        TestCase.tearDown(self)
        Resource.TEMPLATE = self._template
        Resource.CONTENT_TYPES = self._content_types


class TestResourceInternals(TestResourceBase):
    def setUp(self):
        TestResourceBase.setUp(self)
        self._schemas = Resource.SCHEMAS.copy()
        self._pop_empty = Resource.POP_EMPTY_VALUES

    def tearDown(self):
        TestResourceBase.tearDown(self)
        Resource.SCHEMAS.clear()
        Resource.SCHEMAS.update(self._schemas)
        Resource.POP_EMPTY_VALUES = self._pop_empty

    def test_classvar_template(self):
        self.assertIs(Resource.TEMPLATE, NotImplemented)

    def test_classvar_content_types(self):
        self.assertEqual(
            Resource.CONTENT_TYPES, set(["text/html", "application/json"]))

    def test_classvar_load_data_for_methods(self):
        self.assertEqual(
            Resource.LOAD_DATA_FOR_METHODS, set(["POST", "PUT"]))

    def test_classvar_schemas(self):
        self.assertEqual(Resource.SCHEMAS, {})

    def test_content_types_not_set(self):
        Resource.CONTENT_TYPES = None
        with self.assertRaises(AssertionError):
            Resource()

    def test_template_property_not_set(self):
        Resource.TEMPLATE = NotImplemented
        resource = Resource()

        with self.assertRaises(NotImplementedError):
            resource.template()

    def test_template_property(self):
        resource = Resource()
        resource.TEMPLATE = "index.html"
        template = resource.template
        self.assertIsInstance(template, DeferredTemplate)

    def test_methods_property_default(self):
        resource = Resource()
        self.assertEqual(resource.methods, set(["head"]))

    def test_methods_property(self):
        resource = ResourceWithMethods()
        self.assertEqual(resource.methods, set(["head", "delete", "put"]))

    def test_methods_method_property_with_duplicate_methods(self):
        resource = ResourceWithDuplicateMethods()

        with self.assertRaises(ValueError):
            resource.methods()

    def test_content_types_default(self):
        request = DummyRequest(headers={})
        resource = Resource()
        self.assertEqual(resource.content_types(request), set())
        self.assertEqual(
            resource.content_types(request, default="foo"), set(["foo"]))

    def test_content_types(self):
        request = DummyRequest(headers={"content-type": "foo"})
        resource = Resource()
        self.assertEqual(resource.content_types(request), set(["foo"]))

    def test_put_child(self):
        root = Resource()
        child = Resource()
        self.assertIs(child, root.putChild("", child))

    def test_error_html(self):
        request = DummyRequest(headers={"content-type": "text/html"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "error_html")
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertTrue(request.finished)
        self.assertIn("DOCTYPE html", request.data)
        self.assertIn("error_html", request.data)

    def test_error_json(self):
        request = DummyRequest(headers={"content-type": "application/json"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "error_json")
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertTrue(request.finished)
        self.assertEqual(request.data, dumps({"error": "error_json"}))

    def test_error_unhandled(self):
        request = DummyRequest(headers={"content-type": "foobar"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "internal_error")
        self.assertEqual(request.responseCode, UNSUPPORTED_MEDIA_TYPE)
        self.assertTrue(request.finished)
        self.assertEqual(
            request.data,
            dumps({"error":
                       "Can only handle text/html or application/json here"}))


class TestResourceRendering(TestResourceBase):
    def test_leaf_without_trailing_slash(self):
        request = DummyRequest("/foo", headers={"content-type": "application/json"})
        resource = Resource()
        resource.isLeaf = True
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertEqual(request.data, dumps({"error": "/foo does not exist"}))
        self.assertEqual(request.responseCode, NOT_FOUND)

    def test_invalid_content_type(self):
        request = DummyRequest("", headers={"content-type": "foo"})
        resource = Resource()
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertEqual(
            request.data,
            dumps({"error":
                       "Can only handle text/html or application/json here"}))
        self.assertEqual(request.responseCode, UNSUPPORTED_MEDIA_TYPE)

    def test_no_content_provided_in_request(self):
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data="", method="POST")
        resource = PostResource()
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertEqual(request.data, dumps({"error": "No data provided"}))
        self.assertEqual(request.responseCode, BAD_REQUEST)

    def test_post_json(self):
        data = {os.urandom(6).encode("hex"): os.urandom(6).encode("hex")}
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data=data, method="POST")
        resource = PostResource()
        render_result = resource.render(request)
        self.assertIs(render_result["request"], request)
        self.assertEqual(render_result["data"], data)

    def test_post_json_decode_error(self):
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data="{", method="POST")
        resource = PostResource()
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertIn("Failed to decode json data", request.data)
        self.assertEqual(request.responseCode, BAD_REQUEST)

    def test_post_schema(self):
        data = {"foo": 42}
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data=data, method="POST")
        resource = PostResourceWithSchema()
        render_result = resource.render(request)
        self.assertIs(render_result["request"], request)
        self.assertEqual(render_result["data"], data)

    def test_post_schema_error(self):
        data = {"foo": "bar"}
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data=data, method="POST")
        resource = PostResourceWithSchema()
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertEqual(request.data, dumps({
            "error": "Failed to validate the request data against "
                     "the schema: expected int for dictionary value @ "
                     "data[u'foo']"}))
        self.assertEqual(request.responseCode, BAD_REQUEST)

    def test_unsupported_method(self):
        data = {"foo": "bar"}
        request = DummyRequest(
            "", headers={"content-type": "application/json"},
            data=data, method="DELETE")
        resource = PostResource()
        render_result = resource.render(request)
        self.assertEqual(render_result, NOT_DONE_YET)
        self.assertEqual(request.data, dumps({
            "error": "'' only supports the POST, HEAD method(s)"}))

