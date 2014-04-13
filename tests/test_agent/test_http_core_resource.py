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

try:
    from httplib import BAD_REQUEST, INTERNAL_SERVER_ERROR
except ImportError:
    from httplib.client import BAD_REQUEST, INTERNAL_SERVER_ERROR

from twisted.web.test.requesthelper import DummyRequest as _DummyRequest
from twisted.web.http import Headers

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.http.core.resource import Resource
from pyfarm.agent.http.core.template import DeferredTemplate
from pyfarm.agent.utility import dumps


class DummyRequest(_DummyRequest):
    def __init__(self, headers=None):
        _DummyRequest.__init__(self, [""])
        self.data = []
        self.requestHeaders = Headers()

        if headers is not None:
            for key, value in headers.items():
                if isinstance(value, STRING_TYPES):
                    value = [value]
                self.requestHeaders.setRawHeaders(key, value)

    def write(self, data):
        self.data.append(data)


class ResourceWithMethods(Resource):
    delete = lambda *args, **kwargs: None
    render_PUT = lambda *args, **kwargs: None


class ResourceWithDuplicateMethods(Resource):
    get = lambda *args, **kwargs: None
    render_GET = lambda *args, **kwargs: None


class TestResourceInternals(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self._template = Resource.TEMPLATE
        self._content_types = Resource.CONTENT_TYPES

    def tearDown(self):
        TestCase.tearDown(self)
        Resource.TEMPLATE = self._template
        Resource.CONTENT_TYPES = self._content_types

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
        self.assertRaises(AssertionError, lambda: Resource())

    def test_template_property_not_set(self):
        Resource.TEMPLATE = NotImplemented
        resource = Resource()
        self.assertRaises(NotImplementedError, lambda: resource.template)

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
        self.assertRaises(ValueError, lambda: resource.methods)

    def test_content_types_default(self):
        request = DummyRequest({})
        resource = Resource()
        self.assertEqual(resource.content_types(request), set())
        self.assertEqual(
            resource.content_types(request, default="foo"), set(["foo"]))

    def test_content_types(self):
        request = DummyRequest({"content-type": "foo"})
        resource = Resource()
        self.assertEqual(resource.content_types(request), set(["foo"]))

    def test_put_child(self):
        root = Resource()
        child = Resource()
        self.assertIs(child, root.putChild("", child))

    def test_error_html(self):
        request = DummyRequest({"content-type": "text/html"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "error_html")
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertTrue(request.finished)
        self.assertIn("DOCTYPE html", request.data[0])
        self.assertIn("error_html", request.data[0])

    def test_error_json(self):
        request = DummyRequest({"content-type": "application/json"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "error_json")
        self.assertEqual(request.responseCode, BAD_REQUEST)
        self.assertTrue(request.finished)
        self.assertEqual(request.data[0], {"error": "error_json"})

    def test_error_unhandled(self):
        request = DummyRequest({"content-type": "foobar"})
        resource = Resource()
        resource.error(request, BAD_REQUEST, "internal_error")
        self.assertEqual(request.responseCode, INTERNAL_SERVER_ERROR)
        self.assertTrue(request.finished)
        self.assertEqual(
            request.data[0],
            {"error": "Can only handle text/html or application/json here"})


