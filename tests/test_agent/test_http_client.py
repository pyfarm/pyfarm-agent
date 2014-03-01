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

from httplib import responses, OK

from pyfarm.core.config import read_env
from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.http.client import (
    Request, Response, request, head, get, post, put, patch, delete)


class TestPartials(TestCase):
    def test_head(self):
        self.assertIs(head.func, request)
        self.assertEqual(head.args, ("HEAD", ))

    def test_get(self):
        self.assertIs(get.func, request)
        self.assertEqual(get.args, ("GET", ))

    def test_post(self):
        self.assertIs(post.func, request)
        self.assertEqual(post.args, ("POST", ))

    def test_put(self):
        self.assertIs(put.func, request)
        self.assertEqual(put.args, ("PUT", ))

    def test_patch(self):
        self.assertIs(patch.func, request)
        self.assertEqual(patch.args, ("PATCH", ))

    def test_delete(self):
        self.assertIs(delete.func, request)
        self.assertEqual(delete.args, ("DELETE", ))


class TestRequestAssertions(TestCase):
    def test_invalid_method(self):
        self.assertRaises(AssertionError, lambda: request("", ""))

    def test_invalid_uri_type(self):
        self.assertRaises(AssertionError, lambda: request("GET", None))

    def test_invalid_empty_uri(self):
        self.assertRaises(AssertionError, lambda: request("GET", ""))

    def test_invalid_callback_type(self):
        self.assertRaises(AssertionError,
                          lambda: request("GET", "/", callback=""))

    def test_invalid_errback_type(self):
        self.assertRaises(AssertionError,
                          lambda: request("GET", "/", errback=""))

    def test_invalid_header_value_length(self):
        self.assertRaises(AssertionError,
                          lambda: request(
                              "GET", "/", callback=lambda: _,
                              headers={"foo": ["a", "b"]}))

    def test_invalid_header_value_type(self):
        self.assertRaises(NotImplementedError,
                          lambda: request(
                              "GET", "/", callback=lambda: _,
                              headers={"foo": None}))


class RequestTestCase(TestCase):
    BASE_URL = read_env(
        "PYFARM_AGENT_TEST_URL", "https://httpbin.org")

    @classmethod
    def get_url(cls, url):
        assert isinstance(url, STRING_TYPES)
        return cls.BASE_URL + ("/%s" % url if not url.startswith("/") else url)

    @classmethod
    def get(cls, url, **kwargs):
        kwargs.setdefault("persistent", False)
        return get(cls.get_url(url), **kwargs)

    @classmethod
    def post(cls, url, **kwargs):
        kwargs.setdefault("persistent", False)
        return post(cls.get_url(url), **kwargs)

    @classmethod
    def put(cls, url, **kwargs):
        kwargs.setdefault("persistent", False)
        return put(cls.get_url(url), **kwargs)

    @classmethod
    def delete(cls, url, **kwargs):
        kwargs.setdefault("persistent", False)
        return delete(cls.get_url(url), **kwargs)

    def assert_response(self, response, code,
                        content_type=None, user_agent=None):
        if content_type is None:
            content_type = ["application/json"]

        if user_agent is None:
            user_agent = ["PyFarm (agent) 1.0"]

        # check some of the attribute we expect
        # against data coming back from the server
        try:
            data = response.json()
            self.assertEqual(data["headers"]["User-Agent"], user_agent[0])

            # even if we're making a https request the underlying
            # url might be http
            self.assertIn(data["url"], set([
                response.request.uri,
                response.request.uri.replace("https", "http")]))
        except ValueError:
            pass

        # check the types under the hod
        self.assertIsInstance(response, Response)
        self.assertIsInstance(response.request, Request)
        self.assertIsInstance(response.headers, dict)

        # return code check
        self.assertIn(code, responses)
        self.assertEqual(response.code, code)
        self.assertEqual(response.code, response.response.code)

        # ensure our request and response attributes match headers match
        self.assertEqual(response.headers["Content-Type"], content_type)
        self.assertEqual(response.headers["User-Agent"], user_agent)
        self.assertEqual(response.content_type, content_type[0])
        self.assertEqual(response.headers, response.request.headers)


class TestGet(RequestTestCase):
    def test_get_data(self):
        def callback(response):
            data = response.json()
            self.assert_response(response, OK)
            self.assertIn("foo", data["args"])
            self.assertEqual(data["args"]["foo"], "1")

        d = self.get("/get?foo=1", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_get_header(self):
        def callback(response):
            data = response.json()
            self.assert_response(response, OK)
            self.assertEqual(response.headers["X-Foo"], ["True"])
            print data
            self.assertIn("foo", data["args"])
            self.assertEqual(data["args"]["foo"], "1")

        d = self.get(
            "/get?foo=1", callback=callback, headers={"X-Foo": str(True)})
        d.addBoth(lambda r: self.assertIsNone(r))
        return d
