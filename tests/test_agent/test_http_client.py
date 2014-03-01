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

from httplib import OK

from pyfarm.core.config import read_env
from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.testutil import TestCase
from pyfarm.agent.http.client import (
    request, head, get, post, put, patch, delete)


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


class TestGet(RequestTestCase):
    def test_basic_get(self):
        def callback(response):
            self.assertEqual(response.code, OK)
            self.assertTrue(True)

        d = self.get("/get", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

# TODO: add request testing, see treq: https://github.com/dreid/treq/tree/master/treq/test
