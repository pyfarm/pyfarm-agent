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
import os
from collections import namedtuple
from httplib import responses, OK
from urllib import quote

from twisted.internet.defer import Deferred
from twisted.internet.error import DNSLookupError
from twisted.internet.protocol import Protocol, connectionDone
from twisted.web.error import SchemeNotSupported
from twisted.web.client import Response as TWResponse, Headers, ResponseDone
from pyfarm.core.config import read_env
from pyfarm.core.enums import STRING_TYPES

from pyfarm.agent.testutil import TestCase
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import (
    Request, Response, request, head, get, post, put, patch, delete, build_url,
    http_retry_delay)


# fake object we use for triggering Response.connectionLost
responseDone = namedtuple("reason", ["type"])(type=ResponseDone)


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

    def test_invalid_url_type(self):
        self.assertRaises(AssertionError, lambda: request("GET", None))

    def test_invalid_empty_url(self):
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
                              "GET", "/", callback=lambda: None,
                              headers={"foo": ["a", "b"]}))

    def test_invalid_header_value_type(self):
        self.assertRaises(NotImplementedError,
                          lambda: request(
                              "GET", "/", callback=lambda: None,
                              headers={"foo": None}))


class RequestTestCase(TestCase):
    HTTP_SCHEME = read_env(
        "PYFARM_AGENT_TEST_HTTP_SCHEME", "http")
    BASE_URL = read_env(
        "PYFARM_AGENT_TEST_URL", "%(scheme)s://httpbin.org")
    REDIRECT_TARGET = read_env(
        "PYFARM_AGENT_TEST_REDIRECT_TARGET", "http://example.com")
    base_url = BASE_URL % {"scheme": HTTP_SCHEME}

    def setUp(self):
        super(RequestTestCase, self).setUp()
        config["persistent-http-connections"] = False

    def get_url(self, url):
        assert isinstance(url, STRING_TYPES)
        return self.base_url + ("/%s" % url if not url.startswith("/") else url)

    def get(self, url, **kwargs):
        return get(self.get_url(url), **kwargs)

    def post(self, url, **kwargs):
        return post(self.get_url(url), **kwargs)

    def put(self, url, **kwargs):
        return put(self.get_url(url), **kwargs)

    def delete(self, url, **kwargs):
        return delete(self.get_url(url), **kwargs)

    def assert_response(self, response, code,
                        content_type=None, user_agent=None):
        if content_type is None:
            content_type = "application/json"

        if user_agent is None:
            user_agent = ["PyFarm (agent) 1.0"]

        # check some of the attribute we expect
        # against data coming back from the server
        try:
            data = response.json()
            if "headers" in data:
                self.assertEqual(data["headers"]["User-Agent"], user_agent[0])

            # even if we're making a https request the underlying
            # url might be http
            if "url" in data:
                self.assertIn(data["url"], (
                    response.request.url,
                    response.request.url.replace("https", "http")))
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
        if "headers" in response.request.kwargs:
            self.assertEqual(
                response.request.kwargs["headers"]["User-Agent"], user_agent)
        self.assertEqual(response.content_type, content_type)


class TestClientErrors(RequestTestCase):
    def test_unsupported_scheme(self):
        self.base_url = ""
        return self.get(
            "/get",
            callback=lambda _: None,
            errback=lambda failure:
                self.assertIs(failure.type, SchemeNotSupported))

    def test_unknown_hostname(self):
        self.base_url = self.HTTP_SCHEME + "://" + os.urandom(32).encode("hex")
        return self.get(
            "/",
            callback=lambda _: None,
            errback=lambda failure:
                self.assertIs(failure.type, DNSLookupError))


class TestRetryDelay(TestCase):
    def test_default(self):
        config["http-retry-delay"] = 1
        self.assertEqual(
            http_retry_delay(uniform=True), config["http-retry-delay"])

    def test_custom_delay_multiplier(self):
        self.assertEqual(
            http_retry_delay(initial=1, uniform=False, get_delay=lambda: 2), 2)

    def test_minimum(self):
        self.assertEqual(
            http_retry_delay(
                initial=5, uniform=True, get_delay=lambda: 1, minimum=3), 5)
        self.assertEqual(
            http_retry_delay(
                initial=0, uniform=True, get_delay=lambda: 1, minimum=10), 10)

    def test_invalid_types(self):
        self.assertRaises(
            AssertionError, lambda: http_retry_delay(initial=""))
        self.assertRaises(
            AssertionError, lambda: http_retry_delay(minimum=""))


class TestClientFunctions(RequestTestCase):
    def test_auth(self):
        username = os.urandom(6).encode("hex")
        password = os.urandom(6).encode("hex")

        def callback(response):
            self.assert_response(response, OK)
            self.assertEqual(
                response.json(),
                {"authenticated": True, "user": username})

        d = self.get(
            "/basic-auth/%s/%s" % (username, password),
            callback=callback, auth=(username, password),)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_redirect(self):
        def callback(response):
            self.assert_response(response, OK, content_type="text/html")
            self.assertIn("<title>Example Domain</title>", response.data())

        d = self.get(
            "/redirect-to?url=%s" % self.REDIRECT_TARGET,
            callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_gzipped(self):
        def callback(response):
            self.assert_response(response, OK)
            self.assertTrue(response.json()["gzipped"])

        d = self.get("/gzip", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_retry(self):
        self.retried = False

        def callback(response):
            self.assert_response(response, OK)

            if not self.retried:
                retry = response.request.retry()
                self.assertIsInstance(retry, Deferred)
                self.retried = True
                return retry
            else:
                self.assertEqual(response.request.url, self.get_url("/get"))
                self.assertEqual(response.request.method, "GET")

        # special cleanup step to test self.retried
        self.addCleanup(lambda: self.assertTrue(self.retried, "not retried"))

        d = self.get("/get", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d


class TestMethods(RequestTestCase):
    def test_get(self):
        def callback(response):
            self.assert_response(response, OK)

        d = self.get("/get", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_response_header(self):
        key = os.urandom(6).encode("hex")
        value = os.urandom(6).encode("hex")

        def callback(response):
            self.assertEqual(
                response.response.headers.getRawHeaders(key), [value])

        d = self.get(
            "/response-headers?%s=%s" % (key, value), callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_request_header(self):
        key = "X-" + os.urandom(6).encode("hex")
        value = os.urandom(6).encode("hex")

        def callback(response):
            data = response.json()
            self.assert_response(response, OK)
            self.assertEqual(response.request.kwargs["headers"][key], [value])

            # case insensitive comparison
            for k, v in data["headers"].items():
                if k.lower() == key.lower():
                    self.assertEqual(v, value)
                    break

        d = self.get("/get", callback=callback, headers={key: value})
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_url_argument(self):
        key = "X-" + os.urandom(6).encode("hex")
        value = os.urandom(6).encode("hex")

        def callback(response):
            data = response.json()
            self.assert_response(response, OK)
            self.assertEqual(data["args"], {key: value})

        d = self.get("/get?%s=%s" % (key, value), callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_post(self):
        key = os.urandom(6).encode("hex")
        value = os.urandom(6).encode("hex")

        def callback(response):
            self.assertEqual(response.json()["json"], {key: value})
            self.assert_response(response, OK)

        d = self.post("/post", callback=callback, data={key: value})
        d.addBoth(lambda r: self.assertIsNone(r))
        return d

    def test_delete(self):
        def callback(response):
            self.assert_response(response, OK)

        d = self.delete("/delete", callback=callback)
        d.addBoth(lambda r: self.assertIsNone(r))
        return d


class TestResponse(RequestTestCase):
    def setUp(self):
        RequestTestCase.setUp(self)
        self.callbacks = set()

    def test_instance_type(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        self.assertIsInstance(r, Protocol)

    def test_attributes(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        self.assertIs(r._deferred, deferred)
        self.assertIs(r.request, request)
        self.assertIs(r.response, twisted_response)
        self.assertEqual(r.method, request.method)
        self.assertEqual(r.url, request.url)
        self.assertEqual(r.code, twisted_response.code)
        self.assertEqual(r.content_type, "application/json")

    def test_headers(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})
        r = Response(deferred, twisted_response, request)
        self.assertEqual(r.headers, {"Content-Type": "application/json"})
        self.assertEqual(r.request.kwargs["headers"],
                         {"Content-Type": "application/json"})

    def test_data_received(self):
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)

        r = Response(deferred, twisted_response, request)
        self.assertEqual(r._body, "")
        data = os.urandom(6).encode("hex")
        r.dataReceived(data)
        self.assertEqual(r._body, data)

    def test_response_done(self):
        deferred = Deferred()
        deferred.addCallback(lambda _: self.callbacks.add("success"))
        deferred.addErrback(lambda _: self.callbacks.add("failure"))
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        r.connectionLost(responseDone)
        self.assertEqual(self.callbacks, set(["success"]))
        self.assertTrue(r._done)
        return deferred

    def test_connection_done(self):
        deferred = Deferred()
        deferred.addCallback(lambda _: self.callbacks.add("success"))
        deferred.addErrback(lambda _: self.callbacks.add("failure"))
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        r.connectionLost(connectionDone)
        self.assertEqual(self.callbacks, set(["failure"]))
        self.assertFalse(r._done)
        return deferred

    def test_data_early(self):
        deferred = Deferred()
        deferred.addCallback(lambda _: self.callbacks.add("success"))
        deferred.addErrback(lambda _: self.callbacks.add("failure"))
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        self.assertRaises(RuntimeError, lambda: r.data())

    def test_data(self):
        deferred = Deferred()
        deferred.addCallback(lambda _: self.callbacks.add("success"))
        deferred.addErrback(lambda _: self.callbacks.add("failure"))

        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})
        data1 = os.urandom(6).encode("hex")
        data2 = os.urandom(6).encode("hex")
        data = data1 + data2
        r = Response(deferred, twisted_response, request)
        r.dataReceived(data1)
        r.dataReceived(data2)
        r.connectionLost(responseDone)
        self.assertEqual(self.callbacks, set(["success"]))
        self.assertTrue(r._done)
        self.assertEqual(r.data(), data)
        return deferred

    def test_json_early(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        r = Response(deferred, twisted_response, request)
        self.assertRaises(RuntimeError, lambda: r.json())

    def test_json_wrong_content_type(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["text/html"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "text/html"}, "data": None})

        r = Response(deferred, twisted_response, request)
        r._done = True
        self.assertRaisesRegexp(
            ValueError, "Not an application/json response\.", lambda: r.json())

    def test_json_decoding_error(self):
        deferred = Deferred()
        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})
        r = Response(deferred, twisted_response, request)
        r._done = True
        self.assertRaisesRegexp(
            ValueError, "No JSON object could be decoded", lambda: r.json())

    def test_json(self):
        deferred = Deferred()
        deferred.addCallback(lambda _: self.callbacks.add("success"))
        deferred.addErrback(lambda _: self.callbacks.add("failure"))

        twisted_headers = Headers({"Content-Type": ["application/json"]})
        twisted_response = TWResponse(
            ("HTTP", 1, 1), 200, "OK", twisted_headers, None)
        request = Request(
            method="GET", url="/",
            kwargs={
                "headers": {
                    "Content-Type": "application/json"}, "data": None})

        data = {
            os.urandom(6).encode("hex"): os.urandom(6).encode("hex"),
            os.urandom(6).encode("hex"): os.urandom(6).encode("hex")
        }

        r = Response(deferred, twisted_response, request)
        map(r.dataReceived, json.dumps(data))
        r.connectionLost(responseDone)
        self.assertEqual(self.callbacks, set(["success"]))
        self.assertTrue(r._done)
        self.assertEqual(r.json(), data)
        return deferred


class TestBuildUrl(TestCase):
    def test_basic_url(self):
        self.assertEqual(build_url("/foobar"), "/foobar")

    def test_url_with_arguments(self):
        self.assertEqual(
            build_url("/foobar", {"first": "foo", "second": "bar"}),
            "/foobar?first=foo&second=bar")

    def test_quoted_url(self):
        self.assertEqual(
            build_url("/foobar",
                      {"first": "foo", "second": "bar"}, quoted=True),
            "/foobar%3Ffirst%3Dfoo%26second%3Dbar")

    def test_large_random_parameters(self):
        params = {}
        for i in range(50):
            params[os.urandom(24).encode("hex")] = os.urandom(24).encode("hex")

        expected = "/foobar?" + "&".join([
            "%s=%s" % (key, value)for key, value in sorted(params.items())])
        expected_quoted = quote(expected)

        self.assertEqual(
            build_url("/foobar", params), expected)
        self.assertEqual(
            build_url("/foobar", params, quoted=True), expected_quoted)
