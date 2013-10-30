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
HTTP Client
===========

The client library the manager uses to communicate with
the master server.
"""

from twisted.internet import reactor
from twisted.web.client import HTTPClientFactory
from twisted.web.client import (
    getPage, HTTPConnectionPool as _HTTPConnectionPool)
from twisted.web.http_headers import Headers
from twisted.internet.ssl import ClientContextFactory as _ClientContextFactory


class ClientContextFactory(_ClientContextFactory):
    """Context factory used for supporting SSL connections"""
    def getContext(self, hostname=None, port=None):
        return ClientContextFactory.getContext(self)


class HTTPConnectionPool(_HTTPConnectionPool):
    """:class:`._HTTPConnectionPool` object without retries"""
    retryAutomatically = False
    maxPersistentPerHost = 1


class HTTPClient(object):
    METHODS = ("GET", "POST", "PUT", "DELETE")

    def __init__(self, base_uri=None, timeout=30):
        self.base_uri = base_uri
        self.timeout = timeout
        self.context_factory = ClientContextFactory()

    def get(self):
        pass

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass

    def request(self, method, uri, data=None, headers=None, auth=None):
        assert method in self.METHODS
        assert headers is None or isinstance(headers, dict)
        assert auth is None or isinstance(auth, tuple)

        # add base_url if the given url does not have
        # a scheme
        if self.base_uri and not uri.startswith("http"):
            uri = self.base_uri + uri

        # add provided headers to a headers object
        if isinstance(headers, dict):
            constructed_headers = Headers()
            for key, value in headers.iteritems():
                constructed_headers.addRawHeader(key, value)
            headers = constructed_headers

        else:
            headers = Headers()

        # add http basic auth
        if isinstance(auth, tuple):
            headers.addRawHeader(
                "Authorization", "Basic %s" % ":".join(auth).encode("base64"))

        kwargs = {"method": method, "postdata": data}
        return getPage(uri, contextFactory=self.context_factory, **kwargs)

