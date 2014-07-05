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
HTTP Server
-----------

HTTP server responsible for serving requests that
control or query the running agent.  This file produces
a service that the  :class:`pyfarm.agent.manager.service.ManagerServiceMaker`
class can consume on start.
"""

import re

try:
    from httplib import FORBIDDEN
except ImportError:  # pragma: no cover
    from http.client import FORBIDDEN

from os.path import exists

from twisted.web.server import Site as _Site, Request as _Request
from twisted.web.static import File
from twisted.web.error import Error

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.utility import dumps


class RewriteRequest(_Request):
    """
    A custom implementation of :class:`._Request` that will allow us
    to modify an incoming request before it reaches the HTTP server..
    """
    REPLACE_REPEATED_DELIMITER = re.compile("/{2,}")

    def requestReceived(self, command, path, version):
        """
        Override the built in :meth:`._Request.requestReceived` so we
        can rewrite portions of the request, such as the url, before it's
        passed along to the internal server.
        """
        # before we give the path to Twisted, replace any
        # repeated `/`s with `/`
        if "//" in path:
            path = self.REPLACE_REPEATED_DELIMITER.sub("/", path)
        _Request.requestReceived(self, command, path, version)

    def write(self, data):
        """
        Override the built in :meth:`._Request.write` so that any data
        that's not a string will be dumped to json using :func:`.dumps`
        """
        if not isinstance(data, STRING_TYPES):
            data = dumps(data)

        _Request.write(self, data)


class Site(_Site):
    """
    Site object similar to Twisted's except it also carries along
    some of the internal agent data.
    """
    displayTracebacks = True
    requestFactory = RewriteRequest


class StaticPath(File):
    """
    More secure version of :class:`.File` that does not list
    directories.  In addition this will also sending along a
    response header asking clients to cache to data.
    """
    EXPIRES = 604800  # 7 days
    ALLOW_DIRECTORY_LISTING = False

    def __init__(self, *args, **kwargs):
        File.__init__(self, *args, **kwargs)

        if not exists(self.path):
            raise OSError("%s does not exist" % self.path)

    def render(self, request):
        """Overrides :meth:`.File.render` and sets the expires header"""
        request.responseHeaders.addRawHeader(
            "Cache-Control", "max-age=%s" % self.EXPIRES)
        return File.render(self, request)

    def directoryListing(self):
        """Override which ensures directories cannot be listed"""
        if not self.ALLOW_DIRECTORY_LISTING:
            raise Error(FORBIDDEN, "directory listing is not allowed")
        return File.directoryListing(self)

