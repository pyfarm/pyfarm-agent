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

from httplib import FORBIDDEN
from os.path import exists

from twisted.web.server import Site as _Site
from twisted.web.static import File
from twisted.web.error import Error

from pyfarm.agent.http.resource import Request


class Site(_Site):
    """
    Site object similar to Twisted's except it also carries along
    some of the internal agent data.
    """
    requestFactory = Request
    displayTracebacks = False

    def __init__(self, resource, logPath=None, timeout=60*60*12):
        _Site.__init__(self, resource, logPath=logPath, timeout=timeout)


class StaticPath(File):
    """
    More secure version of :class:`.File` that does not list
    directories.  In addition this will also sending along a
    response header asking clients to cache to data.
    """
    EXPIRES = 604800  # 7 days
    ALLOW_DIRECTORY_LISTING = False

    def __init__(
            self, path, defaultType="text/html", ignoredExts=(),
            registry=None, allowExt=0):

        if not exists(path):
            raise OSError("%s does not exist" % path)

        File.__init__(self, path, defaultType=defaultType,
                      ignoredExts=ignoredExts, registry=registry,
                      allowExt=allowExt)

    def render(self, request):
        """overrides :meth:`.File.render` and sets the expires header"""
        request.setHeader("Cache-Control", "max-age=%s" % self.EXPIRES)
        return File.render(self, request)

    def directoryListing(self):
        """override which ensures directories cannot be listed"""
        if not self.ALLOW_DIRECTORY_LISTING:
            raise Error(FORBIDDEN, "directory listing is not allowed")
        return File.directoryListing(self)

