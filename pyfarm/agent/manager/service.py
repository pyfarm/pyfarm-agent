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
Manager Service
===============

Sends and receives information from the master and performs systems level tasks
such as log reading, system information gathering, and management of processes.
"""

from zope.interface import implementer
from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker, Service


class ManagerService(Service):
    def __init__(self, options):
        self.options = options


class Options(usage.Options):
    optParameters = [
        ("http-port", "p",
         50000, "port number to listen for http connections on"),
        ("pb-port", "b",
         50001, "port number to listen for broker connections on"),
        ("remote-retries", None, 20,
         "number times to retry remote requests"),
        ("remote-retries-delay", None, 1,
         "number of seconds in between each remote retry")]


@implementer(IServiceMaker, IPlugin)
class ManagerServiceMaker(object):
    options = Options
    tapname = "pyfarm.agent.manager"
    description = __doc__.split("===============")[-1]  # get doc from module

    def makeService(self, options):
        return ManagerService(options)