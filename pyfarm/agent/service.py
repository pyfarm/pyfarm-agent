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
Core Services
=============

Contains the core classes and objects necessary to construct a new service.  In
addition these objects will also:

    * handle basic command line parsing
    * statsd client setup
"""

from functools import partial

import statsd
from twisted.python import log, usage
from twisted.application.service import MultiService as _MultiService

from pyfarm.core.utility import convert
from pyfarm.agent.utility.tasks import ScheduledTaskManager


class Options(usage.Options):
    """
    Base options for all services classes.

    .. note::
        unlike standard Python classes, :class:`.usage.Options` overriding
        any class level attribute through inheritance will not replace
        the resulting data.
    """
    optParameters = [
        ("statsd", "s", "127.0.0.1:8125",
         "host to send stats information to, this will not result in errors if "
         "the service non-existent or down"),
        ("ipc-port", "i", None,
         "the local port to run the inter process communication on for this "
         "service")]


class MultiService(_MultiService):
    """
    Core service object which should be subclassed by other services.  This
    class is instanced whenever twistd runs and does a few things:
        * ensures certain commandline parameters are provided defauls
        * prepares a statsd client
    """
    DEFAULT_IPC_PORT = NotImplemented

    def __init__(self, options):
        _MultiService.__init__(self)
        self.options = options
        self.config = {}
        self.scheduled_tasks = ScheduledTaskManager()
        self.log = partial(log.msg, system=self.__class__.__name__)

        # TODO: add statsd setup
        # TODO: add statsd argument parsing

        # iterate over all options and run them through
        # a converter
        for key, value in self.options.items():
            self.config[key] = self.convert_option(key, value)

        import logging
        self.log("options: %s" % self.options, level=logging.WARNING)

    def convert_option(self, key, value):
        """
        Converts a key/value command line pair.  Mainly, this is used to fall
        back onto default values or convert incoming command line arguments
        to something Twisted can use.
        """
        if key == "ipc-port":
            if self.DEFAULT_IPC_PORT is NotImplemented:
                raise NotImplementedError(
                    "`%s.DEFAULT_IPC_PORT` not set" % self.__class__.__name__)
            elif value is None:
                return self.DEFAULT_IPC_PORT
            else:
                return convert.stoi(value)

        # default action, return original value
        return value

    def startService(self):
        self.log("starting service")
        self.scheduled_tasks.start()

    def stopService(self):
        self.log("stopping service")
        self.scheduled_tasks.stop()
