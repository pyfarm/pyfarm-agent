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

"""
Logger
------

Agent specific logging library that combines some of
Twisted's and Python's logging facilities.
"""

import re

from logging import (
    DEBUG, INFO, WARNING, ERROR, CRITICAL,
    Logger as _Logger, getLogger as _getLoggerPython)

from twisted.python.log import (
    PythonLoggingObserver as TwistedLogObserver, msg, textFromEventDict)

from pyfarm.core.logger import getLogger as _getLogger

twisted_logger = _getLoggerPython("twisted")


class PythonLoggingObserver(TwistedLogObserver):
    """
    A replacement for :class:`twisted.python.log.PythonLoggingObserver` which
    redirect logging events to the appropriate logger instead of a global
    logger.
    """
    STARTED = False

    # This regular expression pull information out of an http
    # log line so we can present the most relevant, non-duplicated,
    # information.
    REGEX_HTTP_MESSAGE = re.compile(
        '^.*- - \[.*\] \"(GET|POST|HEAD|PUT|DELETE) '
        '(.*) HTTP/\d[.]\d\" (\d{3}).*\".*\" \"(.*)\"')

    # Maps system names from an incoming event to the
    # real logger name.
    event_system_names = {
        "Uninitialized": "twisted",
        "-": "twisted",
        "pyfarm.agent.http.server.Site": "pf.agent.http"}

    # For any system even we don't a default name for, because
    # it's not given above, we use these regular expressions to
    # find the 'final' logger name.
    regex_remappers = {
        re.compile("^HTTP.*ClientProtocol.*$"): "twisted",
        re.compile("^HTTPChannel.*$"): "pf.agent.http"}

    # This class may need to create new logger
    # instances when a message comes in.  Those instances
    # will be stored here.
    loggers = {
        "-": _getLoggerPython("twisted")}

    filters = {
        "pf.agent.http": set([re.compile("^(Starting|Stopping) factory .*$")]),
        "twisted": set([re.compile("^(Starting|Stopping) factory .*$")])}

    def start(self):
        # only want to start once
        if self.STARTED:
            return

        super(PythonLoggingObserver, self).start()
        self.STARTED = True

    def emit(self, event):
        system = event["system"]
        if system in self.event_system_names:
            system = self.event_system_names[system]

        # If we've never seen this system name before see if we
        # can remap it to a proper name using the regular expression
        # names.
        else:
            for regex, value in self.regex_remappers.iteritems():
                if regex.match(system):
                    system = self.event_system_names[system] = value
                    break
            else:
                self.loggers["-"].log(WARNING, "Unknown logger %s'" % system)
                self.event_system_names[system] = "twisted"

        # Do we care about this message?
        text = textFromEventDict(event)
        filters = self.filters.get(system)
        if filters:
            for regex in filters:
                if regex.search(text):
                    return

        # Determine what the real level should be
        if "logLevel" in event:
            level = event["logLevel"]
        elif event["isError"]:
            level = ERROR
        else:
            level = INFO

        # If this is a http log then we should reformat it
        if system == "pf.agent.http" or system[:8] == "twisted":
            system = "pf.agent.http"  # because this could be a twisted log
            matched = self.REGEX_HTTP_MESSAGE.match(text)
            if matched:
                try:
                    code = int(matched.group(3))
                    text = " ".join(matched.groups())

                    if code >= 400:
                        level = ERROR
                    else:
                        level = DEBUG

                except Exception as e:
                    error = "Failed to convert http log: %s" % e
                    self.loggers[system].log(ERROR, error)

        # Create a logger if necessary
        if system not in self.loggers:
            self.loggers[system] = _getLoggerPython(system)

        # Emit the message to the underlying logger
        self.loggers[system].log(level, text)


class Logger(object):
    """
    This is a stand-in for :class:`logging.Logger` that internally emits
    log messages to :meth:`.msg` with the proper level and system name.

    Neither formatting nor level control should happen here, that should
    instead happen on the Python loggers themselves.
    """
    def __init__(self, system, logger):
        assert isinstance(logger, _Logger)
        self.system = system
        self._logger = logger

    def setLevel(self, level):
        self._logger.setLevel(level)

    def debug(self, message, *args):
        msg(message % args, system=self.system, logLevel=DEBUG)

    def info(self, message, *args):
        msg(message % args, system=self.system, logLevel=INFO)

    def warning(self, message, *args):
        msg(message % args, system=self.system, logLevel=WARNING)

    def error(self, message, *args):
        msg(message % args, system=self.system, logLevel=ERROR)

    def critical(self, message, *args):
        msg(message % args, system=self.system, logLevel=CRITICAL)


# module reload protection, we only want OBSERVER
# instanced once
try:
    OBSERVER
except NameError:
    OBSERVER = PythonLoggingObserver()


def start_logging():
    """
    Gets the base agent logger setup and then establishes and observer that
    we can emit log messages to.  You should only need to run this method
    once per process.
    """
    _getLogger("agent")  # setup the global agent logger first
    OBSERVER.start()


def getLogger(name):
    """
    Instances and returns a :class:`.Logger` object with the proper
    name.  New loggers should always be created using this function.

    :raises RuntimeError:
        raised if this function is called before the observer has
        been started with :func:`start_logging`
    """
    if not OBSERVER.STARTED:
        raise RuntimeError("observer not yet started")

    logger = _getLogger(name)
    OBSERVER.loggers[logger.name] = logger
    return Logger(logger.name, logger)
