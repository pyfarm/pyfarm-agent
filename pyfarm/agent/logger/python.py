# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Python Logger
-------------

This module provides the facilities to capture and send
log records from Python's logger into Twisted.  It also
provides a :class:`Logger` class and :func:`getLogger`
function to replace the built-in Python implementations.
"""

from time import time
from logging import (
    NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL, FATAL, Handler)

from twisted.python.log import msg


class Logger(object):
    """
    A stand-in for an instance of :class:`logging.Logger`
    Unlike the standard logger this just forwards all messages
    to Twisted's logging system.
    """
    def __init__(self, name):
        self.name = name
        self.disabled = False

    def debug(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=DEBUG)

    def info(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=INFO)

    def warning(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=WARNING)

    def error(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=ERROR)

    def critical(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=CRITICAL)

    def fatal(self, message, *args):
        if not self.disabled:
            msg(message, args=args, system=self.name,
                time=time(), logLevel=FATAL)


class LogRecordToTwisted(Handler):
    """
    Captures logging events for a standard Python logger
    and sends them to Twisted.  Twisted has a built in
    class to help work with Python's logging library
    however it won't translate everything directly.
    """
    def __init__(self):
        # We don't use these attributes because the observer
        # handles these.  But we still have to provide them
        # because the Python logger system will try to access
        # them.
        self.level = NOTSET
        self.filters = []

    def emit(self, record):
        """
        Emits an instance of :class:`logging.LogRecord` into Twisted's
        logging system.
        """
        msg(record.msg, args=record.args, python_record=True,
            time=record.created, system=record.name, logLevel=record.levelno)

    def acquire(self):
        pass

    def release(self):
        pass

    def createLock(self):
        pass

    def close(self):
        pass


def getLogger(name):
    """
    Analog to Python's :func:`logging.getLogger` except it
    returns instances of :class:`Logger` instead.
    """
    return Logger("pf.%s" % name)