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
Logger
======

Contains the root logger for PyFarm as well as the default
message formatting.

:var root:
    The root logger for PyFarm.  Most log messages will run up through this
    logger instead of through their own handlers.
"""

import sys
import logging
from logging import (
    DEBUG, INFO, WARNING, ERROR, CRITICAL, getLogger as _getLogger,
    captureWarnings)

captureWarnings(True)

# re-import protection, only want init() run once
try:
    colorama
except NameError:
    from colorama import init, Fore, Back, Style
    init()

try:
    from logging.config import dictConfig
except ImportError:
    from logutils.dictconfig import dictConfig

# grab the proper formatter
from pyfarm.core.enums import PY26
if not PY26:
    from logging import Formatter
else:
    from logutils import Formatter


class ColorFormatter(Formatter):
    def formatMessage(self, record):
        message = super(ColorFormatter, self).formatMessage(record)
        if record.levelno == DEBUG:
            return message
        elif record.levelno == INFO:
            return Style.BRIGHT + message + Style.RESET_ALL
        elif record.levelno == WARNING:
            return Fore.YELLOW + message + Fore.RESET
        elif record.levelno == ERROR:
            return Fore.RED + message + Fore.RESET
        elif record.levelno == CRITICAL:
            return \
                Fore.RED + Style.BRIGHT + message + Fore.RESET + Style.RESET_ALL
        else:
            return message

HANDLER = logging.StreamHandler(sys.stdout)
HANDLER.setFormatter(ColorFormatter())

root = logging.getLogger()
root.addHandler(HANDLER)
root.setLevel(DEBUG)


def getLogger(name):
    """
    Wrapper around the :func:`logging.getLogger` function which
    ensures the name is setup properly.
    """
    if not name.startswith("pyfarm."):
        name = "pyfarm.%s" % name

    return _getLogger(name)
