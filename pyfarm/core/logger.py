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
    DEBUG, INFO, WARNING, ERROR, CRITICAL, getLogger as _getLogger)

from pyfarm.core.enums import PY26

try:
    from logging import NullHandler
except ImportError:
    from logutils import NullHandler

try:
    from logging import captureWarnings

# Python 2.6 backport of captureWarnings
except ImportError:
    import warnings
    _warnings_showwarning = None

    def _showwarning(message, category, filename, lineno, file=None, line=None):
        """
        .. note::
            This function is a copy of Python 2.7's ``_showwarning``

        Implementation of showwarnings which redirects to logging, which will
        first check to see if the file parameter is None. If a file is
        specified, it will delegate to the original warnings implementation of
        showwarning. Otherwise, it will call warnings.formatwarning and will
        log the resulting string to a warnings logger named "py.warnings" with
        level logging.WARNING.
        """
        if file is not None:
            if _warnings_showwarning is not None:
                _warnings_showwarning(
                    message, category, filename, lineno, file, line)
        else:
            s = warnings.formatwarning(
                message, category, filename, lineno, line)
            logger = _getLogger("py.warnings")
            if not logger.handlers:
                logger.addHandler(NullHandler())
            logger.warning("%s", s)

    def captureWarnings(capture):
        """
        .. note::
            This function is a copy of Python 2.7's ``captureWarnings``

        If capture is true, redirect all warnings to the logging package.
        If capture is False, ensure that warnings are not redirected to logging
        but to their original destinations.
        """
        global _warnings_showwarning
        if capture:
            if _warnings_showwarning is None:
                _warnings_showwarning = warnings.showwarning
                warnings.showwarning = _showwarning
        else:
            if _warnings_showwarning is not None:
                warnings.showwarning = _warnings_showwarning
                _warnings_showwarning = None


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

if not PY26:
    from logging import Formatter
else:
    from logutils import Formatter


class ColorFormatter(Formatter):
    """Adds colorized formatting to log messages using :mod:`colorama`"""
    FORMATS = {
        INFO: (Style.BRIGHT, Style.RESET_ALL),
        WARNING: (Fore.YELLOW, Fore.RESET),
        ERROR: (Fore.RED, Fore.RESET),
        CRITICAL: (Fore.RED + Style.BRIGHT, Fore.RESET + Style.RESET_ALL)
    }

    def formatMessage(self, record):
        head, tail = self.FORMATS.get(record.levelno, ("", ""))
        return head + super(ColorFormatter, self).formatMessage(record) + tail

# TODO: use dictConfig() to set this up
HANDLER = logging.StreamHandler(sys.stdout)
HANDLER.setFormatter(ColorFormatter())

# TODO: use dictConfig() to set this up
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
