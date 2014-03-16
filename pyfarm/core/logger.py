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
"""

import os
import sys
import json
import logging
import warnings
from logging import Formatter

# Import or construct the necessary objects depending on the Python version
# and use sys.version_info directly to avoid possible circular import issues.
PY_MAJOR, PY_MINOR = sys.version_info[0:2]
PY26 = PY_MAJOR, PY_MINOR == (2, 6)
if (PY_MAJOR, PY_MINOR) >= (2, 7):
    from logging import NullHandler, captureWarnings
    from logging.config import dictConfig
else:  # pragma: no cover
    from logutils import NullHandler
    from logutils.dictconfig import dictConfig
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
            logger = logging.getLogger("py.warnings")
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

try:
    colorama
except NameError:
    from colorama import init, Fore, Back, Style
    init()


class ColorFormatter(Formatter):
    """Adds colorized formatting to log messages using :mod:`colorama`"""
    FORMATS = {
        logging.DEBUG: (Style.DIM, Style.RESET_ALL),
        logging.WARNING: (Fore.YELLOW, Fore.RESET),
        logging.ERROR: (Fore.RED, Fore.RESET),
        logging.CRITICAL: (
            Fore.RED + Style.BRIGHT, Fore.RESET + Style.RESET_ALL)
    }

    # Python 2.6 uses old style classes which means we can't use
    # super().  So we construct the proper method at the class level
    # so we can safe an if statement for each function call.
    if not PY26:  # pragma: no cover
        def format(self, record):
            head, tail = self.FORMATS.get(record.levelno, ("", ""))
            return head + super(ColorFormatter, self).format(record) + tail
    else:  # pragma: no cover
        def format(self, record):
            head, tail = self.FORMATS.get(record.levelno, ("", ""))
            return head + Formatter.format(self, record) + tail


class config(object):
    """
    Namespace class to store and setup the logging configuration.  You
    typically don't have to run the classmethods here manually but you may
    do so under other circumstances if you wish.
    """
    CONFIGURED = False
    DEFAULT_CONFIGURATION = {
        "version": 1,
        "root": {
            "level": "DEBUG",
            "handlers": ["stdout"],
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "colorized"
            }
        },
        "formatters": {
            "colorized": {
                "()": "pyfarm.core.logger.ColorFormatter",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "format":
                    "%(asctime)s %(levelname)-8s - %(name)-15s - %(message)s"
            }
        }
    }

    @classmethod
    def get(cls):
        """
        Retrieves the logging configuration.  By default this searches
        :envvar:`PYFARM_LOGGING_CONFIG` for either a json blob containing
        the logging configuration or a path to a json blob on disk.  If
        :envvar:`PYFARM_LOGGING_CONFIG` is not set then this function falls
        back on :const:`pyfarm.core.logger.DEFAULT_CONFIGURATION`.
        """
        # nothing left to do if it's not in the environment
        if "PYFARM_LOGGING_CONFIG" not in os.environ:
            return cls.DEFAULT_CONFIGURATION.copy()

        environment_config = os.environ["PYFARM_LOGGING_CONFIG"].strip()
        if not environment_config:
            raise ValueError("$PYFARM_LOGGING_CONFIG is empty")

        try:
            with open(environment_config, "r") as stream:
                try:
                    return json.load(stream)
                except ValueError:
                    raise ValueError(
                        "Failed to parse json data from %s" % stream.name)
        except (OSError, IOError):
            try:
                return json.loads(environment_config)
            except ValueError:
                raise ValueError(
                    "Failed to parse json data from $PYFARM_LOGGING_CONFIG")

    @classmethod
    def setup(cls, capture_warnings=True, reconfigure=False):
        """
        Retrieves the logging configuration using :func:`get` and
        then calls :meth:`.dictConfig` on the results.

        :type capture_warnings: bool
        :param capture_warnings:
            If True then all emissions from :mod:`warnings` should instead
            be logged

        :type reconfigure: bool
        :param reconfigure:
            If True then rerun :func:`.dictConfig` even if we've already done
            so.
        """
        if not reconfigure and cls.CONFIGURED:
            return

        dictConfig(cls.get())
        if capture_warnings:
            captureWarnings(True)

        cls.CONFIGURED = True


def getLogger(name):
    """
    Wrapper around the :func:`logging.getLogger` function which
    ensures the name is setup properly.
    """
    config.setup()
    if not name.startswith("pf."):
        name = "pf.%s" % name

    return logging.getLogger(name)