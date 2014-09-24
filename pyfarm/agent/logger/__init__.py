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
Logger
======

This module provides the functionality for handling log
messages in the agent.
"""

from logging import DEBUG, getLogger as _getLogger

from twisted.python.log import startLoggingWithObserver

from pyfarm.agent.logger.twistd import Observer
from pyfarm.agent.logger.python import LogRecordToTwisted, getLogger

# NOTE: Normally we don't place code in __init__.  In this case we're doing
# so because we don't want to change the existing interface for getLogger
# and we only want to expose two functions.

__all__ = ["setup_logging"]


def setup_logging():
    """
    Sets up Twisted's and Python's logging system
    to run through our observer.  This ensures that all
    log messages are handled in a single location and
    that we don't lose any messages.
    """
    if Observer.INSTANCE is None:
        # Setup and configure the logger for Twisted
        observer = Observer()
        startLoggingWithObserver(observer, setStdout=False)

        # Ensure all of Python's log flow into Twisted
        root = _getLogger("")
        root.setLevel(DEBUG)
        root.handlers[:] = [LogRecordToTwisted()]

        # Now that we've configured the observer import the
        # the config and configure the observer itself
        observer.configure()
        Observer.INSTANCE = observer
