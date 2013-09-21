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

DEFAULT_LEVEL = logging.DEBUG

# setup the root logger for PyFarm
NAME_SEP = 15
ROOT_FORMAT = logging.Formatter(
    fmt="[%(asctime)s] %(name)"+str(NAME_SEP)+"s %(levelname)s - %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S")
ROOT_HANDLER = logging.StreamHandler(sys.stdout)
ROOT_HANDLER.setFormatter(ROOT_FORMAT)

# create the root logger and apply the handler
root = logging.getLogger("pyfarm")
root.addHandler(ROOT_HANDLER)
root.propagate = 0  # other handlers should not process our messages (for now)
root.setLevel(DEFAULT_LEVEL)


def getLogger(name):
    """
    Wrapper around the :func:`logging.getLogger` function which
    ensures the name is setup properly.
    """
    logger = logging.getLogger(".".join([root.name, name]))
    logger.setLevel(DEFAULT_LEVEL)
    return logger