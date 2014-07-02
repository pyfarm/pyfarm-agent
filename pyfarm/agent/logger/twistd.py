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

from logging import DEBUG, _levelNames
from collections import deque

from twisted.python.log import textFromEventDict


class Observer(object):
    SETUP = False

    def __init__(self):
        self.backlog = deque()
        self.datefmt = None
        self.levels = []

    def configure(self, config):
        self.datefmt = config["logging"]["datefmt"]

        for name, level in config["logging"]["levels"]:
            if not isinstance(level, int):
                try:
                    level = _levelNames[level.upper()]

                except KeyError:
                    raise KeyError(
                        "%r's level %r does not exist" % (name, level))

            self.levels.append((name, level))

        Observer.SETUP = True

    def handle_event(self, event):
        arguments = event.get("args", ())
        level = event.get("logLevel", DEBUG)

        system = event.get("system", "twisted")
        if system == "-":
            system = "twisted"

        text = textFromEventDict(event)
        if text is None or text == "Log opened.":
            return

        # print text % event["args"]

    def __call__(self, event):
        """
        Handles all logging events which are passed into Twisted's logging
        system.
        """
        # If we have not been fully setup yet then
        # processing any messages would not
        if not self.SETUP:
            self.backlog.append(event)
            return

        # If we have anything built up in the backlog
        # then be sure we handle these first
        while self.backlog:
            try:
                self.handle_event(self.backlog.pop())
            except IndexError:
                break

        self.handle_event(event)




