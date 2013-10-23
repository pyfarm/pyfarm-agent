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
Base Service
============
"""

from twisted.application.service import Service as TwistedService


class UnprivilegedService(TwistedService):
    """Base service for all service application in PyFarm's agent"""
    def __init__(self, options):
        self.options = options

    def startService(self):
        raise NotImplementedError

    def stopService(self):
        raise NotImplementedError