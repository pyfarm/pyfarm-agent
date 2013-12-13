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
Spawn
-----

Responsible for spawning processes, attaching their protocol
objects, and using jobtypes.
"""

from twisted.internet import protocol
from twisted.internet import reactor


class MyPP(protocol.ProcessProtocol):
    def outReceived(self, data):
        print "stdout: %s" % data

    def errReceived(self, data):
        print "stderr: %s" % data

    def errConnectionLost(self):
        print "errConnectionLost! The child closed their stderr."

    def processExited(self, reason):
        print "processExited, status %d" % (reason.value.exitCode,)

    def processEnded(self, reason):
        print "processEnded, status %d" % (reason.value.exitCode,)
        print "quitting"
        reactor.stop()

pp = MyPP()
a = reactor.spawnProcess(pp, "ping", ["ping", "-c", "1", "localhost"], {})
reactor.run()