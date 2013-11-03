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
Manager Protocols
=================

Protocols specific to the manager's services.
"""

from twisted.internet import reactor
from twisted.python import log

from pyfarm.agent.protocols import IPCReceiverProtocolBase
from pyfarm.agent.utility.structures import protobuf_from_error


class IPCProtocol(IPCReceiverProtocolBase):
    """handles each individual bit of incoming data"""

    def reply(self, protobuf):
        self.transport.write(protobuf.SerializeToString())
        self.transport.loseConnection()

    def rawDataReceived(self, data):
        log.msg("receiving message from %s:%s" % self.transport.client)
        self.factory.known_hosts.add(self.transport.client[0])

        try:
            pb = self.protobuf()
            pb.ParseFromString(data)

        except Exception, e:
            log.err("error in message from %s: %s" % (self.transport.client, e))
            data = protobuf_from_error(e)
            reactor.callLater(0, self.reply, data)

        else:
            data = self.protobuf()
            # TODO: go off and do something with the request in a deferred
            # TODO: pass the transport along so we know who to reply to
            # TODO: OR start action, return 'starting' send reply later (probably better)
