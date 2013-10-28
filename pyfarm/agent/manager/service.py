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
Manager Service
===============

Sends and receives information from the master and performs systems level tasks
such as log reading, system information gathering, and management of processes.
"""

from zope.interface import implementer
from twisted.python import log
from twisted.plugin import IPlugin
from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.application import internet
from twisted.application.service import IServiceMaker

from pyfarm.agent.protocols import IPCReceiverProtocolBase
from pyfarm.agent.utility import protobuf_from_error
from pyfarm.agent.service import MultiService, Options


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


class IPCReceieverFactory(Factory):
    """
    Receives incoming connections and hands them off to the protocol
    object.  In addition this class will also keep a list of all hosts
    which have connected so they can be notified upon shutdown.
    """
    protocol = IPCProtocol
    known_hosts = set()

    def stopFactory(self):  # TODO: notify all connected hosts we are stopping
        if self.known_hosts:
            log.msg("notifying all known hosts of termination")


class ManagerService(MultiService):
    """the service object itself"""
    DEFAULT_IPC_PORT = 50001



@implementer(IServiceMaker, IPlugin)
class ManagerServiceMaker(object):
    """
    Main service which which serves as the entry point into the internals
    of the agent.  This services runs an HTTP server for external interfacing
    and an IPC server based on protocol buffers for internal usage.
    """
    tapname = "pyfarm.agent.manager"
    description = __doc__
    options = Options

    def makeService(self, options):
        service = ManagerService(options)

        # ipc service setup
        ipc_factory = IPCReceieverFactory()
        ipc_server = internet.TCPServer(service.options.get("ipc-port"),
                                        ipc_factory)
        ipc_server.setServiceParent(service)

        return service
