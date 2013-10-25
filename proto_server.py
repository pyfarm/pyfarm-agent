from twisted.python import log
from twisted.application.service import Application
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

from pyfarm.agent.utility import pack_result_tuple


class ReceieverProtocol(Protocol):
    known_hosts = set()

    def connectionMade(self):
        address, port = self.transport.client

        # log and keep track of new hosts
        if address not in self.known_hosts:
            log.msg("new connection from %s received" % address)
            self.known_hosts.add(address)

    def dataReceived(self, data):
        self.transport.write(pack_result_tuple(0, data))


class ReceieverFactory(Factory):
    protocol = ReceieverProtocol

    def stopFactory(self):  # TODO: notify all connected hosts we are stopping
        if self.protocol.known_hosts:
            log.msg("notifying all known hosts of termination")


endpoint = TCP4ServerEndpoint(reactor, 9000)
endpoint.listen(ReceieverFactory())

application = Application("server")