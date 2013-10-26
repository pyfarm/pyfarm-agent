from twisted.python import log
from twisted.application.service import Application
from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

from pyfarm.agent.ipc_pb2 import IPCMessage


class ProtobufProtocol(LineReceiver):
    protobuf = NotImplemented
    line_mode = 0

    def rawDataReceived(self, data):
        raise NotImplementedError


class IPCReceiever(ProtobufProtocol):
    protobuf = IPCMessage

    def rawDataReceived(self, data):
        self.factory.known_hosts.add(self.transport.client[0])

        try:
            pb = self.protobuf()
            pb.ParseFromString(data)

        except Exception, e:  # TODO: be more explicit about errors
            def next():
                # TODO: add more information
                pb = self.protobuf()
                pb.code = 1
                pb.error.classname = e.__class__.__name__
                self.transport.write(pb.SerializeToString())
                self.transport.loseConnection()
                log.err(
                    "failed to handle message from %s:%s" %
                    self.transport.client)

        else:
            def next():
                pb = self.protobuf()
                self.transport.write(pb.SerializeToString())
                self.transport.loseConnection()
                log.msg("handled message from %s:%s" % self.transport.client)

        reactor.callLater(0, next)




class IPCReceieverFactory(Factory):
    protocol = IPCReceiever
    known_hosts = set()

    def stopFactory(self):  # TODO: notify all connected hosts we are stopping
        if self.known_hosts:
            log.msg("notifying all known hosts of termination")


endpoint = TCP4ServerEndpoint(reactor, 9000)
endpoint.listen(IPCReceieverFactory())

application = Application("server")