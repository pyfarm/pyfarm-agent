
from pyfarm.agent.utility import unpack_result_tuple, pack_result_tuple
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 9000))

from pyfarm.agent.ipc_pb2 import IPCMessage
message = IPCMessage()
message.code = 1
s.send(message.SerializeToString())
response = IPCMessage()
response.ParseFromString((s.recv(2048)))

print response.code
s.close()