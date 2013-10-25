
from pyfarm.agent.utility import unpack_result_tuple
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 9000))
s.send("foo?")
print unpack_result_tuple(s.recv(1024))
s.close()