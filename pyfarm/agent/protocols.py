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
Protocols
=========

Contains basic and common protocols which are used in similar ways in different
parts of the project.
"""

from twisted.protocols.basic import LineReceiver

from pyfarm.agent.ipc_pb2 import IPCMessage


class ProtobufProtocol(LineReceiver):
    """
    Basic protocol based on Twisted's :class:`.LineReceiver` protocol
    with specific modifications for protocol buffers.
    """
    protobuf = NotImplemented
    line_mode = 0

    def __init__(self):
        assert self.protobuf is not NotImplemented, "self.protobuf not set"

    def rawDataReceived(self, data):
        raise NotImplementedError("rawDataReceived() must be overridden")


class IPCReceiverProtocolBase(ProtobufProtocol):
    """
    Subclass of :class:`.ProtobufProtocol` which sets the :attr:`.protobuf`
    attribute to :class:`.IPCMessage`
    """
    protobuf = IPCMessage
