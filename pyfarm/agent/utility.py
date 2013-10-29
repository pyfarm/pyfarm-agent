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
Utility
=======

General functions, utilities, and objects to operate on
basic data.
"""

import struct
import traceback
from functools import partial

from twisted.python import log
from twisted.internet.task import LoopingCall

from pyfarm.agent.ipc_pb2 import IPCMessage, DynamicType


def pack_result_tuple(code, message):
    """
    Packs a struct containing a return code and message using
    :func:`struct.pack`.

    >>> pack_result_tuple(21, "It's not your lucky day, something went wrong.")
    "\x00\x15\x00\x00\x00.It's not your lucky day, something went wrong."

    This can be used to send typed responses over a TCP connection which
    can then be reliably unpacked on the other side into a tuple using
    :func:`.unpack_result_tuple`
    """
    message_length = len(message)

    # be explict about our requirements
    assert isinstance(message, str), "expected string for `message`"
    assert isinstance(code, int), "expected integer for `code`"
    assert 0 <= code <= 65535, "assert(0 <= code <= 65535)"
    assert 0 <= message_length <= 4294967295,\
        "assert(0 <= len(message) <= 4294967295)"

    return struct.pack(
        ">HI%ss" % message_length, code, message_length, message)


def unpack_result_tuple(packed):
    """
    Used to unpack data sent by :func:`.pack_result_tuple` into a tuple
    containing the original error code and message.

    >>> unpack_result_tuple(
    ...   "\x00\x15\x00\x00\x00.It's not your lucky day, something went wrong.")
    (21, "It's not your lucky day, something went wrong.")
    """
    code, message_length = struct.unpack(">HI", packed[:6])
    return code, struct.unpack(">%ss" % message_length, packed[6:])[0]


def protobuf_from_error(error):
    """
    produce a :class:`.IPCMessage` protobuf which contains an exception
    """
    assert isinstance(error, Exception), "expected an exception"
    protobuf = IPCMessage()

    # add data from exception
    protobuf.error.classname = error.__class__.__name__
    protobuf.error.message = error.message
    protobuf.error.traceback = traceback.format_exc()

    # add input arguments from exception to error args
    for arg_value in getattr(error, "args", []):
        arg = protobuf.error.args.add()
        if isinstance(arg_value, basestring):
            arg.type = DynamicType.PythonType.Value("STRING")
            arg.string = arg_value
        elif isinstance(arg_value, int):
            arg.type = DynamicType.PythonType.Value("INTEGER")
            arg.int = arg_value
        elif isinstance(arg_value, bool):
            arg.type = DynamicType.PythonType.Value("BOOLEAN")
            arg.bool = arg_value
        else:
            arg.type = DynamicType.PythonType.Value("STRING")
            arg.string = str(arg_value)

    return protobuf


class ScheduledTaskManager(object):
    """
    Manages and keeps track of several scheduled tasks.
    """
    def __init__(self):
        self.tasks = {}
        self.log = partial(log.msg, system=self.__class__.__name__)

    def register(self, function, interval, start=False,
                 func_args=None, func_kwargs=None):
        """
        Register a callable function to run at a given interval.  This function
        will do nothing if ``function`` has already been registered.

        :param function:
            a callable function that should be run on an interval

        :type interval: int or float
        :param interval:
            the interval in which ``function`` should be urn

        :param bool start:
            if True, start the interval timer after it has been added

        :param tuple func_args:
            the positional arguments to pass into ``function``

        :param dict func_kwargs:
            the keyword arguments to pass into ``function``

        :exception AssertionError:
            raised if ``function`` is not callable
        """
        assert callable(function)
        args, kwargs = func_args or (), func_kwargs or {}

        if function not in self.tasks:
            looping_call = LoopingCall(function, *args, **kwargs)
            self.tasks[function] = (looping_call, interval)
            self.log("registering %s")
            if start:
                looping_call.start(interval)

    def start(self, now=True):
        """
        start all :class:`.LoopingCall` instances stored from
        :meth:`.register`
        """
        self.log("starting tasks")
        for function, (looping_call, interval) in self.tasks.iteritems():
            if not looping_call.running:
                looping_call.start(interval, now=now)

    def stop(self):
        """
        stop all :class:`.LoopingCall` instances stored from
        :meth:`.register`
        """
        self.log("stopping tasks")
        for function, (looping_call, interval) in self.tasks.iteritems():
            if not looping_call.running:
                looping_call.stop(interval)