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
