# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
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
Converters
~~~~~~~~~~

Basic functions for converting command line arguments.
"""


from argparse import ArgumentParser
from functools import wraps
from os.path import isdir

from netaddr import AddrFormatError, IPAddress

from pyfarm.core.enums import OS
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert

logger = getLogger("agent")


def assert_parent(func):
    """
    ensures that the instance argument passed along to the validation
    function contains data we expect
    """
    @wraps(func)
    def run(*args, **kwargs):
        instance = kwargs.get("instance")
        assert instance is not None
        assert hasattr(instance, "args") and hasattr(instance, "parser")
        assert isinstance(instance.parser, ArgumentParser)
        return func(*args, **kwargs)
    return run


@assert_parent
def ip(value, instance=None):
    """make sure the ip address provided is valid"""
    try:
        IPAddress(value)
    except (ValueError, AddrFormatError):
        instance.parser.error("%s is not a valid ip address" % value)
    else:
        return value


@assert_parent
def port(value, instance=None):
    """convert and check to make sure the provided port is valid"""
    try:
        value = convert.ston(value)
    except ValueError:
        instance.parser.error("failed to convert --port to a number")
    else:
        low_port = 1 if instance.args.uid == 0 else 49152
        high_port = 65535

        if low_port <= value <= high_port:
            instance.parser.error(
                "valid port range is %s-%s" % (low_port, high_port))

        return value

@assert_parent
def uidgid(value=None, flag=None,
           get_id=None, check_id=None, set_id=None, instance=None):
        """
        Retrieves and validates the user or group id for a command line flag
        """
        # make sure the partial function is setting
        # the input values
        assert flag is not None
        assert get_id is not None
        assert check_id is not None
        assert set_id is not None

        if set_id is NotImplemented:
            logger.info("--%s is ignored on %s" % (flag, OS.title()))
            return

        elif not value:
            return

        # convert the incoming argument to a number or fail
        try:
            value = convert.ston(value)
        except ValueError:
            instance.parser.error("failed to convert --%s to a number" % flag)

        # make sure the id actually exists
        try:
            check_id(value)
        except KeyError:
            instance.parser.error(
                "%s %s does not seem to exist" % (flag, value))

        # get the uid/gid of the current process
        # so we can reset it after checking it
        original_id = get_id()

        # Try to set the uid/gid to the value requested, fail
        # otherwise.  We're doing this here because we'll be
        # able to stop the rest of the code from running faster
        # and provide a more useful error message.
        try:
            set_id(value)
        except OSError:
            instance.parser.error(
                "Failed to set %s to %s, please make sure you have "
                "the necessary permissions to perform this action.  "
                "Typically you must be running as root." % (flag, value))

        # set the uid/gid back to the original value since
        # the id change should occur inside the form or right
        # before the agent is started
        try:
            set_id(original_id)
        except OSError:
            instance.parser.error(
                "failed to set %s back to the original value" % flag)

        return value


@assert_parent
def chroot(path, instance=None):
    """check to make sure the chroot directory exists"""
    if not isdir(path):
        instance.parser.error(
            "cannot chroot into %s, it does not exist" % path)

    return path