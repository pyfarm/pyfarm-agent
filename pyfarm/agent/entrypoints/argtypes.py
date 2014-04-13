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
Argument Types
==============

Provides functions which are used to parser and/or convert
arguments from the argparse object.
"""


from argparse import ArgumentParser
from functools import partial, wraps
from os.path import isdir

from netaddr import AddrFormatError, IPAddress

from pyfarm.core.enums import OS, NUMERIC_TYPES
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert

INFINITE = set(["inf", "infinite", "unlimited"])
logger = getLogger("agent")


def assert_instance(func):
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


@assert_instance
def ip(value, instance=None):
    """make sure the ip address provided is valid"""
    try:
        IPAddress(value)
    except (ValueError, AddrFormatError):
        instance.parser.error("%s is not a valid ip address" % value)
    else:
        return value


@assert_instance
def port(value, instance=None):
    """convert and check to make sure the provided port is valid"""
    try:
        value = convert.ston(value)
    except ValueError:
        instance.parser.error("failed to convert --port to a number")
    else:
        low_port = 1 if instance.args.uid == 0 else 49152
        high_port = 65535

        if low_port > value or value > high_port:
            instance.parser.error(
                "valid port range is %s-%s" % (low_port, high_port))

        return value

@assert_instance
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


@assert_instance
def direxists(path, instance=None, flag=None):
    """checks to make sure the directory exists"""
    if not isdir(path):
        instance.parser.error(
            "--%s, directory does not exist: %s" % (flag, path))

    return path


@assert_instance
def number(value, types=None, instance=None, allow_inf=False, min_=1,
           flag=None):
    """convert the given value to a number"""
    if isinstance(value, NUMERIC_TYPES):
        return value

    elif value.lower() in INFINITE and allow_inf:
        return float("inf")

    elif value.lower() in INFINITE and not allow_inf:
        instance.parser.error("--%s does not allow an infinite value" % flag)

    try:
        value = convert.ston(value, types=types or NUMERIC_TYPES)
        if min_ is not None and value < min_:
            instance.parser.error(
                "--%s's value must be greater than %s" % (flag, min_))
        return value

    except SyntaxError:  # could not even parse the string as code
        instance.parser.error(
            "--%s failed to convert %s to a number" % (flag, repr(value)))

    except ValueError:  # it's a number, but not the right type
        instance.parser.error(
            "--%s, %s is not an instance of %s" % (flag, repr(value), types))


@assert_instance
def enum(value, instance=None, enum=None, flag=None):
    """ensures that ``value`` is a valid entry in ``enum``"""
    assert enum is not None
    value = value.lower()

    if value not in enum:
        instance.parser.error(
            "invalid enum value %s for --%s, valid values are %s" % (
                value, flag, list(enum)))

    return value

integer = partial(number, types=int)
