# No shebang line, this module is meant to be imported
#
# Copyright 2014 Oliver Palmer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Parser
======

Module which forms the basis of a custom :mod:`argparse` based
command line parser which handles setting configuration values
automatically.
"""

import os
from argparse import (
    SUPPRESS, ArgumentParser, _StoreAction, _StoreConstAction,
    _SubParsersAction, _StoreTrueAction, _StoreFalseAction, _AppendAction,
    _AppendConstAction)
from errno import EEXIST
from functools import partial, wraps
from os.path import isdir, isfile, abspath
from uuid import UUID

from netaddr import AddrFormatError, IPAddress

from pyfarm.core.enums import OS, NUMERIC_TYPES
from pyfarm.core.utility import convert

from pyfarm.agent.logger import getLogger
from pyfarm.agent.config import config

INFINITE = set(["inf", "infinite", "unlimited"])

logger = getLogger("agent.parser")


def assert_parser(func):
    """
    ensures that the instance argument passed along to the validation
    function contains data we expect
    """
    @wraps(func)
    def run(*args, **kwargs):
        parser = kwargs.get("parser")
        assert parser is not None and isinstance(parser, ArgumentParser)
        return func(*args, **kwargs)
    return run


@assert_parser
def ip(value, parser=None, flag=None):
    """make sure the ip address provided is valid"""
    try:
        IPAddress(value)
    except (ValueError, AddrFormatError):
        parser.error("%s is not a valid ip address for %s" % (value, flag))
    else:
        return value


@assert_parser
def port(value, parser=None, get_uid=None, flag=None):
    """convert and check to make sure the provided port is valid"""
    assert callable(get_uid)
    try:
        value = convert.ston(value)
    except (ValueError, SyntaxError):
        parser.error("%s requires a number" % flag)
    else:
        try:
            low_port = 1 if get_uid() == 0 else 49152
        except AttributeError:
            low_port = 49152

        high_port = 65535

        if low_port > value or value > high_port:
            parser.error(
                "valid port range is %s to %s" % (low_port, high_port))

        return value

@assert_parser
def uuid_type(value, parser=None, flag=None):
    """validates that a string is a valid UUID type"""
    try:
        return UUID(value)
    except ValueError:
        parser.error("%s cannot be convert to a UUID for %s" % (value, flag))

# Function is not currently tested because uid/gid mapping is system specific,
# may require access to external network resources, and internally is
# covered for the most part by other tests.
# TODO: find a reliable way to test uidgid()
@assert_parser
def uidgid(value=None, flag=None,
           get_id=None, check_id=None, set_id=None,
           parser=None):  # pragma: no cover
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
            logger.info("%s is ignored on %s" % (flag, OS.title()))
            return

        elif not value:
            return

        # convert the incoming argument to a number or fail
        try:
            value = convert.ston(value)
        except ValueError:
            parser.error("failed to convert %s to a number" % flag)

        # make sure the id actually exists
        try:
            check_id(value)
        except KeyError:
            parser.error(
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
            parser.error(
                "Failed to set %s to %s, please make sure you have "
                "the necessary permissions to perform this action.  "
                "Typically you must be running as root." % (flag, value))

        # set the uid/gid back to the original value since
        # the id change should occur inside the form or right
        # before the agent is started
        try:
            set_id(original_id)
        except OSError:
            parser.error(
                "failed to set %s back to the original value" % flag)

        return value


@assert_parser
def direxists(path, parser=None, flag=None, create=False):
    """checks to make sure the directory exists"""
    if create:
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != EEXIST:
                parser.error(
                    "Failed to create directory %s: %s" % (path, e))
        else:
            logger.debug("Created %s", path)

    elif not isdir(path):
        parser.error(
            "%s, path does not exist or is not "
            "a directory: %s" % (flag, path))

    return abspath(path)


@assert_parser
def fileexists(path, parser=None, flag=None):
    """checks to make sure the provided file exists"""
    if not isfile(path):
        parser.error(
            "Path %r, which was provided to %s, does not exist" % (path, flag))

    return abspath(path)

@assert_parser
def number(value, types=None, parser=None, allow_inf=False, min_=1,
           flag=None):
    """convert the given value to a number"""
    if value == "auto":
        return value

    # Internally used
    if isinstance(value, NUMERIC_TYPES):  # pragma: no cover
        return value

    elif value.lower() in INFINITE and allow_inf:
        return float("inf")

    elif value.lower() in INFINITE and not allow_inf:
        parser.error("%s does not allow an infinite value" % flag)

    try:
        value = convert.ston(value, types=types or NUMERIC_TYPES)
        if min_ is not None and value < min_:
            parser.error(
                "%s's value must be greater than %s" % (flag, min_))
        return value

    except SyntaxError:  # could not even parse the string as code
        parser.error(
            "%s failed to convert %s to a number" % (flag, repr(value)))

    except ValueError:  # it's a number, but not the right type
        parser.error(
            "%s, %s is not an instance of %s" % (flag, repr(value), types))


@assert_parser
def enum(value, parser=None, enum=None, flag=None):
    """ensures that ``value`` is a valid entry in ``enum``"""
    assert enum is not None
    value = value.lower()

    if value not in enum:
        parser.error(
            "invalid enum value %s for %s, valid values are %s" % (
                value, flag, list(enum)))

    return value

integer = partial(number, types=int)


class ActionMixin(object):
    """
    A mixin which overrides the ``__init__`` and ``__call__``
    methods on an action so we can:

        * Setup attributes to manipulate the config object when the
          arguments are parsed
        * Ensure we all required arguments are present
        * Convert the ``type`` keyword into an internal representation
          so we don't require as much work when we add arguments to
          the parser
    """
    # Maps standard Python functions to some more advanced internal
    # functions.  Our internal versions have several additional
    # options and better error handling.
    TYPE_MAPPING = {
        int: integer,
        isdir: direxists,
        isfile: fileexists}

    def __init__(self, *args, **kwargs):
        self.parser = kwargs.pop("parser")
        self.config = kwargs.pop("config", False)
        type_ = kwargs.get("type")
        type_kwargs = kwargs.pop("type_kwargs", {})

        if self.config is not False:
            if self.config not in config and "default" not in kwargs:
                raise AssertionError(
                    "Config value `%s` does not exist and no default was "
                    "provided.  Please either setup a default in the config "
                    "or provide a default to the argument parser" % self.config)

            # Update the config with the default if one
            # was provided
            if "default" in kwargs:
                default = kwargs["default"]
                if callable(kwargs["default"]):
                    default = default()
                config[self.config] = default

        if type_ is not None:
            assert self.parser is not None
            type_ = self.TYPE_MAPPING.get(type_, type_)

            partial_kwargs = {"parser": self.parser}
            partial_kwargs.update(type_kwargs)

            if "flag" not in partial_kwargs:
                # Convert one of the option string, preferably the
                # one starting with --, and use that as the flag
                # in the type function
                option_strings = kwargs.get("option_strings", [])
                assert len(option_strings) >= 1
                option = option_strings[0]

                for option_string in option_strings:
                    if option.startswith("--"):
                        option = option_string
                        break

                partial_kwargs.update(flag=option)

            kwargs.update(type=partial(type_, **partial_kwargs))

        super(ActionMixin, self).__init__(*args, **kwargs)

        # Before we run the parser, assert that certain flags are
        # set on each action object.
        if self.dest != SUPPRESS:
            if not self.help:
                raise AssertionError(
                    "`help` keyword missing for %s" % self.option_strings)

            if self.config is None:
                raise AssertionError(
                    "The config keyword is missing for %s.  Please provide one "
                    "or set config to False." % self.option_strings)

    def __call__(self, parser, namespace, values, option_string=None):
        super(ActionMixin, self).__call__(
            parser, namespace, values, option_string=option_string)

        # Set the config value based upon the value which was set
        # on the resulting action.  This will be done when parser_args()
        # is called.
        if self.dest != SUPPRESS and self.config not in (False, None):
            config[self.config] = getattr(namespace, self.dest)


#
# Create some classes which mix the class above and the
# original action so we can set attributes and work with
# the configuration
mix_action = lambda class_: type(class_.__name__, (ActionMixin, class_), {})
StoreAction = mix_action(_StoreAction)
SubParsersAction = mix_action(_SubParsersAction)
StoreConstAction = mix_action(_StoreConstAction)
StoreTrueAction = mix_action(_StoreTrueAction)
StoreFalseAction = mix_action(_StoreFalseAction)
AppendAction = mix_action(_AppendAction)
AppendConstAction = mix_action(_AppendConstAction)


class AgentArgumentParser(ArgumentParser):
    """
    A modified :class:`ArgumentParser` which interfaces with
    the agent's configuration.
    """
    def __init__(self, *args, **kwargs):
        super(AgentArgumentParser, self).__init__(*args, **kwargs)

        # Override the relevant actions with out own so
        # we have more control of what they're doing
        self.register("action", None,
                      partial(StoreAction, parser=self))
        self.register("action", "store",
                      partial(StoreAction, parser=self))
        self.register("action", "store_const",
                      partial(StoreConstAction, parser=self))
        self.register("action", "store_true",
                      partial(StoreTrueAction, parser=self))
        self.register("action", "store_false",
                      partial(StoreFalseAction, parser=self))
        self.register("action", "append",
                      partial(AppendAction, parser=self))
        self.register("action", "append_const",
                      partial(AppendConstAction, parser=self))
        self.register("action", "parsers",
                      partial(SubParsersAction, parser=self))
