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

from argparse import (
    SUPPRESS, ArgumentParser, _StoreAction, _StoreConstAction,
    _SubParsersAction, _StoreTrueAction, _StoreFalseAction, _AppendAction,
    _AppendConstAction)
from functools import partial
from os.path import isdir

from pyfarm.agent.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.entrypoints.argtypes import integer, direxists

logger = getLogger("agent.parser")


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
        isdir: direxists}

    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        type_ = kwargs.get("type")
        type_kwargs = kwargs.pop("type_kwargs", {})

        if self.config not in (False, None):
            if self.config not in config and "default" not in kwargs:
                raise AssertionError(
                    "Config value `%s` does not exist and no default was "
                    "provided.  Please either setup a default in the config "
                    "or provide a default to the argument parser" % self.config)

            kwargs.update(default=config[self.config])

        if type_ is not None:
            assert AgentArgumentParser.parser is not None
            type_ = self.TYPE_MAPPING.get(type_, type_)

            partial_kwargs = {"parser": AgentArgumentParser.parser}
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

        # If we're not suppressing this configuration value then there's
        # some keywords we expect to be passed into add_argument()
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
    parser = None

    def __new__(cls, *args, **kwargs):
        instance = object.__new__(cls)

        # Only the first parser we create, which is also
        # the one that creates other parsers, will setup
        # the instance attribute
        if cls.parser is None:
            cls.parser = instance

        return instance

    def __init__(self, *args, **kwargs):
        super(AgentArgumentParser, self).__init__(*args, **kwargs)

        # Override the relevant actions with out own so
        # we have more control of what they're doing
        self.register("action", None, StoreAction)
        self.register("action", "store", StoreAction)
        self.register("action", "store_const", StoreConstAction)
        self.register("action", "store_true", StoreTrueAction)
        self.register("action", "store_false", StoreFalseAction)
        self.register("action", "append", AppendAction)
        self.register("action", "append_const", AppendConstAction)
        self.register("action", "parsers", SubParsersAction)
