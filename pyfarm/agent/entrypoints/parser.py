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
    ArgumentParser, _StoreAction, _StoreConstAction, _SubParsersAction,
    _StoreTrueAction, _StoreFalseAction, _AppendAction, _AppendConstAction)


class StoreAction(_StoreAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(StoreAction, self).__init__(*args, **kwargs)


class SubParsersAction(_SubParsersAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(SubParsersAction, self).__init__(*args, **kwargs)


class StoreConstAction(_StoreConstAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(StoreConstAction, self).__init__(*args, **kwargs)


class StoreTrueAction(_StoreTrueAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(StoreTrueAction, self).__init__(*args, **kwargs)


class StoreFalseAction(_StoreFalseAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(StoreFalseAction, self).__init__(*args, **kwargs)


class AppendAction(_AppendAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(AppendAction, self).__init__(*args, **kwargs)


class AppendConstAction(_AppendConstAction):
    def __init__(self, *args, **kwargs):
        self.config = kwargs.pop("config", None)
        super(AppendConstAction, self).__init__(*args, **kwargs)


class AgentArgumentParser(ArgumentParser):
    """
    A modified :class:`ArgumentParser` which interfaces with
    the agent's configuration.
    """
    # We create a class level mapping because every call to
    # create subparsers will instance this same class.
    config_map = {}

    def __init__(self, *args, **kwargs):
        super(AgentArgumentParser, self).__init__(*args, **kwargs)

        # Re-register actions with our actions which handle
        # the config value properly.
        self.register("action", None, StoreAction)
        self.register("action", "store", StoreAction)
        self.register("action", "store_const", StoreConstAction)
        self.register("action", "store_true", StoreTrueAction)
        self.register("action", "store_false", StoreFalseAction)
        self.register("action", "append", AppendAction)
        self.register("action", "append_const", AppendConstAction)
        self.register("action", "parsers", SubParsersAction)

    def add_argument(self, *args, **kwargs):
        """
        Custom implementation of the builtin :meth:`ArgumentParser.add_argument`
        method.  This implementation ensures that for every command line flag
        added we're able to map the resulting value back to the agent's
        global configuration.
        """
        config = kwargs.pop("config", None)
        action = super(AgentArgumentParser, self).add_argument(*args, **kwargs)

        if action.dest != "help" and config is None:
            raise AssertionError("`config` keyword required")

        return action
