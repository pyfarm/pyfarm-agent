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
Configuration Object
====================

Basic module used for changing and storing configuration values used by all
modules at execution time.
"""

import os

try:
    from ast import literal_eval
except ImportError:  # pragma: no cover
    from pyfarm.core.backports import literal_eval

from pyfarm.core.logger import getLogger

logger = getLogger("core.config")

NOTSET = object()


class Config(object):
    """
    Dictionary like object used to centrally store configuration
    information.  Generally this will be used at the module level and
    populated on startup.
    """
    def __init__(self, data=None):
        assert isinstance(data, dict) or data is None, "bad type for `data`"
        self.__data = {} if data is None else data.copy()

    def __iter__(self):
        return self.__data.__iter__()

    def __repr__(self):  # pragma: no cover
        return self.__data.__repr__()

    def __contains__(self, item):
        return self.__data.__contains__(item)

    def items(self):
        """same as :meth:`dict.iteritems` in Python < 3.x"""
        return self.__data.iteritems()

    def get(self, key, default=NOTSET):
        """
        similar to :meth:`dict.get` except that if a value does not exist
        it will raise an exception unless a default is provided
        """
        if default is NOTSET and key not in self:
            raise KeyError("%s not found" % key)
        return self.__data.get(key, default)

    def set(self, key, value):
        """sets `key` to `value`"""
        self.__data.__setitem__(key, value)

    def setdefault(self, key, default=None):
        return self.__data.setdefault(key, default)

    def update(self, data):
        """
        similar to :meth:`dict.update` except only a dictionary as input
        is allowed
        """
        assert isinstance(data, dict), "`data` must be a dictionary"
        return self.__data.update(data)


def read_env(envvar, default=NOTSET, warn_if_unset=False, eval_literal=False,
             raise_eval_exception=True, log_result=True, desc=None):
    """
    Lookup and evaluate an environment variable.

    :param string envvar:
        The environment variable to lookup in :class:`os.environ`

    :keyword object default:
        Alternate value to return if ``envvar`` is not present.  If this
        is instead set to ``NOTSET`` then an exception will be raised if
        ``envvar`` is not found.

    :keyword bool warn_if_unset:
        If True, log a warning if the value being returned is the same
        as ``default``

    :keyword eval_literal:
        if True, run :func:`.literal_eval` on the value retrieved
        from the environment

    :keyword bool raise_eval_exception:
        If True and we failed to parse ``envvar`` with :func:`.literal_eval`
        then raise a :class:`EnvironmentKeyError`

    :keyword bool log_result:
        If True, log the query and result to INFO.  If False, only log the
        query itself to DEBUG.  This keyword mainly exists so environment
        variables such as :envvar:`PYFARM_SECRET` or
        :envvar:`PYFARM_DATABASE_URI` stay out of log files.

    :keyword string desc:
        Describes the purpose of the value being returned.  This may also
        be read in at the time the documentation is built.
    """
    if not log_result:  # pragma: no cover
        logger.debug("read_env(%s)" % repr(envvar))

    if envvar not in os.environ:
        # default not provided, raise an exception
        if default is NOTSET:
            raise EnvironmentError("$%s is not in the environment" % envvar)

        if warn_if_unset:  # pragma: no cover
            logger.warning("$%s is using a default value" % envvar)

        if log_result:  # pragma: no cover
            logger.info("read_env(%s): %s" % (repr(envvar), repr(default)))

        return default
    else:
        value = os.environ[envvar]

        if not eval_literal:
            logger.debug("skipped literal_eval($%s)" % repr(envvar))
            return value

        try:
            return literal_eval(value)

        except (ValueError, SyntaxError), e:
            if raise_eval_exception:
                raise

            args = (envvar, e)
            logger.error(
                "$%s contains a value which could not be parsed: %s" % args)
            logger.warning("returning default value for $%s" % envvar)
            return default

cfg = Config()