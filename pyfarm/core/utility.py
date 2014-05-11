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
Utilities
=========

General utility functions that are not specific to individual components
of PyFarm.
"""

from __future__ import division

import json
from functools import partial
from ast import literal_eval

try:
    from UserDict import UserDict
except ImportError:  # pragma: no cover
    from collections import UserDict

from pyfarm.core.config import read_env_bool
from pyfarm.core.enums import (
    NUMERIC_TYPES, STRING_TYPES, PY2, PY3,
    BOOLEAN_TRUE, BOOLEAN_FALSE, NONE, Values)


class ImmutableDict(dict):
    """
    A basic immutable dictionary that's built on top of Python's
    standard :class:`dict` class.  Once :meth:`__init__` has been
    run the contents of the instance can no longer be modified
    """
    def __init__(self, iterable=None, **kwargs):
        if self:
            raise RuntimeError("__init__ already run.")

        self.__writable = True
        try:
            super(ImmutableDict, self).__init__(iterable or [], **kwargs)
        except RuntimeError:  # pragma: no cover
            raise
        finally:
            del self.__writable

    # Force Python 2.x to use generators for items/keys/values
    if PY2:  # pragma: no cover
        items = dict.iteritems
        keys = dict.iterkeys
        values = dict.itervalues

    # Decorator to check if we're allowed to write
    # data to the class
    def write_required(method):  # pragma: no cover
        def wrapper(*args, **kwargs):
            if not hasattr(args[0], "__writable"):
                raise RuntimeError("Cannot modify a read-only dictionary.")
            return method(*args, **kwargs)
        return wrapper

    # Wrap the 'writable' methods using the decorator so
    # we can raise exceptions if someone tries to modify
    # the instance after __init__ is run.
    __setitem__ = write_required(dict.__setitem__)
    __delitem__ = write_required(dict.__delitem__)
    clear = write_required(dict.clear)
    pop = write_required(dict.pop)
    popitem = write_required(dict.popitem)
    setdefault = write_required(dict.setdefault)
    update = write_required(dict.update)

    # Once we've applied the decorator, we don't
    # need it anymore.
    del write_required


class PyFarmJSONEncoder(json.JSONEncoder):
    def encode(self, o):
        # Introspect dictionary objects for our
        # enum value type so we can dump out the string
        # value explicitly.  Otherwise, json.dumps will
        # dump out a tuple object instead.
        if isinstance(o, dict):
            o = o.copy()
            for key, value in o.items():
                if isinstance(value, Values):
                    o[key] = value.str

        return super(PyFarmJSONEncoder, self).encode(o)

dumps = partial(
    json.dumps,
    indent=4 if read_env_bool("PYFARM_PRETTY_JSON", False) else None,
    cls=PyFarmJSONEncoder)


class convert(object):
    """
    Namespace containing various static methods for converting data.

    Some staticmethods are named the same as builtin types. The name
    indicates the expected result but the staticmethod may not behave the
    same as the equivalently named Python object.  Read the documentation
    for each staticmethod to learn the differences, expected input and
    output.
    """
    @staticmethod
    def bytetomb(value):
        """
        Convert bytes to megabytes

        >>> convert.bytetomb(10485760)
        10.0
        """
        return value / 1024 / 1024

    @staticmethod
    def mbtogb(value):
        """
        Convert megabytes to gigabytes

        >>> convert.mbtogb(2048)
        2.0
        """
        return value / 1024

    @staticmethod
    def ston(value, types=NUMERIC_TYPES):
        """
        Converts a string to an integer or fails with a useful error
        message

        :param string value:
            The value to convert to an integer

        :raises ValueError:
            Raised if ``value`` could not be converted using
            :func:`.literval_eval`

        :raises TypeError:
            Raised if ``value`` was not converted to a float, integer, or long
        """
        # already a number
        if isinstance(value, types):
            return value

        # we only convert strings
        if not isinstance(value, STRING_TYPES):
            raise TypeError("`value` must be a string")

        value = literal_eval(value)

        # ensure we got a number out of literal_eval
        if not isinstance(value, types):
            raise ValueError("`value` did not convert to a number")

        return value

    @staticmethod
    def bool(value):
        """
        Converts ``value`` into a boolean object.  This function mainly exits
        so human-readable booleans such as 'yes' or 'y' can be handled in
        a single location.  Internally it does *not* use :func:`bool` and
        instead checks ``value`` against :const:`pyfarm.core.enums.BOOLEAN_TRUE`
        and :const:`pyfarm.core.enums.BOOLEAN_FALSE`.

        :param value:
            The value to attempt to convert to a boolean.  If this value is a
            string it will be run through ``.lower().strip()`` first.

        :raises ValueError:
            Raised if we can't convert ``value`` to a true boolean object
        """
        if isinstance(value, STRING_TYPES):
            value = value.lower().strip()

        if value in BOOLEAN_TRUE:
            return True
        elif value in BOOLEAN_FALSE:
            return False
        else:
            raise ValueError(
                "Cannot convert %r to either True or False" % value)

    @staticmethod
    def none(value):
        """
        Converts ``value`` into ``None``.  This function mainly exits
        so human-readable values such as 'None' or 'null' can be handled in
        a single location.  Internally this checks ``value``
        against :const:`pyfarm.core.enums.NONE`

        :param value:
            The value to attempt to convert to ``None``.  If this value is a
            string it will be run through ``.lower().strip()`` first.

        :raises ValueError:
            Raised if we can't convert ``value`` to ``None``
        """
        if isinstance(value, STRING_TYPES):
            value = value.lower().strip()

        if value in NONE:
            return None
        else:
            raise ValueError(
                "Cannot convert %r to None" % value)

    @staticmethod
    def list(value, sep=",", strip=True, filter_empty=True):
        """
        Converts ``value`` into a list object by splitting on ``sep``.

        :param str value:
            The string we should convert into a list

        :param str sep:
            The string that we should split ``value`` by.

        :param bool strip:
            If ``True``, strip extra whitespace from the results so
            ``'foo, bar'`` becomes ``['foo', 'bar']``

        :param bool filter_empty:
            If ``True``, any result that evaluates to ``False`` will be
            removed so ``'foo,,'`` would become ``['foo']``
        """
        if not isinstance(value, STRING_TYPES) \
                or not isinstance(sep, STRING_TYPES):
            raise TypeError("Expected a string for `value` and/or `sep`")

        # split the string
        value = value.split(sep)
        if strip:
            value = map(str.strip, value)

        # filter out empty values
        if filter_empty:
            value = filter(bool, value)

        # if we're in Python 3, we may be working with an iterable
        if not isinstance(value, list):
            value = list(value)

        return value
