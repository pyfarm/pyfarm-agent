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
from decimal import Decimal, ROUND_HALF_DOWN
from functools import partial
from io import StringIO
from ast import literal_eval

try:
    from UserDict import UserDict
except ImportError:  # pragma: no cover
    from collections import UserDict

try:
    _range = xrange
except NameError:  # pragma: no cover
    _range = range

from pyfarm.core.config import read_env_bool
from pyfarm.core.enums import NUMERIC_TYPES, STRING_TYPES, Values


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


def rounded(value, places=4, rounding=ROUND_HALF_DOWN):
    """
    Returns a floating point number rounded to `places`.

    :type value: float
    :param value:
        the value to round

    :type places: int
    :param places:
        the number of decimal places to round to
    """
    if isinstance(value, int) or int(value) == value:
        return value

    if not isinstance(places, int):
        raise TypeError("expected an integer for `places`")

    if places < 1:
        raise ValueError("expected at least one decimal place for `places`")

    # rounding
    dec = Decimal(str(value))
    zeros = "0" * (places - 1)
    rounded_float = dec.quantize(
        Decimal("0.%s1" % zeros), rounding=rounding)

    return float(rounded_float)


def _floatrange_generator(start, end, by, add_endpoint):
    """
    Underlying function for generating float ranges.  Values
    are passed into this function via :func:`float_range`
    """
    # we can handle either integers or floats here
    float_start = isinstance(start, NUMERIC_TYPES)
    float_end = isinstance(end, NUMERIC_TYPES)
    float_by = isinstance(by, NUMERIC_TYPES)
    last_value = None

    if float_start and end is None and by is None:
        end = start
        by = 1
        i = 0
        while i <= end:
            yield i
            last_value = i
            i = rounded(i + by)

    elif float_start and float_by and end is None:
        end = start
        i = 0
        while i <= end:
            yield i
            last_value = i
            i = rounded(i + by)

    elif float_start and float_end and by is None:
        by = 1
        i = start
        while i <= end:
            yield i
            last_value = i
            i = rounded(i + by)

    elif float_start and float_end and float_by:
        i = start
        while i <= end:
            yield i
            last_value = i
            i = rounded(i + by)

    # produce the endpoint if requested
    if add_endpoint and last_value is not None and last_value != end:
        yield end


def float_range(start, end=None, by=None, add_endpoint=False):
    """
    Creates a generator which produces a list between `start` and `end` with
    a spacing of `by`.  See below for some examples:

    .. note::
        results from this function may vary slightly from your
        expectations when not using Python 2.7 or higher

    :type start: int or float
    :param start:
        the number to start the range at

    :type end: int or float
    :param end:
        the number to finish the range at

    :type by: int or float
    :param by:
        the 'step' to use in the range

    :type add_endpoint: bool
    :param add_endpoint:
        If True then ensure that the last value generated
        by :func:`float_range` is the end value itself
    """
    if end is not None and end < start:
        raise ValueError("`end` must be greater than `start`")

    if by is not None and by <= 0:
        raise ValueError("`by` must be non-zero")

    int_start = isinstance(start, int)
    int_end = isinstance(end, int)
    int_by = isinstance(by, int)

    # integers - only start/by were provided
    if int_start and end is None and int_by:
        end = start
        start = 0
        return _range(start, end, by)

    # integers - only start was provided
    elif int_start and end is None and by is None:
        return _range(start)

    # integers - start/end/by are all integers
    elif all([int_start, int_end, int_by]):
        return _range(start, end, by)

    else:
        return _floatrange_generator(start, end, by, add_endpoint)


class convert(object):
    """Namespace containing various static methods for conversion"""

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
        converts a string to an integer or fails with a useful error
        message

        :attr string value:
            the value to convert to an integer

        :exception ValueError:
            raised if ``value`` could not be converted using
            :func:`.literval_eval`

        :exception TypeError:
            raised if ``value`` was not converted to a float, integer, or long
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


def dictformat(data, indent="    ", columns=4):
    """
    basic dictionary formatter similar to :func:`pprint.pformat` but
    with a few extra options

    :param str indent:
        the indentation which will prefix each line

    :param int columns:
        how many keys should be displayed on a single line
    """
    assert isinstance(data, (UserDict, dict))
    assert isinstance(columns, int) and columns >= 1

    # UserDict objects need to use the base data
    if isinstance(data, UserDict):
        data = data.data.copy()
    else:
        data = data.copy()

    output = StringIO()

    while data:
        last_line = False
        values = []

        # pull as many keys as requested out of the
        # dictionary
        for i in xrange(columns):
            try:
                key, value = data.popitem()
            except KeyError:
                last_line = True
                break
            else:
                values.append(": ".join([repr(key), repr(value)]))

        value = indent + ", ".join(values)
        if not last_line:
            value += ","

        print >> output, value

    return indent + "{" + output.getvalue().strip() + "}"
