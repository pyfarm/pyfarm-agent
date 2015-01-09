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
Utilities
---------

Top level utilities for the agent to use internally.  Many of these
are copied over from the master (which we can't import here).
"""

import csv
import codecs
import cStringIO
import os
from decimal import Decimal
from datetime import datetime, timedelta
from errno import EEXIST, ENOENT
from json import dumps as _dumps
from os.path import join, dirname
from UserDict import UserDict
from uuid import UUID, uuid1, uuid4

try:
    from urlparse import urlsplit
    from urllib import quote
except ImportError:  # pragma: no cover
    from http.client import urlsplit
    from urllib.parse import quote

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

from voluptuous import Schema, Any, Required, Optional, Invalid

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger

logger = getLogger("agent.util")
STRINGS = Any(*STRING_TYPES)
try:
    WHOLE_NUMBERS = Any(*(int, long))
    NUMBERS = Any(*(int, long, float, Decimal))
except NameError:  # pragma: no cover
    WHOLE_NUMBERS = int
    NUMBERS = Any(*(int, float, Decimal))


def validate_environment(values):
    """
    Ensures that ``values`` is a dictionary and that it only
    contains string keys and values.
    """
    if not isinstance(values, dict):
        raise Invalid("Expected a dictionary")

    for key, value in values.items():
        if not isinstance(key, STRING_TYPES):
            raise Invalid("Key %r must be a string" % key)

        if not isinstance(value, STRING_TYPES):
            raise Invalid("Value %r for key %r must be a string" % (key, value))


def validate_uuid(value):
    """
    Ensures that ``value`` can be converted to or is a UUID object.
    """
    if isinstance(value, UUID):
        return value
    elif isinstance(value, STRING_TYPES):
        try:
            return UUID(hex=value)
        except ValueError:
            raise Invalid("%s cannot be converted to a UUID" % value)
    else:
        raise Invalid("Expected string or UUID instance for `value`")


# Shared schema declarations
JOBTYPE_SCHEMA = Schema({
    Required("name"): STRINGS,
    Required("version"): WHOLE_NUMBERS})
TASK_SCHEMA = Schema({
    Required("id"): WHOLE_NUMBERS,
    Required("frame"): NUMBERS,
    Required("attempt", default=0): WHOLE_NUMBERS})
TASKS_SCHEMA = lambda values: map(TASK_SCHEMA, values)
JOB_SCHEMA = Schema({
    Required("id"): WHOLE_NUMBERS,
    Required("by"): NUMBERS,
    Optional("batch"): WHOLE_NUMBERS,
    Optional("user"): STRINGS,
    Optional("ram"): WHOLE_NUMBERS,
    Optional("ram_warning"): Any(WHOLE_NUMBERS, type(None)),
    Optional("ram_max"): Any(WHOLE_NUMBERS, type(None)),
    Optional("cpus"): WHOLE_NUMBERS,
    Optional("data"): dict,
    Optional("environ"): validate_environment,
    Optional("title"): STRINGS})


def default_json_encoder(obj, return_obj=False):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, UUID):
        return str(obj)
    elif return_obj:
        return obj


def json_safe(source):
    """
    Recursively converts ``source`` into something that should be safe for
    :func:`json.dumps` to handle.  This is used in conjunction with
    :func:`default_json_encoder` to also convert keys to something the json
    encoder can understand.
    """
    if not isinstance(source, dict):
        return source

    result = {}

    try:
        items = source.iteritems
    except AttributeError:  # pragma: no cover
        items = source.items

    for k, v in items():
        result[default_json_encoder(k, return_obj=True)] = \
            default_json_encoder(json_safe(v), return_obj=True)

    return result


def quote_url(source_url):
    """
    This function serves as a wrapper around :func:`.urlsplit`
    and :func:`.quote` and a url that has the path quoted.
    """
    url = urlsplit(source_url)

    # If a url is just "/" then we don't want to add ://
    # since a scheme was not included.
    if url.scheme:
        result = "{scheme}://".format(scheme=url.scheme)
    else:
        result = ""

    result += url.netloc + quote(url.path, safe="/?&=")

    if url.query:
        result += "?" + url.query

    if url.fragment:
        result += "#" + url.fragment

    return result


def dumps(*args, **kwargs):
    """
    Agent's implementation of :func:`json.dumps` or
    :func:`pyfarm.master.utility.jsonify`
    """
    indent = None
    if config["agent_pretty_json"]:
        indent = 2

    if len(args) == 1 and not isinstance(args[0], (dict, UserDict)):
        obj = args[0]
    else:
        obj = dict(*args, **kwargs)

    return _dumps(obj, default=default_json_encoder, indent=indent)


def request_from_master(request):
    """Returns True if the request appears to be coming from the master"""
    return request.getHeader("User-Agent") == config["master_user_agent"]


# Unicode CSV reader/writers from the standard library docs:
#   https://docs.python.org/2/library/csv.html

class UTF8Recoder(object):
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeCSVReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class UnicodeCSVWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


def total_seconds(td):
    """
    Returns the total number of seconds in the time delta
    object.  This function is provided for backwards
    comparability with Python 2.6.
    """
    assert isinstance(td, timedelta)
    try:
        return td.total_seconds()
    except AttributeError:  # pragma: no cover
        return (
           td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6


class AgentUUID(object):
    """
    This class wraps all the functionality required to load, cache and retrieve
    an Agent's UUID.
    """
    log = getLogger("agent.uuid")

    @classmethod
    def load(cls, path):
        """
        A classmethod to load a UUID object from a path.  If the provided
        ``path`` does not exist or does not contain data which can be converted
        into a UUID object ``None`` will be returned.
        """
        assert isinstance(path, STRING_TYPES), path
        cls.log.debug("Attempting to load agent UUID from %r", path)

        try:
            with open(path, "r") as loaded_file:
                data = loaded_file.read().strip()

            return UUID(data)

        except (OSError, IOError) as e:
            if e.errno == ENOENT:
                cls.log.warning("UUID file %s does not exist.", path)
                return None

            cls.log.error("Failed to load uuid from %s: %s", path, e)
            raise

        except ValueError:  # pragma: no cover
            cls.log.error("%r does not contain valid data for a UUID", path)
            raise

    @classmethod
    def save(cls, agent_uuid, path):
        """
        Saves ``agent_uuid`` to ``path``.  This classmethod will
        also create the necessary parent directories and handle
        conversion from the input type :class:`uuid.UUID`.
        """
        assert isinstance(agent_uuid, UUID)
        assert isinstance(path, STRING_TYPES)
        cls.log.debug("Saving %r to %r", agent_uuid, path)

        # Create directory if it does not exist
        parent_dir = dirname(path)
        if parent_dir.strip():
            try:
                os.makedirs(parent_dir)

            except (OSError, IOError) as e:  # pragma: no cover
                if e.errno != EEXIST:
                    cls.log.warning("Failed to create %s, %s", parent_dir, e)
                    raise

        # Write uuid to disk
        try:
            with open(path, "w") as output:
                output.write(str(agent_uuid))

            cls.log.debug("Cached %s to %r", agent_uuid, path)

        except (OSError, IOError) as e:  # pragma: no cover
            cls.log.error("Failed to write %s, %s", path, e)
            raise

    @classmethod
    def generate(cls):
        """
        Generates a UUID object.  This simply wraps :func:`uuid.uuid4` and
        logs a warning.
        """
        agent_uuid = uuid4()
        cls.log.warning("Generated agent UUID: %s", agent_uuid)
        return agent_uuid
