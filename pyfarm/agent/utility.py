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
from decimal import Decimal
from datetime import datetime
from json import dumps as _dumps
from UserDict import UserDict
from uuid import uuid1

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

from pyfarm.core.config import read_env
from voluptuous import Schema, Any, Required

from pyfarm.core.enums import STRING_TYPES
from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger

MASTER_USERAGENT = read_env("PYFARM_MASTER_USERAGENT", "PyFarm/1.0 (master)")
logger = getLogger("agent.util")
STRINGS = Any(*STRING_TYPES)
try:
    WHOLE_NUMBERS = Any(*(int, long))
    NUMBERS = Any(*(int, long, float, Decimal))
except NameError:  # pragma: no cover
    WHOLE_NUMBERS = int
    NUMBERS = Any(*(int, float, Decimal))

# Shared schema declarations
JOBTYPE_SCHEMA = Schema({
    Required("name"): STRINGS,
    Required("version"): WHOLE_NUMBERS})
TASK_SCHEMA = Schema({
    Required("id"): WHOLE_NUMBERS,
    Required("frame"): NUMBERS})
TASKS_SCHEMA = lambda values: map(TASK_SCHEMA, values)


def uuid():
    """Wrapper around :func:`uuid1` which incorporates our system id"""
    return uuid1(node=config["systemid"])


def default_json_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()


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
    pretty = config.get("pretty-json", False)
    if pretty:
        indent = 2

    if len(args) == 1 and not isinstance(args[0], (dict, UserDict)):
        obj = args[0]
    else:
        obj = dict(*args, **kwargs)

    return _dumps(obj, default=default_json_encoder, indent=indent)


def request_from_master(request):
    """Returns True if the request appears to be coming from the master"""
    return request.getHeader("User-Agent") == MASTER_USERAGENT


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



