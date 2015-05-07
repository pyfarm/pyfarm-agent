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

import atexit
import csv
import codecs
import os
import shutil
from decimal import Decimal
from datetime import datetime, timedelta
from errno import EEXIST, ENOENT, errorcode
from json import dumps as _dumps
from os.path import dirname
from UserDict import UserDict
from uuid import UUID, uuid4

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

try:
    WindowsError
except NameError:  # pragma: no cover
    WindowsError = OSError

from voluptuous import Schema, Any, Required, Optional, Invalid
from twisted.internet import reactor
from twisted.internet.defer import AlreadyCalledError, DeferredLock, Deferred
from twisted.internet.error import AlreadyCalled

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
NOTIFIED_USERS_SCHEMA = Schema({
    Required("username"): STRINGS,
    Required("on_success", default=True): bool,
    Required("on_failure", default=True): bool,
    Required("on_deletion", default=False): bool})
JOB_SCHEMA = Schema({
    Required("id"): WHOLE_NUMBERS,
    Required("by"): NUMBERS,
    Optional("agent_id"): WHOLE_NUMBERS,
    Optional("batch"): WHOLE_NUMBERS,
    Optional("user"): STRINGS,
    Optional("ram"): WHOLE_NUMBERS,
    Optional("ram_warning"): Any(WHOLE_NUMBERS, type(None)),
    Optional("ram_max"): Any(WHOLE_NUMBERS, type(None)),
    Optional("cpus"): WHOLE_NUMBERS,
    Optional("data"): dict,
    Optional("environ"): validate_environment,
    Optional("title"): STRINGS,
    Optional("notified_users"): [NOTIFIED_USERS_SCHEMA],
    Optional("priority"): WHOLE_NUMBERS,
    Optional("job_group_id"): WHOLE_NUMBERS,
    Optional("job_group"): STRINGS,
    Optional("notes"): STRINGS,
    Optional("tags"): [STRINGS]})


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
    def __init__(self, f, dialect=csv.excel, **kwds):
        self.writer = csv.writer(f, dialect=dialect, **kwds)

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") if isinstance(s, unicode) else s
                              for s in row])

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

        except (WindowsError, OSError, IOError) as e:
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

        except (WindowsError, OSError, IOError) as e:  # pragma: no cover
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


def remove_file(
        path, retry_on_exit=False, raise_=True, ignored_errnos=(ENOENT, )):
    """
    Simple function to remove the provided file or retry on exit
    if requested.  This function standardizes the log output, ensures
    it's only called once per path on exit and handles platform
    specific exceptions (ie. ``WindowsError``).

    :param bool retry_on_exit:
        If True, retry removal of the file when Python exists.

    :param bool raise_:
        If True, raise an exceptions produced.  This will always be
        False if :func:`remove_file` is being executed by :module:`atexit`

    :param tuple ignored_errnos:
        A tuple of ignored error numbers.  By default this function
        only ignores ENOENT.
    """
    try:
        os.remove(path)
    except (WindowsError, OSError, IOError) as error:
        if error.errno in ignored_errnos:
            return

        if retry_on_exit:
            logger.warning(
                "Failed to remove %s (%s), will retry on exit",
                path, errorcode[error.errno])

            # Make sure we're not added multiple times
            keywords = dict(retry_on_exit=False, raise_=False)
            signature = (remove_file, (path, ), keywords)
            if signature not in atexit._exithandlers:
                atexit.register(remove_file, path, **keywords)
        else:
            logger.error(
                "Failed to remove %s (%s)",
                path, errorcode[error.errno])

        if raise_:
            raise
    else:
        logger.debug("Removed %s", path)


def remove_directory(
        path, retry_on_exit=False, raise_=True, ignored_errnos=(ENOENT, )):
    """
    Simple function to **recursively** remove the provided directory or retry on
    exit if requested.  This function standardizes the log output, ensures
    it's only called once per path on exit and handles platform
    specific exceptions (ie. ``WindowsError``).

    :param bool retry_on_exit:
        If True, retry removal of the file when Python exists.

    :param bool raise_:
        If True, raise an exceptions produced.  This will always be
        False if :func:`remove_directory` is being executed by :module:`atexit`

    :param tuple ignored_errnos:
        A tuple of ignored error numbers.  By default this function
        only ignores ENOENT.
    """
    try:
        shutil.rmtree(path)
    except (WindowsError, OSError, IOError) as error:
        if error.errno in ignored_errnos:
            return

        if retry_on_exit:
            logger.warning(
                "Failed to remove %s (%s), will retry on exit",
                path, errorcode[error.errno])

            # Make sure we're not added multiple times
            keywords = dict(retry_on_exit=False, raise_=False)
            signature = (remove_directory, (path, ), keywords)
            if signature not in atexit._exithandlers:
                atexit.register(remove_directory, path, **keywords)
        else:
            logger.error(
                "Failed to remove %s (%s)",
                path, errorcode[error.errno])

        if raise_:
            raise
    else:
        logger.debug("Removed %s", path)


class LockTimeoutError(Exception):
    """Raised if we timeout while attempting to acquire a deferred lock"""


class TimedDeferredLock(DeferredLock):
    """
    A subclass of :class:`DeferredLock` which has a timeout
    for the :meth:`acquire` call.
    """
    def _timeout(self, deferred):
        """
        Raise a :class:`LockTimeoutError` in the given :class:`Deferred`
        instance. This ensures that when a timeout is triggered it's
        propagated all the way up the calling stack.
        """
        try:
            raise LockTimeoutError
        except LockTimeoutError as timeout:
            deferred.errback(fail=timeout)

    def _cancel_timeout(self, _, scheduled_call):
        """
        Cancels a scheduled call, ignoring cases where the event has already
        been canceled.
        """
        try:
            scheduled_call.cancel()
        except AlreadyCalled:  # pragma: no cover
            pass

    def _call_callback(self, _, deferred):
        """
        Calls the callback() on the given deferred instance, ignoring cases
        where it may already be called.  This can happen if acquire() has
        already run.
        """
        try:
            deferred.callback(None)
        except AlreadyCalledError:  # pragma: no cover
            pass

    def acquire(self, timeout=None):
        """
        This method operates the same as :meth:`DeferredLock.acquire` does
        except it requires a timeout argument.

        :param int timeout:
            The number of seconds to wait before timing out.

        :raises LockTimeoutError:
            Raised if the timeout was reached before we could acquire
            the lock.
        """
        assert timeout is None \
            or isinstance(timeout, (int, float)) and timeout > 0

        lock = DeferredLock.acquire(self)
        if timeout is None:
            return lock

        # Schedule a call to trigger finished.errback() which will raise
        # an exception.  If lock finishes first however cancel the timeout
        # and unlock the lock by calling finished.
        finished = Deferred()
        lock.addCallback(
            self._cancel_timeout,
            reactor.callLater(timeout, self._timeout, finished))
        lock.addCallback(self._call_callback, finished)

        return finished
