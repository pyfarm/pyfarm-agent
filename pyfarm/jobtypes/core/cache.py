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
Cache
-----

This module is responsible for retrieval and caching of job types.  These
functions provide a small interface to the config object.  It's
provided so the way we store job types can be handled in a single
location and so that we can control the structure from a couple of
places instead of the whole project.
"""

from json import loads

try:
    from httplib import OK
except ImportError:  # pragma: no cover
    from http.client import OK

try:
    import zlib
except ImportError:  # pragma: no cover
    zlib = NotImplemented

from twisted.internet import threads
from twisted.internet.defer import Deferred

from pyfarm.core.config import read_env_bool
from pyfarm.core.enums import STRING_TYPES, INTERGER_TYPES
from pyfarm.core.logger import getLogger
from pyfarm.agent.config import config
from pyfarm.agent.http.core.client import get
from pyfarm.agent.utility import dumps

logger = getLogger("jobtypes.cache")


COMPRESSED = \
    zlib is not NotImplemented and read_env_bool(
        "PYFARM_AGENT_COMPRESS_JOBTYPES", True)


def _retrieve_jobtype(jobtype, version):
    """
    The internal method used to retrieve a specific job type
    and version which is then directly returned.  This function
    generally should not be directly executed and instead :func:`.get`
    should be used as that will also handle retrieval of missing job
    types.

    :returns:
        Returns a dictionary structure containing all the
        information necessary to execute a job type.
    """
    assert "jobtypes" in config
    assert (jobtype, version) in config["jobtypes"]
    logger.debug("Returning job type %s", (jobtype, version))
    data = config["jobtypes"][(jobtype, version)]

    # If the data is being stored as a string due to compression
    # then package the
    if COMPRESSED:
        data = loads(zlib.decompress(data))

    return data


def _store_jobtype(data, replace=False):
    """
    Given the data directly from the master store the job type in
    the config
    """
    assert isinstance(data, dict)
    cache_key = data["name"], data["version"]

    # Either we have not cached the job type or we've been
    # ordered to replace it
    if cache_key not in config["jobtypes"] or replace:
        if COMPRESSED:
            logger.debug("Compressing job type %s", cache_key)
            config["jobtypes"][cache_key] = zlib.compress(dumps(data), 9)
        else:
            logger.debug("Storing job type %s", cache_key)
            config["jobtypes"][cache_key] = data

    return data


def download_jobtype(jobtype, version):
    """Downloads the requested job type and version from the master"""
    assert isinstance(jobtype, STRING_TYPES)
    assert isinstance(version, INTERGER_TYPES)
    url = "%(master-api)s/jobtypes/%(jobtype)s/versions/%(version)s" % {
        "master-api": config["master-api"],
        "jobtype": jobtype,
        "version": version}

    logger.debug("Downloading job type from %s", url)

    # Retrieve the job type fire deferred.callback on success
    # and deferred.errback on failure
    deferred = Deferred()
    get(str(url),
        callback=deferred.callback,
        errback=deferred.errback)
    return deferred


def get_jobtype(jobtype, version):
    """
    Returns a deferred object which will either return the
    cached job type or _retrieve the job type, cache it, and
    then return it.
    """
    assert isinstance(jobtype, STRING_TYPES)
    assert isinstance(version, INTERGER_TYPES)

    # Job type is cached, defer the unpacking to a thread
    # so any decompression won't block the reactor
    if (jobtype, version) in config["jobtypes"]:
        return threads.deferToThread(_retrieve_jobtype, jobtype, version)

    deferred_result = Deferred()

    def success(response):
        if response.code != OK:
            response.request.retry()
        else:
            pack = threads.deferToThread(_store_jobtype, response.json())
            pack.addCallback(deferred_result.callback)

    def failure(failure):
        return get_jobtype(jobtype, version)

    # Job type is not cached, we must _retrieve it.
    deferred = download_jobtype(jobtype, version)
    deferred.addCallbacks(success, failure)

    return deferred_result
