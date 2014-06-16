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
Job Type Internals
==================

Contains classes which contain internal methods for
the :class:`pyfarm.jobtypes.core.jobtype.JobType` class.
"""

import os
import tempfile
from os.path import dirname, isdir, join, isfile
from string import Template

from twisted.internet import reactor, threads
from twisted.internet.defer import Deferred

from pyfarm.agent.config import config
from pyfarm.agent.logger import getLogger
from pyfarm.agent.http.core.client import get, http_retry_delay

logcache = getLogger("jobtypes.cache")
logger = getLogger("jobtypes.core")


class Cache(object):
    """Internal methods for caching job types"""
    CACHE_DIRECTORY = Template(
        config.get("jobtype_cache_directory", "")).safe_substitute(
        temp=tempfile.gettempdir())

    if not CACHE_DIRECTORY:
        CACHE_DIRECTORY = None
        logger.warning("Job type cache directory has been disabled.")

    elif not isdir(CACHE_DIRECTORY):
        try:
            os.makedirs(CACHE_DIRECTORY)

        except OSError:
            logger.error(
                "Failed to create %r.  Job type caching is "
                "now disabled.", CACHE_DIRECTORY)
            CACHE_DIRECTORY = None

        else:
            logger.info("Created job type cache directory %r", CACHE_DIRECTORY)

    else:
        logger.debug("Job type cache directory is %r", CACHE_DIRECTORY)

    @classmethod
    def _download_jobtype(cls, name, version):
        """
        Downloads the job type specified in ``assignment``.  This
        method will pass the response it receives to :meth:`_cache_jobtype`
        however failures will be retried.
        """
        url = config["master-api"] + "/jobtypes/" + name + "/" + str(version)
        result = Deferred()
        download = lambda *_: \
            get(url,
                callback=result.callback,
                errback=lambda: reactor.callLater(http_retry_delay(), download))
        download()
        return result

    @classmethod
    def _cache_jobtype(cls, cache_key, jobtype):
        """
        Once the job type is downloaded this classmethod is called
        to store it on disk.  In the rare even that we fail to write it
        to disk, we store it in memory instead.
        """
        filename = str(join(
            CACHE_DIRECTORY,
            "_".join(map(str, cache_key)) + "_" + jobtype["classname"] + ".py"))
        success = Deferred()

        def write_to_disk(filename):
            try:
                os.makedirs(dirname(filename))
            except (IOError, OSError):
                pass

            if isfile(filename):
                logcache.debug("%s is already cached on disk", filename)
                jobtype.pop("code", None)
                return filename, jobtype

            try:
                with open(filename, "w") as stream:
                    stream.write(jobtype["code"])

                jobtype.pop("code", None)
                return filename, jobtype

            # If the above fails, use a temp file instead
            except (IOError, OSError) as e:
                fd, tmpfilepath = tempfile.mkstemp(suffix=".py")
                logcache.warning(
                    "Failed to write %s, using %s instead: %s",
                    filename, tmpfilepath, e)

                with open(tmpfilepath, "w") as stream:
                    stream.write(jobtype["code"])

                jobtype.pop("code", None)
                return filename, jobtype

        def written_to_disk(results):
            filename, jobtype = results
            cls.cache[cache_key] = (jobtype, filename)
            logcache.info("Created cache for %r at %s", cache_key, filename)
            success.callback((jobtype, filename))

        def failed_to_write_to_disk(error):
            logcache.error(
                "Failed to write job type cache to disk, will use "
                "memory instead: %s", error)

            # The code exists in the job type because it's
            # only removed on success.
            cls.cache[cache_key] = (jobtype, None)
            success.callback((jobtype, None))

        # Defer the write process to a thread so we don't
        # block the reactor if the write is slow
        writer = threads.deferToThread(write_to_disk, filename)
        writer.addCallbacks(written_to_disk, failed_to_write_to_disk)
        return success
