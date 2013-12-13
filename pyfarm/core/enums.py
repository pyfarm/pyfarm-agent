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
Enums
=====

Provides enum values for certain aspect of PyFarm.  See below for more
detailed information.


Operating System
----------------

Describes an operating system type.

.. csv-table:: **OperatingSystem**
    :header: Attribute, Description
    :widths: 10, 50

    LINUX, operating system on agent is a Linux variant
    WINDOWS, operating system on agent is a Windows variant
    MAC, operating system on agent is an Apple OS variant


Job Type Load Mode
------------------

Determines how a custom job type will be loaded.

.. csv-table::
    :header: Attribute, Description
    :widths: 10, 50

    DOWNLOAD, download the jobtype file from a url
    OPEN, open the jobtype file from a url
    IMPORT, import the jobtype from the given string (ex. `foo.bar.ClassName`)


Agent State
-----------

The last known state of the remote agent, used for making queue decisions
and locking off resources.

.. csv-table::
    :header: Attribute, Description
    :widths: 10, 50

    OFFLINE, agent cannot be reached
    ONLINE, agent is waiting for work
    DISABLED, agent is online but cannot accept work
    RUNNING, agent is currently processing work
    ALLOC, special internal state used when the agent entry is being built


Work State
----------

The state a job or task is currently in.  These values apply more directly
to tasks as job statuses are built from task status values.

.. csv-table::
    :header: Attribute, Description
    :widths: 10, 50

    PAUSED, this task cannot be assigned right now but can be once unpaused
    BLOCKED, this task cannot be assigned to an agent at this point in time
    QUEUED, waiting on queue to assign this work
    ASSIGN, work has been assigned to an agent but is waiting to start
    RUNNING, work is currently being processed
    DONE, work is finished (previous failures may be present)
    FAILED, work as failed and cannot be continued
    ALLOC, special internal state for a job or task entry is being built


REST API Errors
---------------

Various error which the REST api may throw.  Numerical values will remain
constant however the error message may be rewritten.

.. csv-table::
    :header: Attribute, Integer Value, Description
    :widths: 10, 5, 50

    JSON_DECODE_FAILED, 0, failed to decode any json data from the request
    UNEXPECTED_DATATYPE, 1, the base data type decoded for the json class was not what was expected
    MISSING_FIELDS, 2, one or more of the expected fields were missing in the request
    UNEXPECTED_NULL, 3, a null value was found in a field that requires a non-null value
    DATABASE_ERROR, 4, problem inserting or updating entry in database
    EXTRA_FIELDS_ERROR, 5, an unexpected number of fields or columns were provided


Use Agent Address
-----------------

Describes which address should be used to contact the agent

.. csv-table::
    :header: Attribute, Description
    :widths: 10, 50

    LOCAL, use the address which was provided by the agent
    REMOTE, use the address which we received the request from
    HOSTNAME, disregard both the local IP and the remote IP and use the hostname
    PASSIVE, agent cannot be contacted but will still request work and process jobs
"""

import sys
from warnings import warn

try:
    from collections import namedtuple
except ImportError:  # pragma: no cover
    from pyfarm.core.backports import namedtuple

from pyfarm.core.warning import NotImplementedWarning

__all__ = [
    "OperatingSystem", "JobTypeLoadMode", "AgentState", "WorkState",
    "APIError", "UseAgentAddress"]


def Enum(classname, **kwargs):
    """
    Produce an enum object using :func:`.namedtuple`

    >>> Foo = Enum("Foo", A=1, B=2)
    >>> assert Foo.A == 1 and Foo.B == 2
    >>> FooTemplate = Enum("Foo", A=int, instance=False)
    >>> Foo = FooTemplate(A=1)
    >>> assert Foo.A == 1

    :param str classname:
        the name of the class to produce

    :keyword to_dict:
        a callable function to add to the named tuple for
        converting the internal values into a dictionary

    :keyword bool instance:
        by default calling :func:`.Enum` will produce an instanced
        :func:`.namedtuple` object, setting ``instance`` to False
        will instead produce the named tuple without instancing it
    """
    to_dict = kwargs.pop("to_dict", None)
    instance = kwargs.pop("instance", True)
    template = namedtuple(classname, kwargs.keys())

    if to_dict is not None:
        setattr(template, "to_dict", to_dict)

    if instance:
        return template(**kwargs)
    return template


# 1xx - work states
WorkState = Enum(
    "WorkState",
    PAUSED=100, QUEUED=101, BLOCKED=102, ALLOC=103, ASSIGN=104,
    RUNNING=105, DONE=106, FAILED=107, FAILED_IMPORT=108)

# 2xx - agent states
AgentState = Enum(
    "AgentState",
    DISABLED=200, OFFLINE=201, ONLINE=202, RUNNING=203)

# 3xx - non-queue related modes or states
OperatingSystem = Enum(
    "OperatingSystem",
    LINUX=300, WINDOWS=301, MAC=302, OTHER=303)

UseAgentAddress = Enum(
    "UseAgentAddress",
    LOCAL=310, REMOTE=311, HOSTNAME=312, PASSIVE=313)

JobTypeLoadMode = Enum(
    "JobTypeLoadMode",
    DOWNLOAD=320, OPEN=321, IMPORT=322)

APIErrorValue = Enum(
    "APIErrorValue",
    value=int, description=str,
    instance=False, to_dict=lambda self: {
        "value": self.value, "description": self.description})

APIError = Enum(
    "APIError",
    JSON_DECODE_FAILED=APIErrorValue(
        1, "failed to decode any json data from the request"),
    UNEXPECTED_DATATYPE=APIErrorValue(
        2, "the base data type decoded for the json class was not what was "
           "expected"),
    MISSING_FIELDS=APIErrorValue(
        3, "one or more of the expected fields were missing in the request"),
    UNEXPECTED_NULL=APIErrorValue(
        4, "a null value was found in a field that requires a non-null value"),
    DATABASE_ERROR=APIErrorValue(
        5, "problem inserting or updating entry in database"),
    EXTRA_FIELDS_ERROR=APIErrorValue(
        6, "an unexpected number of fields or columns were provided"))


def get_operating_system(platform=sys.platform):
    """returns the operating system for the given platform"""
    if platform.startswith("linux"):
        return OperatingSystem.LINUX
    elif platform.startswith("win"):
        return OperatingSystem.WINDOWS
    elif platform.startswith("darwin"):
        return OperatingSystem.MAC
    else:
        warn("unknown operating system: %s" % platform, NotImplementedWarning)
        return OperatingSystem.OTHER


OS = get_operating_system()
