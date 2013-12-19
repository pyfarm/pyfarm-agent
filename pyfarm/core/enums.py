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


class EnumValue(namedtuple("Values", ("int", "str"))):
    """
    Stores values to be used in an enum.  Each time this
    class is instanced it will ensure that the input values
    are of the correct type and unique.
    """
    _values = set()

    def __init__(self, *args, **kwargs):
        if not isinstance(self.int, int):
            raise TypeError("`int` must be an integer")

        if not isinstance(self.str, basestring):
            raise TypeError("`str` must be a string")

        if self.int in self._values:
            raise ValueError("value %s is being reused" % self.int)

        self._values.add(self.int)


def cast_enum(enum, enum_type):
    """
    Pulls the requested ``enum_type`` from ``enum`` and produce a new
    named tuple which contains only the requested data

    >>> from pyfarm.core.enums import Enum, EnumValue
    >>> FooBase = Enum("Foo", A=EnumValue(int=1, str="1")
    >>> Foo = cast_enum(FooBase, str)
    >>> assert Foo.A == "1"
    >>> Foo = cast_enum(FooBase, int)
    >>> assert Foo.A == 1
    >>> assert Foo._map == {"A": 1, 1: "A"}

    .. warning::
        This function does not perform any kind of caching.  For the most
        efficient usage it should only be called once per process or
        module for a given enum and enum_type combination.
    """
    enum_data = {}
    reverse_map = {}

    # construct the reverse mapping and push
    # the request type into enum_data
    for key, value in enum._asdict().iteritems():
        reverse_map[value.int] = value.str
        reverse_map[value.str] = value.int

        if enum_type is int:
            enum_data[key] = value.int
        elif enum_type is str:
            enum_data[key] = value.str
        else:
            raise ValueError("valid values for `enum_type` are int or str")

    class MappedEnum(namedtuple(enum.__class__.__name__, enum_data.keys())):
        _map = reverse_map

    return MappedEnum(**enum_data)


# 1xx - work states
# NOTE: these values are directly tested test_enums.test_direct_work_values
_WorkState = Enum(
    "WorkState",
    PAUSED=EnumValue(100, "paused"),
    QUEUED=EnumValue(101, "queued"),
    BLOCKED=EnumValue(102, "blocked"),
    ALLOC=EnumValue(103, "alloc"),
    ASSIGN=EnumValue(104, "assign"),
    RUNNING=EnumValue(105, "running"),
    DONE=EnumValue(106, "done"),
    FAILED=EnumValue(107, "failed"),
    JOBTYPE_FAILED_IMPORT=EnumValue(108, "jobtype_failed_import"),
    JOBTYPE_INVALID_CLASS=EnumValue(109, "jobtype_invalid_class"),
    NO_SUCH_COMMAND=EnumValue(110, "no_such_command"))

# 2xx - agent states
# NOTE: these values are directly tested test_enums.test_direct_agent_values
_AgentState = Enum(
    "AgentState",
    DISABLED=EnumValue(200, "disabled"),
    OFFLINE=EnumValue(201, "offline"),
    ONLINE=EnumValue(202, "online"),
    RUNNING=EnumValue(203, "running"))

# 3xx - non-queue related modes or states
# NOTE: these values are directly tested test_enums.test_direct_os_values
_OperatingSystem = Enum(
    "OperatingSystem",
    LINUX=EnumValue(300, "linux"),
    WINDOWS=EnumValue(301, "windows"),
    MAC=EnumValue(302, "mac"),
    OTHER=EnumValue(303, "other"))

# NOTE: these values are directly tested test_enums.test_direct_agent_addr
_UseAgentAddress = Enum(
    "UseAgentAddress",
    LOCAL=EnumValue(310, "local"),
    REMOTE=EnumValue(311, "remote"),
    HOSTNAME=EnumValue(312, "hostname"),
    PASSIVE=EnumValue(313, "passive"))

# NOTE: these values are directly tested test_enums.test_direct_agent_addr
_JobTypeLoadMode = Enum(
    "JobTypeLoadMode",
    DOWNLOAD=EnumValue(320, "download"),
    OPEN=EnumValue(321, "open"),
    IMPORT=EnumValue(322, "import"))

# cast the enums defined above
WorkState = cast_enum(_WorkState, str)
AgentState = cast_enum(_AgentState, str)
OperatingSystem = cast_enum(_OperatingSystem, str)
UseAgentAddress = cast_enum(_UseAgentAddress, str)
JobTypeLoadMode = cast_enum(_JobTypeLoadMode, str)
DBWorkState = cast_enum(_WorkState, int)
DBAgentState = cast_enum(_AgentState, int)
DBOperatingSystem = cast_enum(_OperatingSystem, int)
DBUseAgentAddress = cast_enum(_UseAgentAddress, int)
DBJobTypeLoadMode = cast_enum(_JobTypeLoadMode, int)

RUNNING_WORK_STATES = set([
    WorkState.ALLOC,
    WorkState.ASSIGN,
    WorkState.RUNNING])

DB_RUNNING_WORK_STATES = set([
    DBWorkState.ALLOC,
    DBWorkState.ASSIGN,
    DBWorkState.RUNNING])

FAILED_WORK_STATES = set([
    WorkState.FAILED,
    WorkState.JOBTYPE_FAILED_IMPORT,
    WorkState.JOBTYPE_INVALID_CLASS,
    WorkState.NO_SUCH_COMMAND])

DB_FAILED_WORK_STATES = set([
    DBWorkState.FAILED,
    DBWorkState.JOBTYPE_FAILED_IMPORT,
    DBWorkState.JOBTYPE_INVALID_CLASS,
    DBWorkState.NO_SUCH_COMMAND])

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
