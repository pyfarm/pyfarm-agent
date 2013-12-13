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
Tasks
-----

Simple tasks which are run at a scheduled interval by
:class:`.ScheduledTaskManager`
"""


from functools import partial

from twisted.python import log
from twisted.internet.task import LoopingCall

from pyfarm.core.sysinfo import memory

memlog = partial(log.msg, system="task.memory_utilization")


class ScheduledTaskManager(object):
    """
    Manages and keeps track of several scheduled tasks.
    """
    def __init__(self):
        self.tasks = {}
        self.log = partial(log.msg, system=self.__class__.__name__)

    def register(self, function, interval, start=False,
                 func_args=None, func_kwargs=None):
        """
        Register a callable function to run at a given interval.  This function
        will do nothing if ``function`` has already been registered.

        :param function:
            a callable function that should be run on an interval

        :type interval: int or float
        :param interval:
            the interval in which ``function`` should be urn

        :param bool start:
            if True, start the interval timer after it has been added

        :param tuple func_args:
            the positional arguments to pass into ``function``

        :param dict func_kwargs:
            the keyword arguments to pass into ``function``

        :exception AssertionError:
            raised if ``function`` is not callable
        """
        args = func_args or ()
        kwargs = func_kwargs or {}
        assert callable(function)
        assert isinstance(args, (tuple, list))
        assert isinstance(kwargs, dict)

        if function not in self.tasks:
            looping_call = LoopingCall(function, *args, **kwargs)
            self.tasks[function] = (looping_call, interval)
            self.log("registering %s")
            if start:
                looping_call.start(interval)

    def start(self, now=True):
        """
        start all :class:`.LoopingCall` instances stored from
        :meth:`.register`
        """
        self.log("starting tasks")

        for function, (looping_call, interval) in self.tasks.iteritems():
            if not looping_call.running:
                self.log("...starting %s" % function.__name__)
                looping_call.start(interval, now=now)

    def stop(self):
        """
        stop all :class:`.LoopingCall` instances stored from
        :meth:`.register`
        """
        self.log("stopping tasks")

        for function, (looping_call, interval) in self.tasks.iteritems():
            if looping_call.running:
                self.log("...stopping %s" % function.__name__)
                looping_call.stop()
            else:
                self.log("...%s is already stopped" % function.__name__)



# TODO: only send memory information if memory has risen by X amount in N time
# TODO: replace with callable class (to support the above)
def memory_utilization(config):
    """
    Returns the amount of free free and the amount of swap used.
    """
    try:
        ram_report_delta = config["ram_report_delta"]
        ram_record_delta = config["ram_record_delta"]
        swap_report_delta = config["swap_report_delta"]
        swap_record_delta = config["swap_record_delta"]

    except KeyError:
        # TODO: handle this better, this was added to just stop being annoying
        import warnings
        warnings.warn("configuration not available yet")
        return

    ram_free, swap_used = memory.ram_free(), memory.swap_used()
    log.msg("ram_free=%s, swap_used=%s" % (ram_free, swap_used),
            system="task.memory_utilization")
    return ram_free, swap_used
