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
Taks
----

Simple tasks which are run at a scheduled interval by the
manager service.
"""

from functools import partial

from twisted.python import log

from pyfarm.core.sysinfo import memory

memlog = partial(log.msg, system="task.memory_utilization")


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
