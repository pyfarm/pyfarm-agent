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
Manager Tasks
=============

Simple tasks which are run at a scheduled interval by the
manager service.
"""

from functools import partial
from twisted.python import log
from pyfarm.core.sysinfo import memory_info


log_memory = partial(log.msg, system="task.memory_utilization")

def memory_utilization():
    """
    Returns the amount of free free and the amount of swap used.
    """
    ram_free, swap_used = memory_info.ram_free(), memory_info.swap_used()
    log_memory("ram_free=%s, swap_used=%s" % (ram_free, swap_used))
    return ram_free, swap_used
