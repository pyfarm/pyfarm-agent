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
Memory
------

Provides information about memory including swap usage, system memory usage,
and general capacity information.
"""

import psutil
from pyfarm.core.utility import convert


def swap_used():
    """Amount of swap currently in use"""
    return convert.bytetomb(psutil.swap_memory().used)


def swap_free():
    """Amount of swap currently free"""
    return convert.bytetomb(psutil.swap_memory().free)


def ram_used():
    """Amount of swap currently free"""
    return convert.bytetomb(psutil.virtual_memory().used)


def ram_free():
    """Amount of ram currently free"""
    return convert.bytetomb(psutil.virtual_memory().available)


def total_ram():
    """Total physical memory (ram) installed on the system"""
    return convert.bytetomb(psutil.phymem_usage().total)


def total_swap():
    """Total virtual memory (swap) installed on the system"""
    return convert.bytetomb(psutil.swap_memory().total)
