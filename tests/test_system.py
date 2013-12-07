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

import os
import time
import socket
import psutil
import tempfile
import netifaces

from utcore import TestCase, skip_on_ci
from pyfarm.core.utility import convert
from pyfarm.core.enums import OS, OperatingSystem
from pyfarm.core.sysinfo import (
    os_info, network_info, cpu_info, memory, user_info)


class BaseSystem(TestCase):
    def test_user(self):
        try:
            import pwd
            sysuser = pwd.getpwuid(os.getuid())[0]

        except ImportError:
            import getpass
            sysuser = getpass.getuser()

        self.assertEqual(user_info.username(), sysuser)

    def test_uptime(self):
        t1 = os_info.uptime()
        t2 = time.time() - psutil.BOOT_TIME
        self.assertEqual(t2 - t1 < 5, True)

    def test_classvars(self):
        self.assertEqual(os_info.OS, OS)
        self.assertEqual(os_info.IS_LINUX, OS == OperatingSystem.LINUX)
        self.assertEqual(os_info.IS_WINDOWS, OS == OperatingSystem.WINDOWS)
        self.assertEqual(os_info.IS_MAC, OS == OperatingSystem.MAC)
        self.assertEqual(os_info.IS_OTHER, OS == OperatingSystem.OTHER)
        self.assertEqual(os_info.IS_POSIX,
                         OS in (OperatingSystem.LINUX, OperatingSystem.MAC))

    def test_case_sensitive(self):
        fid, path = tempfile.mkstemp()
        exists = map(os.path.isfile, [path, path.lower(), path.upper()])
        if not any(exists):
            raise ValueError("failed to determine if path was case sensitive")
        elif all(exists):
            self.assertEqual(os_info.CASE_SENSITIVE, False)
        elif exists.count(True) == 1:
            self.assertEqual(os_info.CASE_SENSITIVE, True)

        self.remove(path)


class Network(TestCase):
    def test_packets_sent(self):
        v = psutil.net_io_counters(
            pernic=True)[network_info.interface()].packets_sent
        self.assertEqual(network_info.packets_sent() >= v, True)

    def test_packets_recv(self):
        v = psutil.net_io_counters(
            pernic=True)[network_info.interface()].packets_recv
        self.assertEqual(network_info.packets_received() >= v, True)

    def test_data_sent(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network_info.interface()].bytes_sent)
        self.assertEqual(network_info.data_sent() >= v, True)

    def test_data_recv(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network_info.interface()].bytes_recv)
        self.assertEqual(network_info.data_received() >= v, True)

    def test_error_incoming(self):
        v = psutil.net_io_counters(pernic=True)[network_info.interface()].errin
        self.assertEqual(network_info.incoming_error_count() >= v, True)

    def test_error_outgoing(self):
        v = psutil.net_io_counters(pernic=True)[network_info.interface()].errout
        self.assertEqual(network_info.outgoing_error_count() >= v, True)

    def test_hostname(self):
        _hostname = socket.gethostname()
        hostnames = set([
            _hostname, _hostname + ".", socket.getfqdn(),
            socket.getfqdn(_hostname + ".")])
        for hostname in hostnames:
            if hostname == network_info.hostname():
                break
        else:
            self.fail("failed to get hostname")

    def test_addresses(self):
        self.assertEqual(len(list(network_info.addresses())) >= 1, True)
        self.assertEqual(isinstance(list(network_info.addresses()), list), True)

    def test_interfaces(self):
        names = list(network_info.interfaces())
        self.assertEqual(len(names) > 1, True)
        self.assertEqual(isinstance(names, list), True)
        self.assertEqual(all(name in netifaces.interfaces() for name in names),
                         True)

        addresses = map(netifaces.ifaddresses, names)
        self.assertEqual(all(socket.AF_INET in i for i in addresses), True)

    @skip_on_ci
    def test_interface(self):
        self.assertEqual(any(
            i.get("addr") == network_info.ip()
            for i in netifaces.ifaddresses(
            network_info.interface()).get(socket.AF_INET, [])), True)


class Processor(TestCase):
    def test_count(self):
        try:
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
        except (ImportError, NotImplementedError):
            cpu_count = psutil.NUM_CPUS

        self.assertEqual(cpu_info.NUM_CPUS, cpu_count)

    def test_usertime(self):
        self.assertEqual(psutil.cpu_times().user <= cpu_info.user_time(),
                         True)

    def test_systemtime(self):
        self.assertEqual(psutil.cpu_times().system <= cpu_info.system_time(),
                         True)

    def test_idletime(self):
        self.assertEqual(psutil.cpu_times().idle <= cpu_info.idle_time(),
                         True)

    def test_iowait(self):
        if os_info.IS_LINUX:
            self.assertEqual(cpu_info.iowait() <= psutil.cpu_times().iowait,
                             True)
        else:
            self.assertEqual(cpu_info.iowait(), None)


class Memory(TestCase):
    def test_totalram(self):
        self.assertEqual(memory.TOTAL_RAM,
                         convert.bytetomb(psutil.TOTAL_PHYMEM))

    def test_totalswap(self):
        self.assertEqual(memory.TOTAL_SWAP,
                         convert.bytetomb(psutil.swap_memory().total))

    def test_swapused(self):
        v1 = convert.bytetomb(psutil.swap_memory().used)
        v2 = memory.swap_used()
        self.assertEqual(v1-v2 < 5, True)

    def test_swapfree(self):
        v1 = convert.bytetomb(psutil.swap_memory().free)
        v2 = memory.swap_free()
        self.assertEqual(v1-v2 < 5, True)

    def test_ramused(self):
        v1 = convert.bytetomb(psutil.virtual_memory().used)
        v2 = memory.ram_used()
        self.assertEqual(v1-v2 < 5, True)

    def test_ramfree(self):
        v1 = convert.bytetomb(psutil.virtual_memory().available)
        v2 = memory.ram_free()
        self.assertEqual(v1-v2 < 5, True)