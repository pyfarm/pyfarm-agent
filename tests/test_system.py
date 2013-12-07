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
import uuid
import netifaces

from utcore import TestCase, skip_on_ci
from pyfarm.core.utility import convert
from pyfarm.core.enums import OS, OperatingSystem
from pyfarm.core.sysinfo import system, network, cpu, memory, user


class BaseSystem(TestCase):
    def test_user(self):
        try:
            import pwd
            sysuser = pwd.getpwuid(os.getuid())[0]

        except ImportError:
            import getpass
            sysuser = getpass.getuser()

        self.assertEqual(user.username(), sysuser)

    def test_uptime(self):
        t1 = system.uptime()
        t2 = time.time() - psutil.BOOT_TIME
        self.assertEqual(t2 - t1 < 5, True)

    def test_classvars(self):
        self.assertEqual(system.OS, OS)
        self.assertEqual(system.IS_LINUX, OS == OperatingSystem.LINUX)
        self.assertEqual(system.IS_WINDOWS, OS == OperatingSystem.WINDOWS)
        self.assertEqual(system.IS_MAC, OS == OperatingSystem.MAC)
        self.assertEqual(system.IS_OTHER, OS == OperatingSystem.OTHER)
        self.assertEqual(system.IS_POSIX,
                         OS in (OperatingSystem.LINUX, OperatingSystem.MAC))

    def test_case_sensitive_filesystem(self):
        fd, path = tempfile.mkstemp()
        if path.islower() or not path.isupper():
            senstive = os.path.isfile(path) and not os.path.isfile(path.upper())
        else:
            senstive = os.path.isfile(path) and not os.path.isfile(path.lower())

        self.assertEqual(system.CASE_SENSITIVE_FILESYSTEM, senstive)
        self.remove(path)

    def test_case_sensitive_environment(self):
        envvar_lower = uuid.uuid4().get_hex()
        envvar_upper = envvar_lower.upper()

        # populate environment then compare the difference
        os.environ.update({envvar_lower: "0", envvar_upper: "1"})
        self.assertEqual(
            system.CASE_SENSITIVE_ENVIRONMENT,
            os.environ[envvar_lower] != os.environ[envvar_upper])

        os.environ.pop(envvar_lower)
        os.environ.pop(envvar_upper)


class Network(TestCase):
    def test_packets_sent(self):
        v = psutil.net_io_counters(
            pernic=True)[network.interface()].packets_sent
        self.assertEqual(network.packets_sent() >= v, True)

    def test_packets_recv(self):
        v = psutil.net_io_counters(
            pernic=True)[network.interface()].packets_recv
        self.assertEqual(network.packets_received() >= v, True)

    def test_data_sent(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network.interface()].bytes_sent)
        self.assertEqual(network.data_sent() >= v, True)

    def test_data_recv(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network.interface()].bytes_recv)
        self.assertEqual(network.data_received() >= v, True)

    def test_error_incoming(self):
        v = psutil.net_io_counters(pernic=True)[network.interface()].errin
        self.assertEqual(network.incoming_error_count() >= v, True)

    def test_error_outgoing(self):
        v = psutil.net_io_counters(pernic=True)[network.interface()].errout
        self.assertEqual(network.outgoing_error_count() >= v, True)

    def test_hostname(self):
        _hostname = socket.gethostname()
        hostnames = set([
            _hostname, _hostname + ".", socket.getfqdn(),
            socket.getfqdn(_hostname + ".")])
        for hostname in hostnames:
            if hostname == network.hostname():
                break
        else:
            self.fail("failed to get hostname")

    def test_addresses(self):
        self.assertEqual(len(list(network.addresses())) >= 1, True)
        self.assertEqual(isinstance(list(network.addresses()), list), True)

    def test_interfaces(self):
        names = list(network.interfaces())
        self.assertEqual(len(names) > 1, True)
        self.assertEqual(isinstance(names, list), True)
        self.assertEqual(all(name in netifaces.interfaces() for name in names),
                         True)

        addresses = map(netifaces.ifaddresses, names)
        self.assertEqual(all(socket.AF_INET in i for i in addresses), True)

    @skip_on_ci
    def test_interface(self):
        self.assertEqual(any(
            i.get("addr") == network.ip()
            for i in netifaces.ifaddresses(
            network.interface()).get(socket.AF_INET, [])), True)


class Processor(TestCase):
    def test_count(self):
        try:
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
        except (ImportError, NotImplementedError):
            cpu_count = psutil.NUM_CPUS

        self.assertEqual(cpu.NUM_CPUS, cpu_count)

    def test_usertime(self):
        self.assertEqual(psutil.cpu_times().user <= cpu.user_time(), True)

    def test_systemtime(self):
        self.assertEqual(psutil.cpu_times().system <= cpu.system_time(), True)

    def test_idletime(self):
        self.assertEqual(psutil.cpu_times().idle <= cpu.idle_time(), True)

    def test_iowait(self):
        if system.IS_LINUX:
            self.assertEqual(cpu.iowait() <= psutil.cpu_times().iowait, True)
        else:
            self.assertEqual(cpu.iowait(), None)


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