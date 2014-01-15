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
from os.path import isfile

from utcore import TestCase, skip_on_ci
from pyfarm.core.utility import convert
from pyfarm.core.sysinfo import system, network, cpu, memory, user
from pyfarm.core.enums import LINUX


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

    def test_case_sensitive_filesystem(self):
        fd, path = tempfile.mkstemp()
        self.assertEqual(
            not all(map(isfile, [path, path.lower(), path.upper()])),
            system.filesystem_is_case_sensitive())
        self.remove(path)

    def test_case_sensitive_environment(self):
        envvar_lower = "PYFARM_CHECK_ENV_CASE_" + uuid.uuid4().hex
        envvar_upper = envvar_lower.upper()

        # populate environment then compare the difference
        os.environ.update({envvar_lower: "0", envvar_upper: "1"})
        self.assertEqual(
            os.environ[envvar_lower] != os.environ[envvar_upper],
            system.environment_is_case_sensitive())

        # remove the envvars we just made
        for envvar in (envvar_lower, envvar_upper):
            os.environ.pop(envvar, None)


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
        if LINUX:
            self.assertEqual(cpu.iowait() <= psutil.cpu_times().iowait, True)
        else:
            self.assertEqual(cpu.iowait(), None)


class Memory(TestCase):
    def test_totalram(self):
        self.assertEqual(memory.total_ram(),
                         convert.bytetomb(psutil.TOTAL_PHYMEM))

    def test_totalswap(self):
        self.assertEqual(memory.total_swap(),
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