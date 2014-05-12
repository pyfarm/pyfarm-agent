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
import socket
import tempfile
import time
import psutil
import uuid
from os.path import isfile

try:
    import pwd
except ImportError:
    import getpass
    pwd = NotImplemented

import netifaces

from pyfarm.core.utility import convert
from pyfarm.core.enums import LINUX, WINDOWS
from pyfarm.agent.testutil import TestCase, skip_on_ci, skip_if
from pyfarm.agent.sysinfo import system, network, cpu, memory, user


class BaseSystem(TestCase):
    def test_user(self):
        if pwd is not NotImplemented:
            username = pwd.getpwuid(os.getuid())[0]
        else:
            username = getpass.getuser()

        self.assertEqual(user.username(), username)

    def test_uptime(self):
        t1 = system.uptime()
        t2 = time.time() - psutil.boot_time()
        self.assertEqual(t2 - t1 < 5, True)

    def test_case_sensitive_filesystem(self):
        fd, path = tempfile.mkstemp()
        self.assertEqual(
            not all(map(isfile, [path, path.lower(), path.upper()])),
            system.filesystem_is_case_sensitive())
        self.add_cleanup_path(path)

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

    def test_operation_system(self):
        self.assertEqual(system.operating_system("linux"), "linux")
        self.assertEqual(system.operating_system("windows"), "windows")
        self.assertEqual(system.operating_system("darwin"), "mac")
        self.assertEqual(system.operating_system("foobar"), "other")

    def test_machine_architecture(self):
        for arch in ("amd64", "x86_64", "ia64", "Syswow64"):
            self.assertEqual(system.machine_architecture(arch), 64)

        for arch in ("i386", "i686", "x86"):
            self.assertEqual(system.machine_architecture(arch), 32)

        self.assertRaises(
            NotImplementedError, lambda: system.machine_architecture(""))
        self.assertRaises(
            NotImplementedError, lambda: system.machine_architecture("foobar"))


class Network(TestCase):
    @skip_if(WINDOWS, "Not POSIX")
    def test_packets_sent_posix(self):
        v = psutil.net_io_counters(
            pernic=True)[network.interface()].packets_sent
        self.assertEqual(network.packets_sent() >= v, True)

    @skip_if(WINDOWS, "Not POSIX")
    def test_packets_recv_posix(self):
        v = psutil.net_io_counters(
            pernic=True)[network.interface()].packets_recv
        self.assertEqual(network.packets_received() >= v, True)

    @skip_if(WINDOWS, "Not POSIX")
    def test_data_sent_posix(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network.interface()].bytes_sent)
        self.assertEqual(network.data_sent() >= v, True)

    @skip_if(WINDOWS, "Not POSIX")
    def test_data_recv_posix(self):
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[network.interface()].bytes_recv)
        self.assertEqual(network.data_received() >= v, True)

    @skip_if(WINDOWS, "Not POSIX")
    def test_error_incoming_posix(self):
        v = psutil.net_io_counters(pernic=True)[network.interface()].errin
        self.assertEqual(network.incoming_error_count() >= v, True)

    @skip_if(WINDOWS, "Not POSIX")
    def test_error_outgoing_posix(self):
        v = psutil.net_io_counters(pernic=True)[network.interface()].errout
        self.assertEqual(network.outgoing_error_count() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_packets_sent_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = psutil.net_io_counters(
            pernic=True)[interface].packets_sent
        self.assertEqual(network.packets_sent() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_packets_recv_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = psutil.net_io_counters(
            pernic=True)[interface].packets_recv
        self.assertEqual(network.packets_received() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_data_sent_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[interface].bytes_sent)
        self.assertEqual(network.data_sent() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_data_recv_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = convert.bytetomb(psutil.net_io_counters(
            pernic=True)[interface].bytes_recv)
        self.assertEqual(network.data_received() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_error_incoming_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = psutil.net_io_counters(pernic=True)[interface].errin
        self.assertEqual(network.incoming_error_count() >= v, True)

    @skip_if(not WINDOWS, "Not Windows")
    def test_error_outgoing_windows(self):
        interface = network.interface_guid_to_nicename(network.interface())
        v = psutil.net_io_counters(pernic=True)[interface].errout
        self.assertEqual(network.outgoing_error_count() >= v, True)

    def test_hostname_ignore_dns_mappings(self):
        reverse_hostnames = set()
        for address in network.addresses():
            try:
                dns_name, aliases, dns_addresses = socket.gethostbyaddr(address)
            except socket.herror:
                pass
            else:
                if address in dns_addresses:
                    reverse_hostnames.add(dns_name)

        local_hostname = socket.gethostname()
        local_fqdn_query = socket.getfqdn()

        if local_fqdn_query in reverse_hostnames:
            hostname = local_fqdn_query
        elif local_hostname in reverse_hostnames:
            hostname = local_hostname
        else:
            hostname = socket.getfqdn(local_hostname)

        self.assertEqual(
            network.hostname(trust_name_from_ips=False),
            hostname)

    def test_hostname_trust_dns_mappings(self):
        reverse_hostnames = set()
        for address in network.addresses():
            try:
                dns_name, aliases, dns_addresses = socket.gethostbyaddr(address)
            except socket.herror:
                pass
            else:
                if address in dns_addresses:
                    reverse_hostnames.add(dns_name)

        if len(reverse_hostnames) == 1:
            self.assertEqual(
                network.hostname(trust_name_from_ips=True),
                reverse_hostnames.pop())
        else:
            self.skipTest(
                "This host's addresses resolve to more than one hostname")

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

    @skip_if(WINDOWS, "Not POSIX")
    def test_interface_guid_to_nicename_windows_only(self):
        self.assertRaises(
            NotImplementedError,
            lambda: network.interface_guid_to_nicename(None))

    @skip_if(WINDOWS, "Not POSIX")
    def test_wmi_import_not_imported(self):
        self.assertIs(network.wmi, NotImplemented)

    @skip_if(not WINDOWS, "Not Windows")
    def test_wmi_imported(self):
        self.assertIsNot(network.wmi, NotImplemented)


class Processor(TestCase):
    def test_count(self):
        self.assertEqual(psutil.cpu_count(), cpu.total_cpus())

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
                         convert.bytetomb(psutil.virtual_memory().total))

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