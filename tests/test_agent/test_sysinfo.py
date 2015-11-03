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

import ctypes
import imp
import os
import socket
import time
import json
import psutil
import subprocess
import sys
import uuid
from contextlib import nested
from httplib import OK, INTERNAL_SERVER_ERROR, NOT_FOUND
from os.path import isfile
from StringIO import StringIO
from textwrap import dedent

try:
    from xml.etree.cElementTree import ElementTree
except ImportError:  # pragma: no cover
    from xml.etree.ElementTree import ElementTree

try:
    import pwd
except ImportError:  # pragma: no cover
    pwd = NotImplemented

try:  # pragma: no cover
    import win32api
    from win32com.shell import shell
except ImportError:  # pragma: no cover
    win32api = NotImplemented

try:
    import getpass
except ImportError:  # pragma: no cover
    getpass = NotImplemented

try:
    from os import getuid
except ImportError:  # pragma: no cover
    getuid = NotImplemented

from mock import Mock, patch
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, succeed

from pyfarm.core.utility import convert
from pyfarm.core.enums import LINUX, WINDOWS
from pyfarm.agent.http.core.client import get_direct
from pyfarm.agent.testutil import TestCase, skipIf, APITestServer
from pyfarm.agent.sysinfo import (
    system, network, cpu, memory, user, disks, graphics, software)


class TestSystem(TestCase):
    def test_uptime(self):
        t1 = system.uptime()
        t2 = time.time() - psutil.boot_time()
        self.assertEqual(t2 - t1 < 5, True)

    def test_case_sensitive_filesystem(self):
        path = self.create_file()
        self.assertEqual(
            not all(map(isfile, [path, path.lower(), path.upper()])),
            system.filesystem_is_case_sensitive())

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

        with self.assertRaises(NotImplementedError):
            system.machine_architecture("")

        with self.assertRaises(NotImplementedError):
            system.machine_architecture("foobar")


class TestNetwork(TestCase):
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
            network.hostname(trust_name_from_ips=False).lower(),
            hostname.lower())

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

        if not reverse_hostnames:
            self.skipTest(
                "Could not retrieve any DNS names for this host")

        if len(reverse_hostnames) > 1:
            self.skipTest(
                "This host's addresses resolve to more than one hostname")

    def test_addresses(self):
        self.assertGreaterEqual(len(network.addresses(private_only=False)), 1)

    def test_addresses_type(self):
        self.assertIsInstance(network.addresses(), tuple)

    def test_interfaces(self):
        names = list(network.interfaces())

        # We assume all hosts have at least the loopback interface.
        self.assertGreaterEqual(len(names), 1)

        for name in names:
            for psutil_name, nics in psutil.net_if_addrs().items():
                if name != psutil_name:
                    continue
                for nic in nics:
                    if (nic.family in (socket.AF_INET, socket.AF_INET6)
                        and nic.address):
                        break
                else:
                    self.fail("Failed to locate nic entry for %s" % name)
            break
        else:
            self.fail("Failed to locate nic")


class TestCPU(TestCase):
    def test_count(self):
        self.assertEqual(psutil.cpu_count(), cpu.total_cpus())

    def test_load(self):
        # Make several attempts to test cpu.load().  Because this
        # depends on the system load at the time we make a few
        # attempts to get the results we expect.
        for _ in range(15):
            load = psutil.cpu_percent(.25) / cpu.total_cpus()
            try:
                self.assertApproximates(cpu.load(.25), load, .75)
            except AssertionError:
                continue
            else:
                break
        else:
            self.fail("Failed get a non-failing result after several attempts")

    def test_user_time(self):
        self.assertEqual(psutil.cpu_times().user <= cpu.user_time(), True)

    def test_system_time(self):
        self.assertEqual(psutil.cpu_times().system <= cpu.system_time(), True)

    def test_idle_time(self):
        self.assertEqual(psutil.cpu_times().idle <= cpu.idle_time(), True)

    def test_iowait(self):
        if LINUX:
            self.assertEqual(cpu.iowait() <= psutil.cpu_times().iowait, True)
        else:
            self.assertEqual(cpu.iowait(), None)


class TestMemory(TestCase):
    def test_totalram(self):
        self.assertEqual(memory.total_ram(),
                         int(convert.bytetomb(psutil.virtual_memory().total)))

    def test_ramused(self):
        v1 = memory.total_ram() - memory.free_ram()
        v2 = memory.used_ram()
        self.assertApproximates(v1, v2, 5)

    def test_ramfree(self):
        v1 = convert.bytetomb(psutil.virtual_memory().available)
        v2 = memory.free_ram()
        self.assertApproximates(v1, v2, 5)

    def test_process_memory(self):
        process = psutil.Process()
        v1 = convert.bytetomb(process.memory_info().rss)
        v2 = memory.process_memory()
        self.assertApproximates(v1, v2, 5)

    def test_total_consumption(self):
        # Spawn a child process so we have something
        # to iterate over
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"])
        parent = psutil.Process()
        start_total = total = parent.memory_info().rss

        for child_process in parent.children(recursive=True):
            try:
                total += child_process.memory_info().rss
            except psutil.NoSuchProcess:
                pass

        # These should not be equal if we've iterated
        # over any children at all.
        self.assertNotEqual(start_total, total)

        v1 = convert.bytetomb(total)
        v2 = memory.total_consumption()
        child.kill()
        self.assertApproximates(v1, v2, 5)


class TestUser(TestCase):
    @skipIf(pwd is NotImplemented, "pwd is NotImplemented")
    def test_username_pwd(self):
        self.assertEqual(pwd.getpwuid(os.getuid())[0], user.username())

    @skipIf(win32api is NotImplemented, "win32api is NotImplemented")
    def test_username_win32api(self):
        self.assertEqual(win32api.GetUserName(), user.username())

    @skipIf(getpass is NotImplemented, "getpass is NotImplemented")
    def test_username_getpass(self):
        self.assertEqual(getpass.getuser(), user.username())

    @skipIf(getuid is NotImplemented, "getuid is NotImplemented")
    def test_administrator_getuid(self):
        self.assertEqual(getuid() == 0, user.is_administrator())

    @skipIf(win32api is NotImplemented, "win32api is NotImplemented")
    def test_administrator_win32api(self):
        self.assertEqual(shell.IsUserAnAdmin(), user.is_administrator())

    @skipIf(win32api is NotImplemented and not WINDOWS,
            "win32api is NotImplemented and not WINDOWS")
    def test_administrator_no_win32api_and_windows(self):
        self.assertEqual(ctypes.windll.shell32.IsUserAnAdmin() != 0,
                         user.is_administrator())


class TestDisks(TestCase):
    def setUp(self):
        super(TestDisks, self).setUp()
        self.disks = [
            Mock(mountpoint="/mnt/1"),
            Mock(mountpoint="/mnt/2")
        ]
        self.disk_usage_data = {
            "/mnt/1": Mock(free=1, size=2),
            "/mnt/2": Mock(free=1, size=2)
        }

        self.namedtuple_type = []
        for disk in self.disks:
            info = self.disk_usage(disk.mountpoint)
            self.namedtuple_type.append(
                disks.DiskInfo(
                    mountpoint=disk.mountpoint,
                    free=info.free,
                    size=info.total
                )
            )

    def disk_usage(self, mountpoint):
        return self.disk_usage_data[mountpoint]

    def test_return_type(self):
        self.assertIsInstance(disks.disks(), list)

    def test_return_type_namedtuple(self):
        result = []
        for disk in self.disks:
            info = self.disk_usage(disk.mountpoint)
            result.append(
                disks.DiskInfo(
                    mountpoint=disk.mountpoint,
                    free=info.free,
                    size=info.total
                )
            )

        with nested(
            patch.object(psutil, "disk_partitions", return_value=self.disks),
            patch.object(psutil, "disk_usage", self.disk_usage)
        ):
            self.assertEqual(disks.disks(), result)

    def test_return_type_dict(self):
        result = []
        for disk in self.disks:
            usage = self.disk_usage(disk.mountpoint)
            result.append(
                {
                    "mountpoint": disk.mountpoint,
                    "free": usage.free,
                    "size": usage.total
                }
            )

        with nested(
            patch.object(psutil, "disk_partitions", return_value=self.disks),
            patch.object(psutil, "disk_usage", self.disk_usage)
        ):
            self.assertEqual(disks.disks(as_dict=True), result)


class TestSoftware(TestCase):
    def test_version_not_found_parent(self):
        self.assertIsInstance(software.VersionNotFound(), Exception)

    #
    # Test software.get_software_version_data
    #

    @inlineCallbacks
    def test_get_software_version_data_ok(self):
        data = json.dumps({"version": "1", "software": "foo"})
        with APITestServer("/software/foo/versions/1", code=OK, response=data):
            result = yield software.get_software_version_data("foo", "1")
            self.assertEqual(json.loads(data), result)

    @inlineCallbacks
    def test_get_software_version_data_internal_server_error_retries(self):
        data = json.dumps({"version": "1", "software": "foo"})
        with APITestServer(
                "/software/foo/versions/1",
                code=INTERNAL_SERVER_ERROR, response=data) as server:
            reactor.callLater(.5, setattr, server.resource, "code", OK)
            result = yield software.get_software_version_data("foo", "1")
            self.assertEqual(json.loads(data), result)

    @inlineCallbacks
    def test_get_software_version_data_not_found_error(self):
        with nested(
            APITestServer(
                "/software/foo/versions/1",
                code=NOT_FOUND, response=json.dumps({})),
            self.assertRaises(software.VersionNotFound)
        ):
            yield software.get_software_version_data("foo", "1")

    @inlineCallbacks
    def test_get_software_version_data_unknown_http_code(self):
        with nested(
            APITestServer(
                "/software/foo/versions/1",
                code=499, response=json.dumps({})),
            self.assertRaises(Exception)
        ):
            yield software.get_software_version_data("foo", "1")

    @inlineCallbacks
    def test_get_software_version_data_unhandled_error_calling_get_direct(self):
        class GetDirect(object):
            def __init__(self):
                self.hits = 0

            def __call__(self, *args, **kwargs):
                self.hits += 1
                if self.hits < 2:
                    raise Exception("Fail!")
                return get_direct(*args, **kwargs)

        data = json.dumps({"version": "1", "software": "foo"})
        with nested(
            APITestServer("/software/foo/versions/1", code=OK, response=data),
            patch.object(software, "get_direct", GetDirect()),
            patch.object(software, "logger")
        ) as (_, _, logger):
            yield software.get_software_version_data("foo", "1")

        # Test the logger output itself since it's the only distinct way of
        # finding out what part of the code base ran.
        self.assertIn(
            "Failed to get data about software", logger.mock_calls[0][1][0])
        self.assertIn(
            "Will retry in %s seconds", logger.mock_calls[0][1][0])

    #
    # Test software.get_discovery_code
    #

    @inlineCallbacks
    def test_get_discovery_code_ok(self):
        data = "Hello world"
        with APITestServer(
                "/software/foo/versions/1/discovery_code",
                code=OK, response=data):
            result = yield software.get_discovery_code("foo", "1")
            self.assertEqual(result, data)

    @inlineCallbacks
    def test_get_discovery_code_internal_server_error_retries(self):
        data = "Hello world"
        with APITestServer(
                "/software/foo/versions/1/discovery_code",
                code=INTERNAL_SERVER_ERROR, response=data) as server:
            reactor.callLater(.5, setattr, server.resource, "code", OK)
            result = yield software.get_discovery_code("foo", "1")
            self.assertEqual(result, data)

    @inlineCallbacks
    def test_get_discovery_code_not_found_error(self):
        with nested(
            APITestServer(
                "/software/foo/versions/1/discovery_code",
                code=NOT_FOUND, response=""),
            self.assertRaises(software.VersionNotFound)
        ):
            yield software.get_discovery_code("foo", "1")

    @inlineCallbacks
    def test_get_discovery_code_unknown_http_code(self):
        with nested(
            APITestServer(
                "/software/foo/versions/1/discovery_code",
                code=499, response=""),
            self.assertRaises(Exception)
        ):
            yield software.get_discovery_code("foo", "1")

    @inlineCallbacks
    def test_get_discovery_code_unhandled_error_calling_get_direct(self):
        class GetDirect(object):
            def __init__(self):
                self.hits = 0

            def __call__(self, *args, **kwargs):
                self.hits += 1
                if self.hits < 2:
                    raise Exception("Fail!")
                return get_direct(*args, **kwargs)

        data = json.dumps({"version": "1", "software": "foo"})
        with nested(
            APITestServer(
                "/software/foo/versions/1/discovery_code",
                code=OK, response=data),
            patch.object(software, "get_direct", GetDirect()),
            patch.object(software, "logger")
        ) as (_, _, logger):
            yield software.get_discovery_code("foo", "1")

        # Test the logger output itself since it's the only distinct way of
        # finding out what part of the code base ran.
        self.assertIn(
            "Failed to get discovery code for software",
            logger.mock_calls[0][1][0])
        self.assertIn(
            "Will retry in %s seconds", logger.mock_calls[0][1][0])

    #
    # Test software.check_software_availability
    #

    @inlineCallbacks
    def test_check_software_availability_creates_module_if_missing(self):
        with nested(
            patch.object(software, "get_software_version_data",
                         return_value=succeed(
                             {"discovery_function_name": "get_foo"})),
            patch.object(software, "get_discovery_code",
                         return_value=succeed("get_foo = lambda: 'foo 1'")),

        ):
            yield software.check_software_availability("foobar", "1")

        self.assertIn("pyfarm.agent.sysinfo.software.foobar_1", sys.modules)

    @inlineCallbacks
    def test_check_software_availability_does_not_recreate_module(self):
        module = imp.new_module("foo")
        sys.modules["pyfarm.agent.sysinfo.software.foobar_2"] = module
        with nested(
            patch.object(software, "get_software_version_data",
                         return_value=succeed(
                             {"discovery_function_name": "get_foo"})),
            patch.object(software, "get_discovery_code",
                         return_value=succeed("get_foo = lambda: 'foo 2'")),

        ):
            yield software.check_software_availability("foobar", "2")

        self.assertIs(
            sys.modules["pyfarm.agent.sysinfo.software.foobar_2"], module)

    @inlineCallbacks
    def test_check_software_availability_runs_discovery_code(self):
        with nested(
            patch.object(software, "get_software_version_data",
                         return_value=succeed(
                             {"discovery_function_name": "get_foo"})),
            patch.object(software, "get_discovery_code",
                         return_value=succeed("get_foo = lambda: 'foo 3'")),

        ):
            result = yield software.check_software_availability("foobar", "3")

        self.assertEqual(result, "foo 3")


class TestGraphicsCards(TestCase):
    @skipIf(not WINDOWS, "Not Windows")
    def test_windows_wmi_exists(self):
        self.assertIsNot(graphics.WMI, NotImplemented)

    def test_windows_wmi(self):
        class FakeGPU(object):
            def __init__(self, name):
                self.name = name

        class FakeWMI(object):
            class Win32_VideoController(object):
                @staticmethod
                def query():
                    return [FakeGPU("foo"), FakeGPU("bar")]

        with nested(
            patch.object(graphics, "WINDOWS", True),
            patch.object(graphics, "MAC", False),
            patch.object(graphics, "LINUX", False),
            patch.object(graphics, "WMI", FakeWMI),
        ):
            self.assertEqual(graphics.graphics_cards(), ["foo", "bar"])

    def test_mac_system_profiler_parsing(self):
        sample_data = dedent("""
        <?xml version="1.0" encoding="UTF-8"?>
        <plist version="1.0">
        <array>
            <dict>
                <array>
                    <dict>
                        <key>sppci_model</key>
                        <string>Intel Iris Pro</string>
                    </dict>
                    <dict>
                        <key>sppci_model</key>
                        <string>NVIDIA GeForce GT 750M</string>
                    </dict>
                </array>
                <key>_timeStamp</key>
                <date>2015-09-24T01:26:52Z</date>
                <key>_versionInfo</key>
            </dict>
        </array>
        </plist>
        """).strip()

        class FakePopenOutput(object):
            stdout = StringIO()
            stdout.write(sample_data)
            stdout.seek(0)

        with nested(
            patch.object(graphics, "WINDOWS", False),
            patch.object(graphics, "MAC", True),
            patch.object(graphics, "LINUX", False),
            patch.object(graphics, "Popen", return_value=FakePopenOutput)
        ):
            self.assertEqual(
                graphics.graphics_cards(),
                ["Intel Iris Pro", "NVIDIA GeForce GT 750M"])

    def test_mac_error_while_calling_sytem_profiler(self):
        def raise_(*args, **kwargs):
            raise OSError

        with nested(
            patch.object(graphics, "WINDOWS", False),
            patch.object(graphics, "MAC", True),
            patch.object(graphics, "LINUX", False),
            patch.object(graphics, "Popen", raise_),
            self.assertRaises(graphics.GPULookupError)
        ):
            graphics.graphics_cards()

    def test_linux_lspci_parsing(self):
        sample_data = dedent("""
        00:00.0 Host bridge:
        00:02.0 VGA compatible controller: BestGPU1
        00:02.1 VGA compatible controller: BestGPU2
        00:14.0 USB controller:
        00:16.0 Communication controller:
        00:1a.0 USB controller:
        00:1c.0 PCI bridge:
        00:1d.0 USB controller:
        00:1f.0 ISA bridge:
        00:1f.2 SATA controller:
        00:1f.3 SMBus:
        02:00.0 Ethernet controller:
        """).strip()

        class FakePopenOutput(object):
            stdout = StringIO()
            stdout.write(sample_data)
            stdout.seek(0)

        with nested(
            patch.object(graphics, "WINDOWS", False),
            patch.object(graphics, "MAC", False),
            patch.object(graphics, "LINUX", True),
            patch.object(graphics, "Popen", return_value=FakePopenOutput)
        ):
            self.assertEqual(
                graphics.graphics_cards(), ["BestGPU1", "BestGPU2"])

    def test_linux_error_while_calling_popen(self):
        def raise_(*args, **kwargs):
            raise OSError

        with nested(
            patch.object(graphics, "WINDOWS", False),
            patch.object(graphics, "MAC", True),
            patch.object(graphics, "LINUX", False),
            patch.object(graphics, "Popen", raise_),
            self.assertRaises(graphics.GPULookupError)
        ):
            graphics.graphics_cards()

    def testUnknownPlatform(self):
        with nested(
            patch.object(graphics, "WINDOWS", False),
            patch.object(graphics, "MAC", False),
            patch.object(graphics, "LINUX", False)
        ):
            with self.assertRaises(graphics.GPULookupError):
                graphics.graphics_cards()

    def test_gpu_lookup_error_parent_exception(self):
        error = graphics.GPULookupError("foo")
        self.assertIsInstance(error, Exception)

    def test_gpu_lookup_error_repr(self):
        error = graphics.GPULookupError("foo")
        self.assertEqual(repr(error.value), str(error))

    def test_gpu_lookup_error_value(self):
        error = graphics.GPULookupError("foo")
        self.assertEqual(error.value, "foo")
