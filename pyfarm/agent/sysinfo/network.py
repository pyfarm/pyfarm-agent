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
Network
-------

Returns information about the network including ip address, dns, data
sent/received, and some error information.

:const IP_SPECIAL_USE:
    network address range referring to the current network
    for use as a source

    .. seealso::
        :rfc:`5735`

:const IP_LINK_LOCAL:
    link local address range

    .. seealso::
        :rfc:`5735`

:const IP_LOOPBACK:
    loopback address range

    .. seealso::
        :rfc:`5735`

:const IP_MULTICAST:
    multicast address range

    .. seealso::
        :rfc:`5771`

:const IP_BROADCAST:
    broadcast address range

    .. seealso::
        :rfc:`919`

:const IP_PRIVATE:
    set of private class A, B, and C network ranges

    .. seealso::
        :rfc:`1918`

:const IP_NONNETWORK:
    set of non-network address ranges including all of the
    above constants except the :const:`IP_PRIVATE`
"""

import socket

import netaddr
import netifaces
import psutil

try:
    import wmi
except ImportError:
    wmi = NotImplemented

from pyfarm.core.enums import MAC, WINDOWS
from pyfarm.core.logger import getLogger
from pyfarm.core.utility import convert

logger = getLogger("agent.dns")

IP_SPECIAL_USE = netaddr.IPNetwork("0.0.0.0/8")
IP_LINK_LOCAL = netaddr.IPNetwork("169.254.0.0/16")
IP_LOOPBACK = netaddr.IPNetwork("127.0.0.0/8")
IP_MULTICAST = netaddr.IPNetwork("224.0.0.0/4")
IP_BROADCAST = netaddr.IPNetwork("255.255.255.255")
IP_PRIVATE = netaddr.IPSet([
    netaddr.IPNetwork("10.0.0.0/8"),
    netaddr.IPNetwork("172.16.0.0/12"),
    netaddr.IPNetwork("192.168.0.0/16")])
IP_NONNETWORK = netaddr.IPSet([
    IP_SPECIAL_USE,
    IP_LINK_LOCAL,
    IP_LOOPBACK,
    IP_MULTICAST,
    IP_BROADCAST])


def iocounter():
    """
    Mapping to the internal network io counter class
    """
    values = psutil.net_io_counters(pernic=True)
    interface_id = interface()
    if WINDOWS:
        interface_id = interface_guid_to_nicename(interface_id)
    return values[interface_id]


def packets_sent():
    """
    Returns the total number of packets sent over the network
    interface provided by :func:`interface`
    """
    return iocounter().packets_sent


def packets_received():
    """
    Returns the total number of packets received over the network
    interface provided by :func:`interface`
    """
    return iocounter().packets_recv


def data_sent():
    """
    Amount of data sent in megabytes over the network
    interface provided by :func:`interface`
    """
    return convert.bytetomb(iocounter().bytes_sent)


def data_received():
    """
    Amount of data received in megabytes over the network
    interface provided by :func:`interface`
    """
    return convert.bytetomb(iocounter().bytes_recv)


def incoming_error_count():
    """
    Returns the number of packets which we failed
    to receive on the network interface provided by :func:`interface`
    """
    return iocounter().errin


def outgoing_error_count():
    """
    Returns the number of packets which we failed
    to receive on the network interface provided by :func:`interface`
    """
    return iocounter().errout


def hostname(trust_name_from_ips=True):
    """
    Returns the hostname which the agent should send to the master.

    :param bool trust_resolved_name:
        If True and all addresses provided by :func:`addresses` resolve
        to a single hostname then just return that name as it's the most
        likely hostname to be accessible by the rest of the network.
    """
    logger.debug("Attempting to discover the hostname")

    # For every address retrieve the hostname we can resolve it
    # to.  We'll use this set later to compare again what the system
    # is telling is the hostname should be.
    reverse_hostnames = set()
    for address in addresses():
        try:
            dns_name, aliases, dns_addresses = socket.gethostbyaddr(address)

        except socket.herror:  # pragma: no cover
            logger.warning(
                "Could not resolve %s to a hostname using DNS.", address)
        else:
            if address in dns_addresses:
                reverse_hostnames.add(dns_name)
                logger.debug(
                    "Lookup of %s resolved to %s", address, dns_name)

    # If all the addresses we know about map to a single
    # hostname then just return that instead of continuing
    # on.  We do this because the DNS system should know better
    # than the local system will what the fully qualified
    # hostname would be.  This is especially true on some
    # platforms where the local DNS implementation seems to
    # produce the wrong information when resolving the local
    # hostname.
    if len(reverse_hostnames) == 1 and trust_name_from_ips:
        return reverse_hostnames.pop()

    if not reverse_hostnames:
        logger.warning(
            "DNS failed to resolve %s to hostnames", list(addresses()))

    local_hostname = socket.gethostname()
    local_fqdn_query = socket.getfqdn()

    if local_fqdn_query in reverse_hostnames:
        name = local_fqdn_query

    elif local_hostname in reverse_hostnames:
        name = local_hostname

    else:
        # If neither local_hostname or local_fqdn_query seemed
        # to map to a known hostname from an IP address then
        # get the FQDN of the locally provided hostname as
        # a fallback.
        name = socket.getfqdn(local_hostname)

    if name.startswith("localhost"):
        logger.warning("Hostname resolved to or contains 'localhost'")

    if name.endswith(".local"):
        logger.warning(
            "Operating system '.local' to hostname.  In some cases, most "
            "often on OS X, this may cause unexpected problems reaching this "
            "host by name on the network.  Manually setting the hostname "
            "with --hostname may be advisable.")

    return name


def addresses():
    """Returns a list of all non-local ip addresses."""
    addresses = []

    for interface in interfaces():
        addrinfo = netifaces.ifaddresses(interface)
        for address in addrinfo.get(socket.AF_INET, []):
            addr = address.get("addr")

            if addr is not None:
                try:
                    ip = netaddr.IPAddress(addr)
                except ValueError:  # pragma: no cover
                    logger.error(
                        "Could not convert %s to a valid IP object" % addr)
                else:
                    if ip in IP_PRIVATE:
                        yield addr
                        addresses.append(addr)

    if not addresses:  # pragma: no cover
        logger.error("No addresses could be found")


def interfaces():
    """Returns the names of all valid network interface names"""
    results = []

    for name in netifaces.interfaces():
        # only add network interfaces which have IPv4
        addresses = netifaces.ifaddresses(name)

        if socket.AF_INET not in addresses:  # pragma: no cover
            continue

        if any(addr.get("addr") for addr in addresses[socket.AF_INET]):
            yield name
            results.append(name)

    if not results:  # pragma: no cover
        logger.warning("Failed to find any interfaces")


def interface_guid_to_nicename(interface_guid):
    """
    A Windows only function that converts a guid to the internal
    nicename.  This function should be called when working with psutil
    and netifaces at the same time because the two libraries use different
    conventions for querying network interface information.
    """
    if not WINDOWS:
        raise NotImplementedError(
            "This function is only implemented on Windows.  Other platforms "
            "do not need to call this.")

    assert wmi is not NotImplemented, "`wmi` was never imported"

    client = wmi.WMI()
    try:
        wmi_interfaces = client.Win32_NetworkAdapter(GUID=interface_guid)

    # Possibly an older system such as XP.  Now we have to try harder
    # to get the information we need.
    except wmi.x_wmi_invalid_query:
        logger.warning(
            "Performing secondary search for the nicename of network "
            "adapter %s" % interface_guid)
        adapters_with_names = set()
        netiface = netifaces.ifaddresses(interface_guid)

        # for every matching ethernet address
        for mac_address in netiface[netifaces.AF_LINK]:
            # ... find all network adapters
            for wmi_adapter in client.Win32_NetworkAdapter(
                    MacAddress=mac_address["addr"]):
                logger.debug(
                    "Found network adapter matching mac %r: %s",
                    mac_address["addr"], wmi_adapter)

                # ... that have some specific attribute(s) set
                if wmi_adapter.NetConnectionID is not None \
                        and wmi_adapter.NetConnectionStatus is not None:
                    adapters_with_names.add(wmi_adapter.NetConnectionID)
                else:
                    logger.debug(
                        "WMI adapter found does not have `NetConnectionID` "
                        "set, skipping")

        if not adapters_with_names:
            raise ValueError(
                "Failed to find any built-in network adapter names using WMI")

        # now find all the names psutil knows about and get the names in common
        known_psutil_names = set(psutil.network_io_counters(pernic=True).keys())
        wmi_interface_names = list(known_psutil_names & adapters_with_names)

        if not wmi_interface_names:
            raise ValueError(
                "Failed to find any network adapter interfaces by name using "
                "a secondary search")

        # there's not a great way to handle more than one match yet
        if len(wmi_interface_names) > 1:
            raise NotImplementedError(
                "Don't know how to handle more than one matching network "
                "adapter that's been found with the fallback search")

        return wmi_interface_names[0]

    else:
        # Didn't find anything matching interface_guid which is odd
        # because netifaces was able to find it
        if not wmi_interfaces:
            raise ValueError(
                "Failed to find any network interfaces with the "
                "GUID %s.  This may be a bug, please report "
                "it." % interface_guid)

        # For now, we don't know how to handle this because we don't have any
        # same data to make some assumptions off of yet.
        if len(wmi_interfaces) != 1:
            raise NotImplementedError(
                "Don't know how to handle multiple results for a single GUID.")

        return wmi_interfaces[0].NetConnectionID


def interface(addr=None):
    """
    Based on the result from :func:`ip` return the network interface
    in use
    """
    addr = ip() if addr is None else addr

    for interface in netifaces.interfaces():
        addresses = netifaces.ifaddresses(interface).get(socket.AF_INET, [])
        for address in addresses:
            if address.get("addr") == addr:
                # in some cases we can get non-standard names
                # off of the name, select the first name only
                return interface.split(":")[0]

    raise ValueError(  # pragma: no cover
        "Could not determine network interface for `%s`" % addr)


def ip(as_object=False):
    """
    Attempts to retrieve the ip address for use on the network.  Because
    a host could have several network adapters installed this method will:

    * find all network adapters on the system which contain network
      addressable IPv4 addresses
    * measure the bytes sent/received and the packets sent/received for
      each adapter found
    * return the adapter with the most number of packets and bytes
      sent and received
    """
    # get the amount of traffic for each network interface,
    # we use this to help determine if the most active interface
    # is the interface dns provides
    sums = []
    counters = psutil.net_io_counters(pernic=True)

    for address in addresses():
        interface_name = interface(address)

        if WINDOWS:
            try:
                interface_name = interface_guid_to_nicename(interface_name)

            except ValueError:
                pass

            # It's possible, although unlikely, that wmi will not be able
            # to map to an adapter name.  Because of this we'll try to see
            # if the match we came up with works and if not we'll just
            # check to see if the name we do have is part of the name
            # psutil has.
            for psutil_interface_name in counters:
                if interface_name == psutil_interface_name or \
                        interface_name in psutil_interface_name:
                    interface_name = psutil_interface_name
                    break
            else:
                logger.warning(
                    "Couldn't map adapter %r to a 'nicename'", interface_name)

        try:
            counter = counters[interface_name]

        except KeyError:  # pragma: no cover
            total_bytes = 0

        else:
            total_bytes = counter.bytes_sent + counter.bytes_recv

        sums.append((address, total_bytes))

    if not sums:  # pragma: no cover
        raise ValueError("no ip address found")

    # sort addresses based on how 'active' they appear
    sums.sort(key=lambda i: i[1], reverse=True)

    ip = netaddr.IPAddress(sums[0][0])

    # now that we have an address, check it against some of
    # our address groups but don't raise exceptions since that
    # should be handled/fail in higher level code
    if ip in IP_SPECIAL_USE:  # pragma: no cover
        logger.warning("ip() discovered a special use address")

    if ip in IP_LOOPBACK:  # pragma: no cover
        logger.warning("ip() discoverd a loopback address")

    if ip in IP_LINK_LOCAL:  # pragma: no cover
        logger.error("ip() discovered a link local address")

    if ip in IP_MULTICAST:  # pragma: no cover
        logger.error("ip() discovered a multicast address")

    if ip in IP_BROADCAST:  # pragma: no cover
        logger.error("ip() discovered a broadcast address")

    return str(ip) if not as_object else ip
