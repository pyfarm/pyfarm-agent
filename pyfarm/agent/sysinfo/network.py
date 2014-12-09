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

:const IP_PRIVATE:
    set of private class A, B, and C network ranges

    .. seealso::
        :rfc:`1918`

:const IP_NONNETWORK:
    set of non-network address ranges including all of the
    above constants except the :const:`IP_PRIVATE`
"""

import socket

import netifaces
from netaddr import IPSet, IPNetwork, IPAddress

from pyfarm.agent.logger import getLogger

logger = getLogger("agent.dns")

IP_PRIVATE = IPSet([
    IPNetwork("10.0.0.0/8"),
    IPNetwork("172.16.0.0/12"),
    IPNetwork("192.168.0.0/16")])
IP_NONNETWORK = IPSet([
    IPNetwork("0.0.0.0/8"),         # special use
    IPNetwork("169.254.0.0/16"),    # link local
    IPNetwork("127.0.0.0/8"),       # loopback
    IPNetwork("224.0.0.0/4"),       # multicast
    IPNetwork("255.255.255.255")])  # broadcast


def mac_addresses(long_addresses=False, as_integers=False):
    """
    Returns a tuple of all mac addresses on the system.

    :param bool long_addresses:
        Some adapters will specify a mac address which is longer
        than the standard value of six pairs.  Setting this value
        to True will allow these to be displayed.

    :param bool as_integers:
        When ``True`` convert all mac addresses to integers.
    """
    results = set()
    for ifaces in map(netifaces.ifaddresses, netifaces.interfaces()):
        for entry in ifaces.get(netifaces.AF_LINK, []):
            mac = entry.get("addr", "")

            if all([mac, not long_addresses, len(mac) == 17]) \
               or all([long_addresses, mac, len(mac) >= 17]):
                mac_as_int = int("0x" + mac.replace(":", ""), 0)
                if mac_as_int != 0:
                    if as_integers:
                        results.add(mac_as_int)
                    else:
                        results.add(mac)

    return tuple(results)


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
    ip_addresses = addresses()
    for address in ip_addresses:
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
            "DNS failed to resolve %s to hostnames", ip_addresses)

    local_hostname = socket.gethostname()
    local_fqdn_query = socket.getfqdn()

    if local_fqdn_query in reverse_hostnames:  # pragma: no cover
        name = local_fqdn_query

    elif local_hostname in reverse_hostnames:  # pragma: no cover
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


def addresses(private_only=True):
    """Returns a tuple of all non-local ip addresses."""
    results = set()

    for interface in netifaces.interfaces():
        addrinfo = netifaces.ifaddresses(interface)
        for address in addrinfo.get(socket.AF_INET, []):
            addr = address.get("addr")

            if addr is not None:
                # Make sure that what we're getting out of
                # netifaces is something we can use.
                try:
                    ip = IPAddress(addr)
                except ValueError:  # pragma: no cover
                    logger.error(
                        "Could not convert %s to a valid IP object" % addr)
                else:
                    if ip in IP_PRIVATE or not private_only:
                        results.add(addr)

    if not addresses:  # pragma: no cover
        logger.error("No addresses could be found")

    return tuple(results)


def interfaces():
    """Returns the names of all valid network interface names"""
    results = set()

    for name in netifaces.interfaces():
        # only add network interfaces which have IPv4
        addresses = netifaces.ifaddresses(name)

        if socket.AF_INET not in addresses:  # pragma: no cover
            continue

        if any(addr.get("addr") for addr in addresses[socket.AF_INET]):
            results.add(name)

    if not results:  # pragma: no cover
        logger.warning("Failed to find any interfaces")

    return tuple(results)

