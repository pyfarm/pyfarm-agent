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

from __future__ import with_statement

import sys
assert sys.version_info[0:2] >= (2, 6), "Python 2.6 or higher is required"

import os
from os import walk
from os.path import isfile, join
from setuptools import setup

install_requires = [
    "pyfarm.core>=0.9.1",
    # "PyOpenSSL", "service_identity",  # required for full SSL support
    "netaddr", "twisted", "ntplib", "requests!=2.4.0", "treq",
    "voluptuous", "jinja2", "psutil>=2.1.0",
    "netifaces>=0.10.2"]

if "READTHEDOCS" in os.environ:
    install_requires += ["sphinxcontrib-httpdomain", "sphinx"]

# Windows is a little special because we have to have pywin32
# installed.  pyfarm.core uses it and certain components of
# other libraries use it too, such as twisted, so we check for
# it here.  Unfortunately, we can't use PyPi for this.
if sys.platform.startswith("win"):
    try:
        import win32api
    except ImportError:
        raise ImportError(
            "On Windows, you must manually install pywin32 before running "
            "pyfarm.core's setup.py.  This is required because there's not "
            "a package that we can pull down and reliably install from the "
            "Python package repository.  Please visit "
            "http://sourceforge.net/projects/pywin32/files/pywin32/ to "
            "download and install this package.")
    install_requires += ["wmi"]

if sys.version_info[0:2] == (2, 6):
    install_requires += ["importlib", "ordereddict", "argparse"]

if isfile("README.rst"):
    with open("README.rst", "r") as readme:
        long_description = readme.read()
else:
    long_description = ""


def get_package_data(parent, roots):
    output = []
    for top in roots:
        if top.startswith("/"):
            raise ValueError("get_package_data was given an absolute path or "
                             "the filesystem root to traverse, refusing.")
        for root, dirs, files in walk(top):
            for filename in files:
                output.append(join(root, filename).split(parent)[-1][1:])

    return output

agent_root = join("pyfarm", "agent")
agent_package_data_roots = (
    join(agent_root, "etc"),
    join(agent_root, "http", "static"),
    join(agent_root, "http", "templates"))

jobtype_root = join("pyfarm", "jobtypes")
jobtype_root_package_data_roots = (
    join(jobtype_root, "etc"), )

setup(
    name="pyfarm.agent",
    version="0.8.2",
    packages=[
        "pyfarm",
        "pyfarm.agent",
        "pyfarm.agent.entrypoints",
        "pyfarm.agent.http",
        "pyfarm.agent.http.api",
        "pyfarm.agent.http.core",
        "pyfarm.agent.logger",
        "pyfarm.agent.sysinfo",
        "pyfarm.jobtypes",
        "pyfarm.jobtypes.core"],
    data_files=[
        ("etc/pyfarm", [
            "pyfarm/jobtypes/etc/jobtypes.yml",
            "pyfarm/agent/etc/agent.yml"])],
    package_data={
        "pyfarm.agent": get_package_data(
            agent_root, agent_package_data_roots),
        "pyfarm.jobtypes": get_package_data(
            jobtype_root, jobtype_root_package_data_roots)},
    namespace_packages=["pyfarm"],
    entry_points={
        "console_scripts": [
            "pyfarm-agent = pyfarm.agent.entrypoints.main:agent",
            "pyfarm-supervisor = "
            "   pyfarm.agent.entrypoints.supervisor:supervisor",
            "pyfarm-dev-fakerender = "
            "   pyfarm.agent.entrypoints.development:fake_render",
            "pyfarm-dev-fakework = "
            "   pyfarm.agent.entrypoints.development:fake_work"]},
    include_package_data=True,
    install_requires=install_requires,
    url="https://github.com/pyfarm/pyfarm-agent",
    license="Apache v2.0",
    author="Oliver Palmer",
    author_email="development@pyfarm.net",
    description="Core module containing code to run PyFarm's agent.",
    long_description=long_description,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: No Input/Output (Daemon)",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing"])

if sys.platform.startswith("win"):
    print("WARNING:  Please be sure you've install the OpenSSL Library as "
          "some modules may break on Windows without it.")
