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
assert sys.version_info[0] < 3, "Python 3.x is not yet supported"
assert sys.version_info[0:2] >= (2, 6), "Python 2.6 or higher is required"

from os import walk
from os.path import isfile, join
from setuptools import setup

install_requires = [
    "pyfarm.core", "pyfarm.jobtypes", "psutil",
    "netaddr", "twisted", "PyOpenSSL",
    "ntplib", "requests", "txtemplate", "voluptuous"]

if sys.version_info[0] == 2:
    install_requires += ["netifaces"]

# if sys.version_info[0] >= 3:
#     install_requires += ["netifaces-py3"]

if sys.version_info[0:2] == (2, 6):
    install_requires += ["simplejson", "importlib", "ordereddict", "argparse"]

if isfile("README.rst"):
    with open("README.rst", "r") as readme:
        long_description = readme.read()
else:
    long_description = ""


# source: http://stackoverflow.com/a/7525163
# When pip installs anything from packages, py_modules, or ext_modules that
# includes a twistd plugin (which are installed to twisted/plugins/),
# setuptools/distribute writes a Package.egg-info/top_level.txt that includes
# "twisted".  If you later uninstall Package with `pip uninstall Package`,
# pip <1.2 removes all of twisted/ instead of just Package's twistd plugins.
# See https://github.com/pypa/pip/issues/355 (now fixed)
#
# To work around this problem, we monkeypatch
# setuptools.command.egg_info.write_toplevel_names to not write the line
# "twisted".  This fixes the behavior of `pip uninstall Package`.  Note that
# even with this workaround, `pip uninstall Package` still correctly uninstalls
# Package's twistd plugins from twisted/plugins/, since pip also uses
# Package.egg-info/installed-files.txt to determine what to uninstall,
# and the paths to the plugin files are indeed listed in installed-files.txt.
try:
    from setuptools.command import egg_info
    egg_info.write_toplevel_names
except (ImportError, AttributeError):
    pass
else:
    def _top_level_package(name):
        return name.split('.', 1)[0]

    def _hacked_write_toplevel_names(cmd, basename, filename):
        pkgs = dict.fromkeys(
            [_top_level_package(k)
                for k in cmd.distribution.iter_distribution_names()
                if _top_level_package(k) != "twisted"
            ]
        )
        cmd.write_file("top-level names", filename, '\n'.join(pkgs) + '\n')

    egg_info.write_toplevel_names = _hacked_write_toplevel_names


def get_package_data():
    master_root = join("pyfarm", "agent")
    packge_data_roots = (
        join("pyfarm", "agent", "static"),
        join("pyfarm", "agent", "templates"))

    output = [join("twisted", "plugins")]
    for top in packge_data_roots:
        for root, dirs, files in walk(top):
            for filename in files:
                output.append(join(root, filename).split(master_root)[-1][1:])

    return output

setup(
    name="pyfarm.agent",
    version="0.7.0-dev0",
    packages=["twisted.plugins",
              "pyfarm",
              "pyfarm.agent",
              "pyfarm.agent.http",
              "pyfarm.agent.process",
              "pyfarm.agent.utility"],
    package_data={
        "pyfarm.agent": get_package_data()},
    namespace_packages=["pyfarm"],
    entry_points={
        "console_scripts": [
            "pyfarm-agent = pyfarm.agent.entrypoints:agent"]},
    include_package_data=True,
    install_requires=install_requires,
    url="https://github.com/pyfarm/pyfarm-core",
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


# Make Twisted regenerate the dropin.cache, if possible.  This is necessary
# because in a site-wide install, dropin.cache cannot be rewritten by
# normal users.
try:
    from twisted.plugin import IPlugin, getPlugins
except ImportError:
    pass
else:
    list(getPlugins(IPlugin))
