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

import sys
assert sys.version_info[0:2] >= (2, 5), "Python 2.5 or higher is required"

from textwrap import dedent
from setuptools import find_packages
from distutils.core import setup

PACKAGE = "pyfarm.core"
VERSION = (1, 0, 0, "alpha0")
NAMESPACE = PACKAGE.split(".")[0]
prefixpkg = lambda name: "%s.%s" % (NAMESPACE, name)

install_requires = ["statsd"]

if sys.version_info[0:2] < (2, 7):
    install_requires.append("simplejson")

setup(
    name=PACKAGE,
    version=".".join(map(str, VERSION)),
    packages=map(prefixpkg, find_packages(NAMESPACE)),
    namespace_packages=[NAMESPACE],
    install_requires=install_requires,
    url="https://github.com/pyfarm/pyfarm-core",
    license="Apache v2.0",
    author="Oliver Palmer",
    author_email="development@pyfarm.net",
    description=dedent("""This sub-library contains core modules, classes,
    and data types which are used by other parts of PyFarm."""),
    long_description=open("README.rst", "r").read(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2 :: Only",  # (for now)
        "Programming Language :: Python :: 2.5",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Distributed Computing"])