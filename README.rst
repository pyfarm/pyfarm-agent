.. Copyright 2013 Oliver Palmer
..
.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

PyFarm Agent
============

.. image:: https://travis-ci.org/pyfarm/pyfarm-agent.png?branch=master
    :target: https://travis-ci.org/pyfarm/pyfarm-agent
    :alt: build status (agent) (posix)

.. image:: https://ci.appveyor.com/api/projects/status/a0fwqwlqrcs57sfn/branch/master?svg=true
    :target: https://ci.appveyor.com/project/opalmer/pyfarm-agent/history
    :alt: build status (agent) (windows)

.. image:: https://coveralls.io/repos/pyfarm/pyfarm-agent/badge.png?branch=master
    :target: https://coveralls.io/r/pyfarm/pyfarm-agent?branch=master
    :alt: coverage


Core module containing code to run PyFarm's agent. This will allow a remote
host to:

    * Inform the master about itself
    * Request, receive and execute work via job types
    * Track and control individual processes
    * Measure and limit system resource usage


Python Version Support
----------------------

This library supports Python 2.6 and Python 2.7 only for the moment.  Coding
practices have been geared towards supporting Python 3 when the underlying
library, Twisted, is ported to Python 3.

Documentation
-------------

The documentation for this this library is hosted on
`Read The Docs <https://pyfarm.readthedocs.org/projects/pyfarm-agent/en/latest/>`_.
It's generated directly from this library using sphinx (setup may vary depending
on platform)::

    virtualenv env
    . env/bin/activate
    pip install sphinx nose
    pip install -e . --egg
    make -C docs html

Testing
-------
All commits and pull requests are tested on Linux, Mac OS X and Windows. Tests
for Linux and Mac OS X are run using `Travis <https://travis-ci.org/pyfarm/pyfarm-agent>`_
while Windows testing is performed on
`Appveyor <https://ci.appveyor.com/project/opalmer/pyfarm-agent/history>`_.
Code coverage analysis is also provided by
`Coveralls <https://coveralls.io/github/pyfarm/pyfarm-agent>`_ for Linux and
Mac OS X.

The tests can can also run locally using Twisted's ``trial``.  Some tests
will require access to external services such as httpbin.org, NTP,
DNS and other network services.

Below are some examples for executing the tests locally.  These are fairly
minimal however and may not work in all cases.  For more complete examples,
checkout the configuration files used to run the tests on Travis and Appveyor:

    * `.travis.yml <https://github.com/pyfarm/pyfarm-agent/blob/master/.travis.yml>`_
    * `appveyor.yml <https://github.com/pyfarm/pyfarm-agent/blob/master/appveyor.yml>`_

**Linux and Mac OS X**::

    virtualenv env
    . env/bin/activate
    pip install -e . --egg
    trial tests

**Windows**::

    virtualenv env
    env\Scripts\activate
    %VIRTUALE_ENV%\Scripts\pip.exe install wheel
    %VIRTUALE_ENV%\Scripts\pip.exe install -e . --egg
    %VIRTUALE_ENV%\Scripts\python.exe %VIRTUALE_ENV%\Scripts\trial.py tests

.. note::

    On Windows, if the tests fail to locate one of the agent's modules be sure
    you don't have another package for PyFarm installed in your system
    site-packages directory.

    You may also have to run trial a little differently.  See
    ``appveyor.yml`` for an example.
