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

Tests are run on `Travis <https://travis-ci.org/pyfarm/pyfarm-agent>`_ and
`Appveyor <https://ci.appveyor.com/project/opalmer/pyfarm-agent/history>`_ for
every commit.  They can also be run locally too using ``trial``.  Currently,
the agent tests require:

    * Access to https://httpbin.pyfarm.net for HTTP client testing.  This is
      configurable however and could be pointed to an internal domain
      using the ``agent_unittest`` configuration variable.
    * The ``pyfarm.master`` module to run the API.  So all the setup steps
      that apply to the master will apply here as well.  This includes the
      requirement to run Redis, RabbitMQ or another backend that supports
      ``celery``.

Newer tests are being designed to be lighter weight so eventually most of the
above may no longer be required for testing.  For now however these are the
basic steps to run the tests and are based on the steps in ``.travis.yml``::

    virtualenv env
    . env/bin/activate
    pip install pyfarm.master uwsgi mock
    pyfarm-tables
    uwsgi resources/uwsgi.ini
    pip install -e . --egg
    trial tests  # in a new shell with the same virtualenv

.. note::

    On Windows, you may have to run trial a little differently.  See
    ``appveyor.yml`` for an example.
