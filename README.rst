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

.. image:: https://badge.waffle.io/pyfarm/pyfarm-agent.png?label=ready
    :target: https://waffle.io/pyfarm/pyfarm-agent
    :align: left

.. image:: https://travis-ci.org/pyfarm/pyfarm-agent.png?branch=master
    :target: https://travis-ci.org/pyfarm/pyfarm-agent
    :align: left

.. image:: https://coveralls.io/repos/pyfarm/pyfarm-agent/badge.png?branch=master
    :target: https://coveralls.io/r/pyfarm/pyfarm-agent?branch=master
    :align: left

Core module containing code to run PyFarm's agent. This will allow a remote
host to:
    * add itself to PyFarm
    * request, receieve, and execute work via job types
    * track and control individual processes
    * measure and limit system resource usage


