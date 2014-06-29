.. _Configuration: https://pyfarm.readthedocs.org/projects/pyfarm-core/en/latest/modules/pyfarm.core.config.html#pyfarm.core.config.Configuration

Configuration Files
===================

Below are the configuration files for this subproject.  These files are
installed along side the source code when the package is installed.  These are
only the defaults however, you can always override these values in your own
environment.  See the Configuration_ object documentation for more detailed
information.

Agent
-----

The below is the current configuration file for the agent.  This
file lives at ``pyfarm/agent/etc/agent.yml`` in the source tree.

.. literalinclude:: ../../pyfarm/agent/etc/agent.yml
    :language: yaml
    :lines: 14-
    :linenos:


Job Types
---------

The below is the current configuration file for job types.  This
file lives at ``pyfarm/jobtypes/etc/jobtypes.yml`` in the source tree.

.. literalinclude:: ../../pyfarm/jobtypes/etc/jobtypes.yml
    :language: yaml
    :lines: 14-
    :linenos:

