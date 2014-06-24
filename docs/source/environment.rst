Environment Variables
=====================
PyFarm's agent has several environment variables which can be used to
change the operation at runtime.  For more information see the individual
sections below.

.. envvar:: PYFARM_JOBTYPE_ALLOW_CODE_EXECUTION_IN_MODULE_ROOT

    If ``True``, then function calls in the root of a job types's source code
    will result in an error when the work is assigned.  By default, this value
    is set to ``True``.

.. envvar:: PYFARM_JOBTYPE_SUBCLASSES_BASE_CLASS

    If ``True`` then job types which do not subclass from
    :class:`pyfarm.jobtypes.core.jobtype.JobType` will raise an exception
    when work is assigned.  By default, this value is set to ``True``.
