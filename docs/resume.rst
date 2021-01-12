Resuming prematurely interrupted computations
=============================================

General
-------

The QCG-PilotJob Manager service implements mechanism for resuming prematurely interrupted computations. All incoming
job submission requests, as well as the finished job iterations are recorded to allow resuming job execution. The
current state is placed in files ``track.*`` in auxiliary directory (the ``.qcgpjm-service-*`` in the working
directory). It is worth to mention that started, but not finished job iterations will be started again, so if they
don't implement automatic computation checkpointing, they will re-start from begin.

Invocation
----------

To resume the QCG-PilotJob Manager with previous jobs, the ``resume`` command line option must be used with path either
direct to the auxiliary QCG-PilotJob Manager directory or to the working directory where auxiliary directory is placed (
in case where there are many auxiliary directories in the working directory, the last modified one will be automatically
selected).

.. note::
    Currently during the resume operation, non of previously used command line option will be re-used. So if for example
    the working directory has been specified in original QCG PilotJob Manager start, the same working directory should
    be used during resuming.

Example invocation:

.. code:: bash

    qcg-pm-service --wd prev_work_dir --resume prev_work_dir/.qcgpjm-service-LAPTOP-CNT0BD0F.5091

Operation
---------

After resume, the QCG-PilotJob Manager will re-use the pointed auxiliary directory, so all log files, current tracking
status and job reports will be appended to the previous files. Thus there is no problem to resume, already resumed
computations.

Issues
------

Currently the resume mechanism is not supported in resource partitioning mode.

