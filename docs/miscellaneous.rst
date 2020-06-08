Miscellaneous
=============

Log files
---------

The QCG PilotJob Manager creates a sub directory `.qcgpjm-service-` in working directory where the following files are
stored:

- ``service.log`` - logs of QCG PilotJob Manager, very useful in case of problems
- ``jobs.report`` - the file containing information about all finished jobs, by default written in text format, but
there is an option for JSON format which will be easier to parse
- ``final_status`` - created at the finish of QCG PilotJob Manager with general statistics about platform, available
resources and jobs in registry (not removed) that finished, failed etc.

The verbosity of log file can be controlled by the ``--log`` parameter where ``debug`` value is the most verbose mode,
and ``critical`` the most silent mode. We recommend to not set the ``debug`` for large HTC workflows, as it additionally
loads the file system.

Reserving a core for QCG PJM
----------------------------

We recommend to use ``--system-core`` parameter for workflows that contains many small jobs (HTC) or bigger allocations
(>256 cores). This will reserve a single core in allocation (on the first node of the allocation) for QCG PilogJob
Manager service.
