Local mode
==========

The QCG PilotJob Manager supports *local* mode that is suitable for locally testing executiong scenarious. In normal
execution mode, where QCG PilotJob Manager is executed in scheduling system allocation, all jobs are launched with the
usage of scheduling system. In the *local* mode, the user itself can define the size of available resources and execute
it's scenario on such defined resources without the having access to scheduling system. It's worth remembering that QCG
PilotJob Manager doesn't verify the physically available resources, also the executed jobs are not launched with any
core/processor affinity. Thus the performance of jobs might not be optimal.

The choice between *allocation* (in scheduling system allocation) or *local* mode is made automatically by the QCG
PilotJob Manager during the start. If scheduling system environment will be detected, the *allocation* mode will be
chosen. In other case, the local mode will be active, and if resources are not defined by the user, the default number
of available cores in the system will be taken.

The command line arguments related to the *local* mode are presented below:

- ``--nodes NODES`` - the available resources definition; the ``NODES`` parameter should have format::

    `[NODE_NAME]:CORES[,[NODE_NAME]:CORES]...`

- ``--envschema ENVSCHEMA`` - job execution environment; for each job the QCG PilotJob Manager can create environment
similar to the Slurm execution environment

Examples of resources definition
--------------------------------

- ``--nodes 4`` - single node with 4 available cores
- ``--nodes n1:2`` - single named node with 2 available cores
- ``--nodes 4,2,2`` - three unnnamed nodes with 8 total cores
- ``--nodes n1:4, n2:4, n3:4`` - three named nodes with 12 total cores

