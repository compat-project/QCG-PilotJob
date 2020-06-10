Modes of execution
==================
In the previously presented examples we submitted a single CPU applications. However QCG-PilotJob Manager is
intended for use in HPC environments, especially with *Slurm* scheduling system. The execution on a cluster
is therefore a default mode of execution of QCG-PilotJob.
In order to support users in testing their scenarios before the actual execution on a cluster,
QCG-PilotJob can be also run in a local environment.
Below we present these two modes of execution of QCG-PilotJob.

Scheduling systems
------------------

In case of execution via Slurm we submit a request to scheduling system and when requested resources are available,
the allocation is created and our application is run inside it. Of course
we might run our job's directly in scheduling system without any pilot job mechanism, but we have to remember about
some limitations of scheduling systems such as - maximum number of submitted/executing jobs in the same time, queueing
time (significant for large number of jobs), job array mechanism only for same resource requirement jobs. Generally,
scheduling systems wasn't designed for handling very large number of small jobs.

To use QCG-PilotJob Manager in HPC environment, we suggest to install QCG-PilotJob Manager via virtual environment in
directory shared among all computing nodes (most of home directories are available from computing nodes). On some
systems, we need to load a proper Python >= 3.6 module before:

.. code:: bash

    $ module load python/3.7.3

Next we can create virtual environment with QCG-PilotJob Manager:

.. code:: bash

    $ python3 -m virtualenv $HOME/qcgpj-venv
    $ source $HOME/qcgpj-venv/bin/activate
    $ pip install qcg-pilotjob

Now we can use this virtual environment in our jobs. The example job submission script for *Slurm* scheduling system
that launched application ``myapp.py`` that uses QCG-PilotJob Manager API, may look like this:

.. code:: bash

    #SBATCH --job-name=qcgpilotjob-ex
    #SBATCH --nodes=2
    #SBATCH --tasks-per-node=28
    #SBATCH --time=60

    module load python/3.7.3
    source $HOME/qcgpj-venv/bin/activate

    python myapp.py

Of course, some scheduling system might require some additional parameters like:

- ``--account`` - name of the account/grant we want to use
- ``--partition`` - the partition name where our job should be scheduled

To submit a job with QCG-PilotJob Manager in batch mode with JSON jobs description file, we have to change the last
line to:

.. code:: bash

    python -m qcg.pilotjob.service --file-path jobs.json


.. note::
    Once QCG-PilotJob is submitted via Slurm or QCG middleware, it inherits the execution environment set
    by those systems. Some environment variables, such as the location of a shared directory,
    may be useful in a user's tasks. In order to get more detailed information on this topic please see
    :ref:`Execution environments`.

Local execution
---------------

QCG-PilotJob Manager supports *local* mode that is suitable for locally testing execution scenarios. In contrast
to execution mode, where QCG-PilotJob Manager is executed in scheduling system allocation, all jobs are launched with
the usage of scheduling system. In the *local* mode, the user itself can define the size of available resources and
execute it's scenario on such defined resources without the having access to scheduling system. It's worth remembering
that QCG-PilotJob Manager doesn't verify the physically available resources, also the executed jobs are not launched
with any core/processor affinity. Thus the performance of jobs might not be optimal.

The choice between *allocation* (in scheduling system allocation) or *local* mode is made automatically by the QCG
PilotJob Manager during the start. If scheduling system environment will be detected, the *allocation* mode will be
chosen. In other case, the local mode will be active, and if resources are not defined by the user, the default number
of available cores in the system will be taken.

The command line arguments, that also might by passed as argument ``server_args`` during instantiating the LocalManager
, related to the *local* mode are presented below:

- ``--nodes NODES`` - the available resources definition; the ``NODES`` parameter should have format::

    `[NODE_NAME]:CORES[,[NODE_NAME]:CORES]...`

- ``--envschema ENVSCHEMA`` - job execution environment; for each job QCG-PilotJob Manager can create environment
  similar to the Slurm execution environment

Some examples of resources definition:

- ``--nodes 4`` - single node with 4 available cores
- ``--nodes n1:2`` - single named node with 2 available cores
- ``--nodes 4,2,2`` - three unnamed nodes with 8 total cores
- ``--nodes n1:4, n2:4, n3:4`` - three named nodes with 12 total cores

