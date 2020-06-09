Usage
=====

The QCG PilotJob Manager can be used in two different ways:

- as an service accessible with API
- as a command line utility to execute static, prepared job workflows

The second method allows to dynamically control the jobs execution.

Example API application
-----------------------

Let's write a simple program that will runs 4 instances of simple bash script.

First, we must create an instance of QCG PilotJob Manager

.. code:: python

    from qcg.pilotjob.api.manager import LocalManager

    manager = LocalManager()

This default instance, when launched outside Slurm scheduling system allocation, will use all local available CPU's.
To check what resources are available for our future jobs, we call a ``resources`` method.

.. code:: bash

    print('available resources: ', manager.resources())

In return we should give something like::

    available resources: {'total_nodes': 1, 'total_cores': 8, 'used_cores': 0, 'free_cores': 8}

where ``total_cores`` and ``free_cores`` depends on number of cores on machine where we are running this example.
So our programs will have access to all ``free_cores``, and QCG PilotJob manager will make sure that tasks do not
interfere with each other, so the maximum number of simultaneously running job's will be exact ``free_cores``.

To run jobs, we have to create a list of job descriptions and sent it to the QCG PilotJob manager.

.. code:: bash

    from qcg.pilotjob.api.job import Jobs
    jobs = Jobs().add(script='echo "job ${it} executed at `date` @ `hostname`"', stdout='job.out.${it}', iteration=4)
    job_ids = manager.submit(jobs)
    print('submited jobs: ', str(job_ids))

In this code, we submitted a job with four iterations. The standard output stream should be redirected to file
job.out with iteration index as postfix. As a program to execute in job iteration, we passed the simple *bash* command.
The above code should print a list with just one element: the submitted job identifier. Because we didn't name our
job, the automatically generated name was returned. The job name can passed as keyword argument ``name`` to ``Jobs.add``
method.

Now we can check the status of our submitted job:

.. code:: bash

    job_status = manager.status(job_ids)
    print('job status: ', job_status)

The ``job_status`` should contain dictionary ``jobs`` with our job status information. Because our job was very short,
and should finish immediately, the ``state`` key of ``data`` dictionary of our job's status, should contain value
``SUCCEED``. For longer jobs, we may want to wait until our submitted jobs finish, to do this we use the ``wait4``
*Manager* method:

.. code:: bash

    manager.wait4(job_ids)

Alternatively we can use the ``wait4all`` method, which will wait until all submitted to the QCG PilotJob Manager jobs
finish:

.. code:: bash

    manager.wait4all()

If we check current directory, we can see that bunch of ``job.out.`` files has been created with a proper content.
If we want to get detailed information about our job, we can use the ``info`` method:

.. code:: bash

    job_info = manager.info(job_ids)
    print('job detailed information: ', job_info)

In return we will get information about iterations (how many finished successfully, how many failed) and when our job
finished.

It is important to call ``finish`` method at the end of our program. This method sent a proper command to QCG PilotJob
Manager instance, and terminates the background thread in which the instance has been run.

.. code:: bash

    manager.finish()

The QCG PilotJob Manager creates a directory `.qcgpjm-service-` where the following files are stored:

- ``service.log`` - logs of QCG PilotJob Manager, very useful in case of problems
- ``jobs.report`` - the file containing information about all finished jobs, by default written in text format, but
  there is an option for JSON format which will be easier to parse.

Example batch usage
-------------------

The same jobs we can launch using the batch method and prepared input files. In this mode, we have to create JSON file
with all requests we want to sent to QCG PilotJob Manager. For example, the file contains jobs we submitted in previous
section will look like this:

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "example",
            "iteration": { "stop": 4 },
            "execution": {
              "script": "echo \"job ${it} executed at `date` @ `hostname`\"",
              "stdout": "job.out.${it}"
            }
          }
        ]
      },
      {
        "request": "control",
        "command": "finishAfterAllTasksDone"
      }
    ]

After placing above content in the JSON file, for example ``jobs.json``, we can execute this workflow with:

.. code:: bash

    $ python -m qcg.pilotjob.service --file-path jobs.json

Alternatively, we can use the ``qcg-pm-service`` command alias, that is installed with ``qcg-pilotjob`` Python package.

.. code:: bash

    $ qcg-pm-service --file-path jobs.json

In the input file, we have placed two requests:

- ``submit`` - with job description we want to run
- ``control`` - with ``finishAfterAllTasksDone`` command, which is required to finish QCG PilotJob Manager (the service
  might listen also on other interfaces, like ZMQ network interface, and must explicitly know when no more requests will
  come and service may be stopped.

The result of executing QCG PilotJob Manager with presented example file should be the same as using the API - the bunch
of output files should be created, as well as ``.qcgpjm-service-`` directory with additional files.

Modes of execution
------------------

Scheduling systems
~~~~~~~~~~~~~~~~~~

In the previous examples we submitted a single CPU applications. The QCG PilotJob Manager is intended for use in HPC
environments, especially with *Slurm* scheduling system. In such environments, we submit a request to scheduling system
and when requested resources are available, the allocation is created and our application is run inside it. Of course
we might run our job's directly in scheduling system without any pilot job mechanism, but we have to remember about
some limitations of scheduling systems such as - maximum number of submitted/executing jobs in the same time, queueing
time (significant for large number of jobs), job array mechanism only for same resource requirement jobs. Generally,
scheduling systems wasn't designed for handling very large number of small jobs.

To use QCG PilotJob Manager in HPC environment, we sugest to install QCG PilotJob Manager via virtual environment in
directory shared among all computing nodes (most of home directories are available from computing nodes). On some
systems, we need to load a proper Python >= 3.6 module before:

.. code:: bash

    $ module load python/3.7.3

Next we can create virtual environment with QCG PilotJob Manager:

.. code:: bash

    $ python3 -m virtualenv $HOME/qcgpj-venv
    $ source $HOME/qcgpj-venv/bin/activate
    $ pip install qcg-pilotjob

Now we can use this virtual environment in our jobs. The example job submission script for *Slurm* scheduling system
that launched application ``myapp.py`` that uses QCG PilotJob Manager API, may look like this:

.. code:: bash

    #SBATCH --job-name=qcgpilotjob-ex
    #SBATCH --nodes=2
    #SBATCH --tasks-per-node=28
    #SBATCH --time=60

    module load python/3.7.3
    source $HOME/qcgpj-venv/bin/activate

    python myapp.py

Of course, some scheduing system might require some additional parameters like:

- ``--account`` - name of the account/grant we want to use
- ``--partition`` - the partition name where our job should be scheduled

To submit a job with QCG PilotJob Manager in batch mode with JSON jobs description file, we have to change the last
line to:

.. code:: bash

    python -m qcg.pilotjob.service --file-path jobs.json

Local execution
~~~~~~~~~~~~~~~

The QCG PilotJob Manager supports *local* mode that is suitable for locally testing executiong scenarious. In contrast
to execution mode, where QCG PilotJob Manager is executed in scheduling system allocation, all jobs are launched with
the usage of scheduling system. In the *local* mode, the user itself can define the size of available resources and
execute it's scenario on such defined resources without the having access to scheduling system. It's worth remembering
that QCG PilotJob Manager doesn't verify the physically available resources, also the executed jobs are not launched
with any core/processor affinity. Thus the performance of jobs might not be optimal.

The choice between *allocation* (in scheduling system allocation) or *local* mode is made automatically by the QCG
PilotJob Manager during the start. If scheduling system environment will be detected, the *allocation* mode will be
chosen. In other case, the local mode will be active, and if resources are not defined by the user, the default number
of available cores in the system will be taken.

The command line arguments, that also might by passed as argument ``server_args`` during instantiating the LocalManager
, related to the *local* mode are presented below:

- ``--nodes NODES`` - the available resources definition; the ``NODES`` parameter should have format::

    `[NODE_NAME]:CORES[,[NODE_NAME]:CORES]...`

- ``--envschema ENVSCHEMA`` - job execution environment; for each job the QCG PilotJob Manager can create environment
  similar to the Slurm execution environment

Some examples of resources definition:

- ``--nodes 4`` - single node with 4 available cores
- ``--nodes n1:2`` - single named node with 2 available cores
- ``--nodes 4,2,2`` - three unnnamed nodes with 8 total cores
- ``--nodes n1:4, n2:4, n3:4`` - three named nodes with 12 total cores


Parallelism
-----------

The QCG PilotJob Manager can handle jobs that require more than a single core. The number of required cores and nodes
is specified with ``numCores`` and ``numNodes`` parameter of ``Jobs.add`` method. The number of required resources
can be specified either as specific values or as a range of resources (with minimum and maximum values), where QCG
PilotJob Manager will try to assign as much resources from those available in the moment. The environment of parallel
job is prepared for *MPI* or *OpenMP* jobs.

MPI
~~~

In case of *MPI* programs only one process is launched by the QCG PilotJob Manager that should call a proper MPI
starting program, such as: ``mpirun`` or ``mpiexec``. All the environment for the parallel job, such as
hosts file, and environment variables are prepared by the QCG PilotJob Manager. For example to run *Quantum Espresso*
application, the example program may look like this:

.. code:: bash

    from qcg.pilotjob.api.manager import LocalManager
    from qcg.pilotjob.api.job import Jobs

    manager = LocalManager()

    jobs = Jobs().add(
        name='qe-example',
        exec='mpirun',
        args=['pw.x'],
        stdin='pw.benzene.scf.in',
        stdout='pw.benzene.scf.out',
        modules=['espresso/5.3.0', 'mkl', 'impi', 'mpich'],
        numCores=8)

    job_ids = manager.submit(jobs)
    manager.wait4(job_ids)

    manager.finish()

As we can see in the example, we run a single program ``mpirun`` which is responsible for setup a proper, parallel
environment for the destination program and spawn the *Quantum Espresso* executables (``pw.x``).

In the example program we used some additional options of ``Jobs.add`` method:

- ``stdin`` - points to the file that content should be sent to job's standard input
- ``modules`` - environment modules that should be loaded before job start
- ``numCores`` - how much cores should be allocated for the job

The JSON job description file for the same example is presented below:

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "qe-example",
            "execution": {
              "exec": "mpirun",
              "args": ["pw.x"],
              "stdin": "pw.benzene.scf.in",
              "stdout": "pw.benzene.scf.out",
              "modules": ["espresso/5.3.0", "mkl", "impi", "mpich"]
            },
            "resources": {
              "numCores": { "exact": 8 }
            }
          }
        ]
      },
      {
        "request": "control",
        "command": "finishAfterAllTasksDone"
      }
    ]

OpenMP
~~~~~~

For *OpenMP* programs (shared memory parallel model), where there is one process that spawns many threads on the same
node, we need to use special option ``model`` with ``threads`` value.
To test execution of *OpenMP* program we need to compile a sample application:

.. code:: bash

    $ wget https://computing.llnl.gov/tutorials/openMP/samples/C/omp_hello.c
    $ gcc -Wall -fopenmp -o omp_hello omp_hello.c

Now we can launch this application with QCG PilotJob Manager:

.. code:: python

    from qcg.pilotjob.api.manager import LocalManager
    from qcg.pilotjob.api.job import Jobs

    manager = LocalManager()

    jobs = Jobs().add(
        name='openmp-example',
        exec='omp_hello',
        stdout='omp.out',
        model='threads',
        numCores=8,
        numNodes=1)

    job_ids = manager.submit(jobs)
    manager.wait4(job_ids)

    manager.finish()

The ``omp.out`` file should contain eight lines with *Hello world from thread =*. It is worth to remember, that OpenMP
applications can operate only on single node, so adding ``numNodes=1`` might be necessary in case where there are more
than single node in available resources.

The equivalent JSON job description file for given example is presented below:

.. code:: json

    [
      {
        "request": "submit",
        "jobs": [
          {
            "name": "openmp-example",
            "execution": {
              "exec": "omp_hello",
              "stdout": "omp.ou",
              "model": "threads"
            },
            "resources": {
              "numCores": { "exact": 8 },
              "numNodes": { "exact": 1 }
            }
          }
        ]
      },
      {
        "request": "control",
        "command": "finishAfterAllTasksDone"
      }
    ]

