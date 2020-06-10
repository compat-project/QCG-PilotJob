Parallelism
===========

QCG-PilotJob Manager can handle jobs that require more than a single core. The number of required cores and nodes
is specified with ``numCores`` and ``numNodes`` parameter of ``Jobs.add`` method. The number of required resources
can be specified either as specific values or as a range of resources (with minimum and maximum values), where
QCG-PilotJob Manager will try to assign as much resources from those available in the moment.
The environment of parallel job is prepared for *MPI* or *OpenMP* jobs.

MPI
---

In case of *MPI* programs only one process is launched by QCG-PilotJob Manager that should call a proper MPI
starting program, such as: ``mpirun`` or ``mpiexec``. All the environment for the parallel job, such as
hosts file, and environment variables are prepared by QCG-PilotJob Manager. For example to run *Quantum Espresso*
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
------

For *OpenMP* programs (shared memory parallel model), where there is one process that spawns many threads on the same
node, we need to use special option ``model`` with ``threads`` value.
To test execution of *OpenMP* program we need to compile a sample application:

.. code:: bash

    $ wget https://computing.llnl.gov/tutorials/openMP/samples/C/omp_hello.c
    $ gcc -Wall -fopenmp -o omp_hello omp_hello.c

Now we can launch this application with QCG-PilotJob Manager:

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