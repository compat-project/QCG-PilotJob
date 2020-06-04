Usage
=====

The QCG PilotJob Manager can be used in two different ways:

- as an service accessible with API
- as a command line utility to execute static, prepared job workflows

The second method allows to dynamically control the jobs execution.

Example application
-------------------

Let's write a simple program that will runs 10 instances of ?? program.

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

    job_status = manager.status(job_ids)
    print('job status: ', job_status)

The ``job_status`` should contain dictionary ``jobs`` with our job status information. The ``state`` key of ``data``
dictionary of our job's status, should contain value ``SUCCEED``. If we check current directory, we can see that bunch
of ``job.out.`` files has been created with a proper content. If we want to get detailed information about our job,
we can use the ``info`` method:

    job_info = manager.info(job_ids)
    print('job detailed information: ', job_info)

In return we will get information about iterations (how many finished successfully, how many failed) and when our job
finished.

It is important to call ``finish`` method at the end of our program. This method sent a proper command to QCG PilotJob
Manager instance, and terminates the background thread in which the instance has been run.

    manager.finish()

The QCG PilotJob Manager creates a directory `.qcgpjm-service-` where the following files are stored:

-

