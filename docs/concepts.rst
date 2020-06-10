Key concepts
============

Modules
-------

QCG-PilotJob Manager consists of the following internal functional modules:

- *Queue* - the queue containing jobs waiting for resources,
- *Scheduler* algorithm - the algorithm selecting jobs and assigning resources to them.
- *Registry* - the permanent registry containing information about all (current and historical) jobs in the system,
- *Executor* - a module responsible for execution of jobs for which resources were assigned.


Queue & scheduler
-----------------

All the jobs submitted to the QCG-PilotJob Manger system are placed in the queue in the order of they arrival. The scheduling algorithm of QCG-PilotJob Manager works on that queue. The goal of the Scheduler is to determine the order of execution and amount of resources assigned to individual jobs to maximise the throughput of the system. The algorithm is based on the following set of rules:

- Jobs being in the queue are processed in the FIFO manner,
- For every feasible (ready for execution) job the maximum (possible) amount of requested resources is determined. If the amount of allocated resources is greater than the minimal requirements requested by the user, the resources are exclusively assigned to the job and the job is removed from the queue to be executed.
- If the minimal resource requirements are greater than total available resources the job is removed from the queue with the ``FAILED`` status.
- If the amount of resources doesn't allow to start the job, it stays in the queue with the ``QUEUED`` status to be taken into consideration again in the next scheduling iteration,
- Jobs waiting for successful finish of any other job, are not taken into consideration and stay in the queue with the ``QUEUED`` state,
- Jobs for which dependency constraints can not be met, due to failure or cancellation of at least one job which they depend on,  are marked as ``OMITTED`` and removed from the queue,
- If the algorithm finishes processing the given job and some resources still remain unassigned the whole procedure is repeated for the next job.


Executors
---------

QCG-PilotJob Manager module named Executor is responsible for execution and control of jobs by interacting with the cluster resource management system. The current implementation contains three different methods of executing jobs:

 - as a local process - this method is used when QCG-PilotJob Manager either has been run outside a Slurm allocation or when parameter ``--resources local`` has been defined,
 - through internal distributed launcher service - currently used only in Slurm allocation for single core jobs,
 - as a Slurm sub job - the job is submitted to the Slurm to be run in current allocation on scheduled resources.

The modular approach allows for relatively easy integration also with other queuing systems.
The QCG-PilotJob Manager and all jobs controlled by it are executed in a single allocation.
To hide this fact from the individual job and to give it an impression that it is executed directly by the queuing
system QCG-PilotJob overrides some of the environment settings. More on this topic is available in
:ref:`Execution environments`

