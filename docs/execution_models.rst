Execution models
======================

The QCG-PJM service manages individual cores, so it assigns specific cores to
the tasks. From the performance perspective, binding tasks to the cores is more
efficient as it separates tasks from each other.

To support CPU binding, the service must have information about physical
available cores in the system. This information is provided by the *SLURM* in
created allocation, but is not available in case of logical resources, i.e.
where user defines *virtual* cores and nodes. So currently the binding is
supported only when QCG-PJM service is run inside *SLURM* allocation.

The binding of single core tasks is achieved with:

- ``taskset`` command by the custom launching method (node launcher),
- ``--cpu-bind`` option of the ``srun`` command.

Currenlty, the parallel tasks that requires more than one core are launched
only by the ``srun`` or ``mpirun`` commands. The `mask_cpu` flag of the ``srun``'s ```--cpu-bind`` parameter
will contain the CPU masks for all allocated nodes separated with the comma.

Additionally, for all tasks launched by the Slurm with binding supported, the
*QCG_PM_CPU_SET* environment variable will be available and set with core
identifiers separated with comma.

To support process affinity for different parallel applications, QCG-PJM supports
different execution models. Currently following modes are available:

- ``default`` - in this model only single process is launched via ``srun`` command with allocation prepared based on task's resource requirements
- ``threads`` - is designed for running OpenMP tasks on a single node, the process is started with ``srun`` command with ``--cpus-per-task`` parameter set according to number of cores defined in resource requirements
- ``openmpi`` - the processes are started with ``mpirun`` command with rankfile created based on task's resource requirements

    **It is important to define the appropriate OpenMPI modules in ``executable\modules`` element of job description or loading them before staring QCG-PJM**

- ``intelmpi`` - the processes are started with ``mpirun`` command (from IntelMPI distribution) with defined multiple component, where each component describing execution node, contains ``-host`` element and ``I_MPI_PIN_PROCESSOR_LIST`` arguments set according to the allocated resources

    **It is important to define the appropriate IntelMPI modules in ``executable\modules`` element of job description or loading them before staring QCG-PJM**

- ``srunmpi`` - the processes are started with ``srun`` command with ``--cpu-bind`` parameter set according to the allocated resources; this model should be used only on sites that have properly configured OpenMPI/IntelMPI/Slurm environments

    **It is important to define the appropriate IntelMPI/OpenMPI modules in `executable\modules` element of job description or loading them before staring QCG-PJM**

We suggest to use ``srunmpi`` model for MPI applications on HPC sites wherever such configurations
are available and properly configured.
