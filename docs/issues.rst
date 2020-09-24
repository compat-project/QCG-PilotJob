Partial nodes allocations
=========================

When running OMP applications on partial nodes allocations, the parameters ``map_cpu`` and ``mask_cpu`` of ``--cpu-bind``
parameter is ignored:

.. code:: bash

    srun -n 1 --cpus-per-task 2 --cpu-bind=verbose,mask_cpu:0x3 omp_app

Will not bind the single executing process of ``omp_app`` to the first two cores in the node, but instead bind the
process to **all** allocated cores on node. When used ``threads`` parameter:

.. code:: bash

    srun -n 1 --cpus-per-task 2 --cpu-bind=verbose,threads omp_app

Slurm will bind the process to the **first** two cores of the allocation, in such case we have no decision about which
cores will be selected.

When allocation contains entire node, the ``mask_cpu`` and ``map_cpu`` parameters are properly interpreted by Slurm.
