.. _iter-res-schedulers:

Iteration resources schedulers
==============================

The role of iteration resources scheduler is to based on single iteration resource requirements described as a minimum
number of resources and number of available resources in allocation, assign exact number of resources in order to
optimize resources usage. Therefore the job's resource requirements do not have to be changed for different allocations.
The resource requirements can apply to both: number of cores and number of nodes specifications.

Currently, two schedulers are implemented:

- ``maximum-iters``
- ``split-into``

``maximum-iters``
-----------------

The iteration resource scheduler for maximizing resource usage. The ``maximum-iters`` iteration resource scheduler is
trying to launch as many iterations in the same time on all available resources. In case where number of iterations
exceeds the number of available resources, the ``maximum-iters`` schedulers splits iterations into *steps* minimizing
this number, and allocates as many resources as possible for each iteration inside *step*. The ``max`` attribute
of resource specification is not allowed when ``maximum-iters`` scheduler is used.

``split-into``
--------------

"The iteration resource scheduler for partitioning available resources. This simple iteration resource scheduler splits
all available resources in given partitions, and each iteration will be executed inside whole single partition.
