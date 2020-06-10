.. _iter-res-schedulers:

Iteration resources schedulers
==============================

The aim of iteration resources schedulers is to optimise resources usage for iterative tasks. To this end,
the schedulers assign an exact number of resources based on single iteration resource requirements
described as minimum number of resources and number of available resources in allocation.
What is important, the job's resource requirements for iterative tasks do not have to be changed
for different allocations.
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

The iteration resource scheduler for partitioning available resources. This simple iteration resource scheduler splits
all available resources into given partitions, and each iteration will be executed inside whole single partition.
