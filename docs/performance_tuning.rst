Performance tuning
==================

Reserving a core for QCG PJM
----------------------------

We recommend to use ``--system-core`` parameter for workflows that contains many small jobs (HTC) or bigger allocations
(>256 cores). This will reserve a single core in allocation (on the first node of the allocation) for QCG PilogJob
Manager service.
