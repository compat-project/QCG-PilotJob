Dictionary
==========

Scheduling system
  A service that controls and schedules access to the fixed set of computational resources (aka. queuing system,
  workload manager, resource management system). The current implementation of QCG Pilot Job supports SLURM cluster
  management and job scheduling system.

Job
  A sequential or parallel program with defined resource requirements

Job array
  A mechanism that allows to submit a set of jobs with the same resource requirements to the scheduling system at once;
  commonly used in parameter sweep scenarios

Allocation
  A set of resources allocated by the scheduling system for a specific time period; resources assigned to an allocation
  are static and do not change in time

QCG PilotJob Manager
  A service started inside a scheduling system allocation that schedules and controls execution of jobs on the same
  allocation

QCG PilotJob Manager API
  An interface in the form of Python module that provides communication with the QCG PilotJob Manager

Application Controller
  A user's program run as one of jobs inside QCG PilotJob Manager that, using the QCG PilotJob Manager API, dynamically
  submits and synchronizes new jobs