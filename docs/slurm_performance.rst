Slurm performance
=================

srun command
------------

The Slurm's ``srun`` client is used to run applications within a created
allocation. Thanks to its tight integration with the queue system, it is able
to run an application on the specified node of our allocation using, for
example, cpu binding mechanisms. Particularly convenient seems to be running
parallel applications using MPI library due to the fact of unification of the
way of running, regardless of the vendor and version of MPI library (note that
the native applications to run applications compiled with different MPI
libraries such as *OpenMPI*/*IntelMPI*/*MPICH* have a different name, syntax and way
of running the target application). Unfortunately, when running an application,
the ``srun`` client communicates with the queue system controller and creates a
step for each running application. It turns out that with too much use of this
client, the queue system controller struggles with quite a heavy load which
affects the performance of the whole queue system.

The QCG-PilotJob service uses the ``srun`` client by default in two cases:

- during service initialization to launch agents running on each allocation node

- in the ``srunmpi`` model when launching user applications.


Recommendations
---------------

Agents
^^^^^^

When running QCG-PilotJob on large allocations (containing more than a few
dozen nodes) it is recommended to use the ``--nl-start-method`` call parameter
with the value ``ssh`` which will cause the QCG-PilotJob service agents to be
started on the allocation nodes using the *ssh* protocol.

.. note::  Ensure that logging in using the `ssh` protocol is done using a
public key without requiring a password. Information on how to configure the
``ssh`` service in this way should be available in the documentation of the
computing system, and usually boils down to generating an ssh key and adding
its public signature to the ``~/.ssh/authorized_keys`` file


Parallel applications
^^^^^^^^^^^^^^^^^^^^^

For scenarios containing a significant number of parallel user jobs, we
recommend that you use the QCG-PilotJob service startup models such as:

- intelmpi

- openmpi


These are models that use native *IntelMPI* and *OpenMPI* library commands to
run parallel applications. Additionally, they allow to configure call
paremeters using ``model_opts/mpirun`` and ``model_opts/mpirun_args`` elements. An
example syntax of such commands is as follows:

1) example of running a LAMMPS application compiled with the *IntelMPI* library
using the *ssh* protocol on a *SupermucNG* system

.. code:: json

			....
			"name": "lammps-bench",
			"execution": {
				"exec": "lmp",
				"args": ["-log", "none", "-i", "in.lammps"],
				"stderr": "stderr",
				"stdout": "stdout",
				"model": "intelmpi",
				"model_opts": { "mpirun_args": [ "-launcher", "ssh" ] }
			},
			"resources": {
				"numCores": {
						"exact": 24
				}
			}
			....

2) example of running an application compiled with the *OpenMPI* library

.. code:: json

		....
		"name": "mpi-app",
        "execution": {
            "exec": "mpiapp",
            "stderr": "stderr",
            "stdout": "stdout",
            "model": "openmpi",
            "model_opts": {
				"mpirun": "/opt/exp_soft/local/skylake/openmpi/4.1.0_gcc620/bin/mpirun",
				"mpirun_args": ["--mca", "rmaps_rank_file_physical", "1"]
			},
			modules = [ "openmpi/4.1.0_gcc620" ],
        },
        "resources": {
            "numCores": {
            	"exact": 24
            }
        }
		....
