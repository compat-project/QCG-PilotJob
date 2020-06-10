#!/bin/bash

echo "job env:"
env

module load openmpi
mpirun -cpu-set "${QCG_PM_CPU_SET}" --report-bindings --bind-to core app/mpi_app
