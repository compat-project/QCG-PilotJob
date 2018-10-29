#!/bin/env bash

echo "pid ($$) @ `hostname --fqdn` (`hostname -i`) - `date` in `pwd`"
env
module load openmpi

mpirun $HOME/mpi-test/mpi_test
sleep 30s
