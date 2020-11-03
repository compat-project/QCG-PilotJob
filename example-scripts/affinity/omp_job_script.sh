#!/bin/bash

export OMP_NUM_THREADS=4

module load gcc/6.2.0
app/omp_app
