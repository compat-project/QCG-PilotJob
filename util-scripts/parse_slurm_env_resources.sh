#!/bin/bash

if [ $# -eq 0 ]; then
    echo -e "error: missing arguments\n\n\tparse_slurm_env_resources.sh {env_filename}\n\n"
    exit 1
fi

fname=$1
echo "from qcg.appscheduler.slurmres import test_environment; test_environment(open(\"${fname}\", \"r\").read())" |  python3
