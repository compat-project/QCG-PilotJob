#!/bin/bash

# 20.11 and 21.08 are currently broken, so not tested
# See https://bugs.schedmd.com/show_bug.cgi?id=13553

# weird command to assign only if not already set
: ${SLURM_VERSIONS:='17.02 17.11 18.08 19.05 20.02'}

QCGPJ_COMMIT=$(git rev-parse --short HEAD)

SUMMARY=$(mktemp)

echo -n "Run at " >>${SUMMARY}
date >>${SUMMARY}
echo "Tested Slurm versions ${SLURM_VERSIONS} with QCG-PilotJob commit ${QCGPJ_COMMIT} and options ${*}" >>${SUMMARY}

mkdir -p test_all_slurm_versions

for SLURM_VERSION in $SLURM_VERSIONS ; do
    export QCG_TEST_SLURM_VERSION=${SLURM_VERSION}

    # build containers
    echo
    echo -n "Building Slurm ${SLURM_VERSION}, started at " ; date
    echo
    echo "Use 'tail -f test_all_slurm_versions/build-${SLURM_VERSION}.out' to track progress."
    echo
    cd slurm-docker-cluster
    docker build --build-arg SLURM_TAG=slurm-${SLURM_VERSION} -t slurm-docker-cluster:${SLURM_VERSION} . >../test_all_slurm_versions/build-${SLURM_VERSION}.out 2>&1
    if [ $? -ne 0 ] ; then
        echo -n "Build failed, stopping at " ; date
        exit 1
    fi
    echo -n "Build done at " ; date

    # start virtual cluster
    echo
    echo "Starting virtual cluster"
    source env.sh

    # clean up any remaining volumes to ensure we start from the same state
    docker volume rm \
        slurm-docker-cluster_etc_munge slurm-docker-cluster_etc_slurm \
        slurm-docker-cluster_slurm_jobdir slurm-docker-cluster_var_lib_mysql \
        slurm-docker-cluster_var_log_slurm
    docker-compose up -d
    echo "Started cluster, sleeping a bit while Slurm initialises everything"
    sleep 30
    echo "Woken up, registering"
    bash ./register_cluster.sh
    echo "Registered ($?), waiting for Slurm to restart"
    sleep 30

    # run tests
    echo
    echo -n "Test run for Slurm ${SLURM_VERSION} starting at " ; date
    echo
    echo "Use 'tail -f components/core/slurm-tests-out.txt' to track progress."
    echo "Use 'docker exec slurmctld /usr/bin/scancel <jobid>' to kill a hanging test."
    echo

    cd ..
    bash ./run-slurm-docker-tests.sh $*
    echo "Result for ${SLURM_VERSION}: $?" >>${SUMMARY}

    cp components/core/slurm-tests-out.txt test_all_slurm_versions/slurm-tests-out-${QCGPJ_COMMIT}-${SLURM_VERSION}.txt
    docker cp slurmctld:/data/. test_all_slurm_versions/data-${QCGPJ_COMMIT}-${SLURM_VERSION}
    echo -n "Test done at " ; date

    # stop virtual cluster
    echo
    echo "Stopping virtual cluster"
    cd slurm-docker-cluster
    docker-compose down
    cd ..
done

# clean up volumes
docker volume rm \
    slurm-docker-cluster_etc_munge slurm-docker-cluster_etc_slurm \
    slurm-docker-cluster_slurm_jobdir slurm-docker-cluster_var_lib_mysql \
    slurm-docker-cluster_var_log_slurm

echo >>"${PWD}/test_all_slurm_versions/summary.txt"
cat ${SUMMARY} >>"${PWD}/test_all_slurm_versions/summary.txt"

echo
cat ${SUMMARY}
rm ${SUMMARY}

echo "All tests done, results saved to test_all_slurm_versions/"

