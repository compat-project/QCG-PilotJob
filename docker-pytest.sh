#!/bin/bash

docker run  --rm -v ${QCG_PM_REPO_DIR}/src:/srv/src qcg-pjm-partitions:latest /srv/pytest-cov.sh $*
