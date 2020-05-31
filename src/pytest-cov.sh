#!/bin/env bash

source /srv/venv/bin/activate

echo "pytest arguments: $*"

rm -fR /srv/src/coverage.out
mkdir /srv/src/coverage.out
rm -fR /srv/src/htmlcov.report

pytest -v --cov-config=/srv/src/coveragerc.ini --cov=qcg.appscheduler $* src/
coverage html --rcfile=/srv/src/coveragerc.ini
