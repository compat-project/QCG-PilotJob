#!/bin/env bash

source /srv/venv/bin/activate

echo "pytest arguments: $*"

rm -fR /srv/src/coverage.out
mkdir /srv/src/coverage.out
rm -fR /srv/src/htmlcov.report

pytest -v --cov-config=/srv/src/coveragerc.ini --cov=qcg.pilotjob $*
EXIT_CODE=$?
echo "pytest returned ${EXIT_CODE}"
coverage html --rcfile=/srv/src/coveragerc.ini
echo "coverage returned ${?}"

exit ${EXIT_CODE}
