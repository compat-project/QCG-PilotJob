#!/bin/env bash

source /srv/venv/bin/activate

echo "pytest arguments: $*"

pytest -v --cov=qcg.appscheduler $* src/
coverage html -d src/htmlcov
