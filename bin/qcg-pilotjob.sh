#!/bin/bash

exec ./compose -f base.yaml -p qcg-pilotjob "$@"
