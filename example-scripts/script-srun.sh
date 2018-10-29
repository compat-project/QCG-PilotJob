#!/bin/env bash

echo "pid ($$) @ `hostname --fqdn` (`hostname -i`) - `date` in `pwd`"
env

srun hostname --fqdn
sleep 20s
