#!/bin/env bash

echo "pid ($$) @ `hostname --fqdn` (`hostname -i`) - `date` in `pwd`"
echo "loaded modules:"
module list
echo "environment:"
env
