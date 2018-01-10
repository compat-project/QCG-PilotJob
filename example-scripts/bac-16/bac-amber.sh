#!/usr/bin/env bash

. /home/plgrid/plgkopta/qcg-pilotmanager/qcg-pilotmanager/qcg-scripts/qcg-apps.qcg

rep=$1
[ "x$rep" == "x" ] && { echo "error: missing replica number argument"; exit 1; }

cd replicas/rep${rep}/fe-calc
qcg_call_app amber_MMPBSA -i ../../nmode.in -sp ../../../build/complex.top -cp ../../../build/com.top -rp ../../../build/rec.top -lp ../../../build/lig.top -y ../equilibration/eq1.dcd
if [ $? -ne 0 ]; then
	echo "error: amber failed"
	exit 1
fi

#cd ../../../..
#tar czf rep${rep}-output.tgz -C input/replicas rep${rep}
