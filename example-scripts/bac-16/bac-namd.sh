#!/usr/bin/env bash

. /home/plgrid/plgkopta/qcg-pilotmanager/qcg-pilotmanager/qcg-scripts/qcg-apps.qcg


rep=$1
[ "x$rep" == "x" ] && { echo "error: missing replica number argument"; exit 1; }

cd mineq_confs
if [ -f "eq0-rep${rep}.conf" ]; then
	qcg_call_app namd eq0-rep${rep}.conf > ../replicas/rep${rep}/equilibration/eq0.log
	[ $? -ne 0 ] && { echo "error: namd eq0 finished with error" && exit 1; }
fi

if [ -f "eq1-rep${rep}.conf" ]; then
	qcg_call_app namd eq1-rep${rep}.conf > ../replicas/rep${rep}/equilibration/eq1.log
	[ $? -ne 0 ] && { echo "error: namd eq0 finished with error" && exit 1; }
fi

if [ -f "eq2-rep${rep}.conf" ]; then
	qcg_call_app namd eq2-rep${rep}.conf > ../replicas/rep${rep}/equilibration/eq2.log
	[ $? -ne 0 ] && { echo "error: namd eq0 finished with error" && exit 1; }
fi

