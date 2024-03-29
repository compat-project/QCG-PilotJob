import logging
import os
import subprocess

from qcg.appscheduler.errors import *
from qcg.appscheduler.resources import Node, Resources


def parse_nodelist(nodespec):
    p = subprocess.Popen(['scontrol', 'show', 'hostnames', nodespec], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    ex_code = p.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol command failed: %s" % stderr)

    return stdout.splitlines()


def parse_slurm_ntasks(ntasks):
    result = []

    for part in ntasks.split(','):
        #		print "part %s" % part
        if part.find('(') != -1:
            cores, n = part.rstrip(')').replace('(x', 'x').split('x')
            #			print "part stripped %s,%s" % (cores,n)
            for i in range(0, int(n)):
                result.append(int(cores))
        else:
            result.append(int(part))

    return result


def parse_slurm_resources():
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    if 'SLURM_TASKS_PER_NODE' not in os.environ:
        raise SlurmEnvError("missing SLURM_TASKS_PER_NODE settings")

    slurm_nodes = os.environ['SLURM_NODELIST']
    node_names = parse_nodelist(slurm_nodes)

    slurm_tasks = os.environ['SLURM_TASKS_PER_NODE']
    cores_num = parse_slurm_ntasks(slurm_tasks)

    if len(node_names) != len(cores_num):
        raise SlurmEnvError(
            "failed to parse slurm env: number of nodes (%d) mismatch number of cores (%d)" % (len(node_names),
                                                                                               len(cores_num)))

    nodes = []
    for i in range(0, len(node_names)):
        nname = bytes.decode(node_names[i])
        logging.debug("%s x %d" % (nname, cores_num[i]))
        nodes.append(Node(nname, cores_num[i], 0))

    logging.debug("generated %d nodes" % len(nodes))
    return Resources(nodes)
