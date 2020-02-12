import logging
import os
import subprocess

from math import log2

from qcg.appscheduler.errors import *
from qcg.appscheduler.resources import Node, Resources, ResourcesType


def parse_nodelist(nodespec):
    p = subprocess.Popen(['scontrol', 'show', 'hostnames', nodespec], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    ex_code = p.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol command failed: %s" % stderr)

    return stdout.splitlines()


def parse_slurm_cpu_binding(cpu_bind_list):
    cores = []
    for hex_mask in cpu_bind_list.split(','):
        mask = int(hex_mask, 0)

        cores.extend([i for i in range(int(log2(mask)) + 1) if mask & (1 << i)])

    # sort uniq values
    return sorted(list(set(cores)))


def parse_slurm_job_cpus(cpus):
    result = []

    for part in cpus.split(','):
        #		print "part %s" % part
        if part.find('(') != -1:
            cores, n = part.rstrip(')').replace('(x', 'x').split('x')
            #			print "part stripped %s,%s" % (cores,n)
            for i in range(0, int(n)):
                result.append(int(cores))
        else:
            result.append(int(part))

    return result


def parse_slurm_resources(config):
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    if 'SLURM_JOB_CPUS_PER_NODE' not in os.environ:
        raise SlurmEnvError("missing SLURM_JOB_CPUS_PER_NODE settings")

    slurm_nodes = os.environ['SLURM_NODELIST']
    node_names = parse_nodelist(slurm_nodes)

    slurm_job_cpus = os.environ['SLURM_JOB_CPUS_PER_NODE']
    cores_num = parse_slurm_job_cpus(slurm_job_cpus)

    if len(node_names) != len(cores_num):
        raise SlurmEnvError(
            "failed to parse slurm env: number of nodes (%d) mismatch number of cores (%d)" % (len(node_names),
                                                                                               len(cores_num)))

    core_ids = None
    binding = False

    if 'SLURM_CPU_BIND_LIST' in os.environ and \
            'SLURM_CPU_BIND_TYPE' in os.environ and \
            os.environ['SLURM_CPU_BIND_TYPE'].startswith('mask_cpu'):
        core_ids = parse_slurm_cpu_binding(os.environ['SLURM_CPU_BIND_LIST'])

        if len(core_ids) < max(cores_num):
            raise SlurmEnvError("failed to parse cpu binding: the core list ({}) mismatch the cores per node ({})".format(
                str(core_ids), str(cores_num)))

        logging.debug("cpu list on each node: {}".format(core_ids))
        binding = True

    nodes = []
    for i in range(0, len(node_names)):
        nname = bytes.decode(node_names[i])
        logging.debug("%s x %d" % (nname, cores_num[i]))
        nodes.append(Node(nname, cores_num[i], 0, coreIds=core_ids))

    logging.debug("generated {} nodes {} binding".format(len(nodes), "with" if binding else "without"))
    return Resources(ResourcesType.SLURM, nodes, binding)


def in_slurm_allocation():
    return 'SLURM_NODELIST' in os.environ and 'SLURM_JOB_CPUS_PER_NODE' in os.environ
