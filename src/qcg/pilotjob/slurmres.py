import logging
import os
import subprocess

from math import log2

from qcg.pilotjob.errors import SlurmEnvError
from qcg.pilotjob.resources import CRType, CRBind, Node, Resources, ResourcesType
from qcg.pilotjob.config import Config


def parse_nodelist(nodespec):
    """Return full node names based on the Slurm node specification.

    This method calls ``scontrol show hostnames`` to get real node host names.

    Args:
        nodespec (str): Slurm node specification

    Returns:
        list(str): node hostnames
    """
    proc = subprocess.Popen(['scontrol', 'show', 'hostnames', nodespec], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ex_code = proc.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol command failed: {}".format(stderr))

    return bytes.decode(stdout).splitlines()


def get_allocation_data():
    """Get information about slurm allocation and pack it into dictionary.
    The information is obtained by 'scontrol show job' command

    Returns:
        dict(str,str): list of all allocation attributes and values and also a dictionary, a dictionary
            might be used to check if any element exist in attributes, but for some attributes like Node, CPU_IDs they
            are not uniq so in the map there will be just the last occurence of these attributes; remember that
            the dictionary doesn't contain information about attributes order.
    """
    slurm_job_id = os.environ.get('SLURM_JOB_ID', None)
    proc = subprocess.Popen(['scontrol', 'show', '-o', '--detail', 'job', slurm_job_id], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    ex_code = proc.wait()
    if ex_code != 0:
        raise SlurmEnvError("scontrol show job failed: {}".format(stderr))

    raw_data = bytes.decode(stdout).replace('\n', ' ')
    result = {'list': [], 'map': {}}
    for element in raw_data.split(' '):
        elements = element.split('=', 1)
        name = elements[0]
        value = elements[1] if len(elements) > 1 else None

        result['map'][name] = value
        result['list'].append((name, value))

    return result


def parse_slurm_cpu_binding(cpu_bind_list):
    """Return CPU identifier list based on Slurm's SLURM_CPU_BIND_LIST variable's value.

    Args:
        cpu_bind_list (str): the value of SLURM_CPU_BIND_LIST

    Returns:
        list (int): list of CPU identifiers
    """
    cores = []
    for hex_mask in cpu_bind_list.split(','):
        mask = int(hex_mask, 0)

        cores.extend([i for i in range(int(log2(mask)) + 1) if mask & (1 << i)])

    # sort uniq values
    return sorted(list(set(cores)))


def parse_slurm_job_cpus(cpus):
    """Return number of cores allocated on each node in the allocation.

    This method parses value of Slurm's SLURM_JOB_CPUS_PER_NODE variable's value.

    Args:
        cpus (str): the value of SLURM_JOB_CPUS_PER_NODE

    Returns:
        list (int): the number of cores on each node
    """
    result = []

    for part in cpus.split(','):
        if part.find('(') != -1:
            cores, times = part.rstrip(')').replace('(x', 'x').split('x')
            for _ in range(0, int(times)):
                result.append(int(cores))
        else:
            result.append(int(part))

    return result


def get_num_slurm_nodes():
    """Return number of nodes in Slurm allocation.

    Returns:
        int: number of nodes
    """
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    return len(parse_nodelist(os.environ['SLURM_NODELIST']))


def parse_slurm_resources(config):
    """Return resources availabe in Slurm allocation.

    Args:
        config (dict): QCG-PilotJob configuration

    Returns:
        Resources: resources available in Slurm allocation
    """
    if 'SLURM_NODELIST' not in os.environ:
        raise SlurmEnvError("missing SLURM_NODELIST settings")

    if 'SLURM_JOB_CPUS_PER_NODE' not in os.environ:
        raise SlurmEnvError("missing SLURM_JOB_CPUS_PER_NODE settings")

    allocation_data = None
    if config.get('parse_nodes', 'yes') == 'yes':
        allocation_data = get_allocation_data()

    slurm_nodes = os.environ['SLURM_NODELIST']

    if config.get('parse_nodes', 'yes') == 'no':
        node_names = slurm_nodes.split(',')
    else:
        node_names = parse_nodelist(slurm_nodes)

    node_start = Config.SLURM_LIMIT_NODES_RANGE_BEGIN.get(config)
    if not node_start:
        node_start = 0

    node_end = Config.SLURM_LIMIT_NODES_RANGE_END.get(config)
    if not node_end:
        node_end = len(node_names)
    else:
        node_end = min(node_end, len(node_names))

    logging.debug('node range %d - %d from %d total nodes', node_start, node_end, len(node_names))

    if node_start > node_end:
        raise SlurmEnvError(
            'Invalid node range arguments - node start {} greater than node end {}'.format(node_start, node_end))

    slurm_job_cpus = os.environ['SLURM_JOB_CPUS_PER_NODE']
    cores_num = parse_slurm_job_cpus(slurm_job_cpus)

    if len(node_names) != len(cores_num):
        raise SlurmEnvError(
            "failed to parse slurm env: number of nodes (%d) mismatch number of cores (%d)" % (len(node_names),
                                                                                               len(cores_num)))
    core_ids = None
    binding = False

    if allocation_data is not None and 'CPU_IDs' in allocation_data['map']:
        core_ids = parse_slurm_allocation_cpu_ids(allocation_data['list'], node_names[node_start:node_end],
                                                  cores_num[node_start:node_end])
        print('got cores binding per node: {}'.format(','.join(
            ['{}: [{}]'.format(node_name, ','.join([str(core) for core in node_cores]))
             for node_name, node_cores in core_ids.items()])))
        binding = True
    elif 'SLURM_CPU_BIND_LIST' in os.environ and 'SLURM_CPU_BIND_TYPE' in os.environ and \
            os.environ['SLURM_CPU_BIND_TYPE'].startswith('mask_cpu'):
        core_ids = parse_slurm_env_binding(os.environ['SLURM_CPU_BIND_LIST'], node_names[node_start:node_end],
                                           cores_num[node_start:node_end])
        binding = True
    else:
        logging.warning('warning: failed to get slurm binding information - missing SLURM_CPU_BIND_LIST and CPU_IDSs')

    if binding:
        logging.debug('core binding: %s', ','.join([str(id) for id in core_ids]))

    n_crs = None
    if 'CUDA_VISIBLE_DEVICES' in os.environ:
        n_crs = {CRType.GPU: CRBind(CRType.GPU, os.environ['CUDA_VISIBLE_DEVICES'].split(','))}

    nodes = [Node(node_names[i], cores_num[i], 0, core_ids=core_ids[node_names[i]] if core_ids is not None else None,
                  crs=n_crs) for i in range(node_start, node_end)]
    logging.debug('generated %d nodes %s binding', len(nodes), 'with' if binding else 'without')

    return Resources(ResourcesType.SLURM, nodes, binding)


def parse_slurm_allocation_cpu_ids(allocation_data_list, node_names, cores_num):
    """Based on allocation data obtained via 'scontrol show job --detail' return information about core bindings per
    node.
    The information in the allocation data is optimized, so getting those binding might be tricky.
    The data can be described in form:

        Nodes=c[1-2] CPU_IDs=0 Mem=0 GRES=

    but also as:

        Nodes=c1 CPU_IDs=1 Mem=0 GRES=
        Nodes=c2 CPU_IDs=0 Mem=0 GRES=

    Args:
        allocation_data_list (dict): allocation data (as map ('map' key) and list ('list'))
        node_names (list(str)): node names for which the binding should be parsed
        cores_num (list(int)): the number of allocated cores for given nodes (the binding information must match #
            of cores)

    Returns:
        dict: map with node names as keys and binded core list as value
    """
    nodes = {}

    try:
        curr_nodes = []
        for param in allocation_data_list:
            name, value = param

            if name == 'Nodes':
                curr_node = value
                if any(c in value for c in ['-', '[', ']']):
                    curr_nodes.extend(parse_nodelist(value))
                else:
                    curr_nodes.append(value)
            elif name == 'CPU_IDs':
                core_ids = []
                for element in value.split(','):
                    if '-' in element:
                        start, end = element.split('-', 1)
                        core_ids.extend(list(range(int(start), int(end) + 1)))
                    else:
                        core_ids.append(int(element))

                if curr_nodes is None:
                    raise Exception('missing node name in allocation data')

                for curr_node in curr_nodes:
                    if curr_node in node_names:
                        nodes[curr_node] = core_ids

                curr_nodes = []
    except Exception as exc:
        raise SlurmEnvError('unknown format of cpu binding: {}'.format(str(exc)))

    if len(nodes) != len(node_names):
        raise SlurmEnvError('failed to parse cpu binding from job info: the node binding list ({}) differes from '
                            'node list ({})'.format(len(nodes), len(node_names)))

    if len(nodes) != len(cores_num):
        raise SlurmEnvError('failed to parse cpu binding from job info: the node binding list ({}) differes from '
                            'ncores per node list ({})'.format(len(nodes), len(cores_num)))

    for idx, node in enumerate(nodes.keys()):
        if len(nodes[node]) != cores_num[idx]:
            raise SlurmEnvError('failed to parse cpu binding: the node binding for node ({}) ({}) differs from cores '
                                'per node {}'.format(node, len(nodes[node]), cores_num[idx]))

    return nodes


def parse_slurm_env_binding(slurm_cpu_bind_list, node_names, cores_num):
    """Based on environment varialbe SLURM_CPU_BIND_LIST set by slurm return information about core bindings per node.
    WARNING: those information might not be as precise as those obtained from allocation data, as environment variable
    contain the same information for all nodes, so if not all nodes has the same architecture and number of allocated
    cores the information might not be correct.

    Args:
        slurm_cpu_bind_list (str): value of SLURM_CPU_BIND_LIST variable
        node_names (list(str)): name of the nodes
        cores_num (list(int)): number of cores on each node

    Returns:
        dict(str,int): map with node names and core identifier list
    """
    core_ids = parse_slurm_cpu_binding(slurm_cpu_bind_list)

    if len(core_ids) < max(cores_num):
        raise SlurmEnvError("failed to parse cpu binding: the core list ({}) mismatch the cores per node ({})".format(
            str(core_ids), str(cores_num)))

    logging.debug("cpu list on each node: %s", str(core_ids))

    return {node_name: core_ids for node_name in node_names}


def in_slurm_allocation():
    """Check if program has been run inside slurm allocation.
    We detect some environment variables (like SLURM_NODELIST) that are always set by slurm.

    :return: true if we are inside slurm allocation, otherwise false
    """
    return 'SLURM_NODELIST' in os.environ and 'SLURM_JOB_CPUS_PER_NODE' in os.environ


def test_environment(env=None):
    """Try to parse slurm resources based on environment passed as dictionary, or string where each
    environment variable is placed in the separate line.
    WARNING: some information must be gathered through slurm client programs like 'scontrol' so gathering resource
    information only on environment variables is limited.

    Args:
        env (dict|string,optional): environment to test, if
            None - the current environment is checked
            dict - the environment in form of dictionary to be checked
            string - the environment in form of string where each variable is placed in separate line

    Returns:
        Resources: instance with gathered slurm information.
    """
    if not env:
        env_d = os.environ
    elif isinstance(env, (dict, os._Environ)):
        env_d = env
    elif isinstance(env, str):
        env_d = {line.split('=', 1)[0]: line.split('=', 1)[1]
                 for line in env.splitlines() if len(line.split('=', 1)) == 2}
    else:
        raise ValueError('Wrong type of argument ({}) - only string/dict are allowed'.format(type(env).__name__))

    old_os_environ = os.environ
    try:
        os.environ = env_d
        result = parse_slurm_resources({'parse_nodes': 'no'})
    finally:
        os.environ = old_os_environ

    print('parsed SLURM resources: {}'.format(result))
    return result
