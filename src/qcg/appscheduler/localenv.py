import multiprocessing as mp
import socket

from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.config import Config


def parse_local_resources(config):
    nodes_conf = Config.EXECUTION_NODES.get(config)

    if nodes_conf:
        nodes = []
        nodes_list = nodes_conf.split(',')
        for nidx, node in enumerate(nodes_list):
            nname, _, ncores = node.rpartition(':')
            nname = nname or 'n{}'.format(nidx)

            if not ncores or int(ncores) < 1:
                raise ValueError('number of cores for a node must not be less than 1')

            nodes.append(Node(nname, int(ncores), 0))
    else:
        nodes = [ Node(socket.gethostname(), mp.cpu_count(), 0) ]

    if not nodes:
        raise ValueError('no node available')

    return Resources(nodes)
