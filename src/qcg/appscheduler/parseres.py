import logging

from qcg.appscheduler.config import Config
from qcg.appscheduler.localres import parse_local_resources
from qcg.appscheduler.slurmres import parse_slurm_resources, in_slurm_allocation


def __detect_resources(config):
    logging.info('determining source of information about available resources ...')

    if Config.EXECUTION_NODES.get(config):
        logging.info('selected local resources information')
        return parse_local_resources(config)
    elif in_slurm_allocation():
        logging.info('selected SLURM resources information')
        return parse_slurm_resources(config)
    else:
        logging.info('selected local resources information')
        return parse_local_resources(config)


__RESOURCES_HANDLING__ = {
    'auto': __detect_resources,
    'local': parse_local_resources,
    'slurm': parse_slurm_resources,
}


def get_resources(config):
    """
    Return available resources according to environment.
    """
    res_source = Config.RESOURCES.get(config)

    if not res_source in __RESOURCES_HANDLING__:
        raise ValueError('Invalid resources configuration setting: {}'.format(str(res_source)))

    logging.info('source of information about available resources: {}'.format(str(res_source)))
    return __RESOURCES_HANDLING__[res_source](config)
