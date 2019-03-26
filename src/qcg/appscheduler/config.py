import logging

from enum import Enum


class Config(Enum):

    EXECUTOR_WD = {
        'name': 'wd',
        'default': '.'
    }

    EXECUTION_SCHEMA = {
        'name': 'schema',
        'default': 'auto'
    }

    EXECUTION_NODES = {
        'name': 'nodes',
        'default': None
    }

    ENVIRONMENT_SCHEMA = {
        'name': 'envs',
        'default': 'auto'
    }

    FILE_PATH = {
        'name': 'file',
        'default': 'qcg_pm_reqs.json'
    }

    ZMQ_IP_ADDRESS = {
        'name': 'zmq.ip',
        'default': '*'
    }

    ZMQ_PORT = {
        'name': 'zmq.port',
        'default': '5555'
    }

    ZMQ_IFACE_ADDRESS = {
        'name': 'zmq.address',
        'get': lambda conf: 'tcp://%s:%s' % (
            str(Config.ZMQ_IP_ADDRESS.get(conf)),
            str(Config.ZMQ_PORT.get(conf)))

    }

    REPORT_FORMAT = {
        'name': 'report.format',
        'default': 'text'
    }

    REPORT_FILE = {
        'name': 'report.file',
        'default': 'jobs.report'
    }

    LOG_LEVEL = {
        'name': 'log.level',
        'default': 'info'
    }

    def get(self, config):
        if 'get' in self.value:
            return self.value['get'](config)
        else:
            return config.get(self, self.value['default'])
