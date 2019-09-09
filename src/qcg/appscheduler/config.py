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

    RESOURCES = {
        'name': 'resources',
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
        'default': None
    }

    ZMQ_PORT_MIN_RANGE = {
        'name': 'zmq.port.min',
        'default': 2222,
    }            

    ZMQ_PORT_MAX_RANGE = {
        'name': 'zmq.port.max',
        'default': 9999,
    }            

    ZMQ_IFACE_ADDRESS = {
        'name': 'zmq.address',
        'get': lambda conf: 'tcp://{}:{}'.format(
            str(Config.ZMQ_IP_ADDRESS.get(conf)),
            str(Config.ZMQ_PORT.get(conf))) if Config.ZMQ_PORT.get(conf) else \
            'tcp://{}'.format(str(Config.ZMQ_IP_ADDRESS.get(conf)))
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

    SYSTEM_CORE = {
        'name': 'system.core',
        'default': False
    }


    def get(self, config):
        if 'get' in self.value:
            return self.value['get'](config)
        else:
            return config.get(self, self.value['default'])
