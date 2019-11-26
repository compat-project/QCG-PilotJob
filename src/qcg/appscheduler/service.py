import argparse
import asyncio
import logging
import os
import sys
import traceback
import resource
import socket
import random
from os.path import exists, join, isabs
from datetime import datetime

from multiprocessing import Process, Queue


from qcg.appscheduler.errors import InvalidArgument
from qcg.appscheduler.fileinterface import FileInterface
from qcg.appscheduler.manager import DirectManager
from qcg.appscheduler.partitions import GovernorManager
from qcg.appscheduler.receiver import Receiver
from qcg.appscheduler.zmqinterface import ZMQInterface
from qcg.appscheduler.config import Config
from qcg.appscheduler.reports import getReporter
import qcg.appscheduler.profile




class QCGPMService:

    def __init__(self, args=None):
        """
        QCG Pilot Job Manager.

        Args:
            args (str[]) - command line arguments, if None the command line arguments are parsed
        """

        parser = argparse.ArgumentParser()
        parser.add_argument('--net',
                            help='enable network interface',
                            action='store_true')
        parser.add_argument('--net-port',
                            help='port to listen for network interface (implies --net)',
                            type=int, default=None)
        parser.add_argument('--net-port-min',
                            help='minimum port range to listen for network interface if exact port number is not defined (implies --net)',
                            type=int, default=None)
        parser.add_argument('--net-port-max',
                            help='maximum port range to listen for network interface if exact port number is not defined (implies --net)',
                            type=int, default=None)
        parser.add_argument('--file',
                            help='enable file interface',
                            action='store_true')
        parser.add_argument('--file-path',
                            help='path to the request file (implies --file)',
                            default=None)
        parser.add_argument('--wd',
                            help='working directory for the service',
                            default=Config.EXECUTOR_WD.value['default'])
        parser.add_argument('--envschema',
                            help='job environment schema [auto|slurm]',
                            default='auto')
        parser.add_argument('--resources',
                            help='source of information about available resources [auto|slurm|local] as well as a method of job execution (through local processes or as a Slurm sub jobs)',
                            default=Config.RESOURCES.value['default'])
        parser.add_argument('--report-format',
                            help='format of job report file [text|json]',
                            default=Config.REPORT_FORMAT.value['default'])
        parser.add_argument('--report-file',
                            help='name of the job report file',
                            default=Config.REPORT_FILE.value['default'])
        parser.add_argument('--nodes',
                            help='configuration of available resources (implies --resources local)',
                            )
        parser.add_argument('--log',
                            help='log level',
                            choices=[ 'critical', 'error', 'warning', 'info', 'debug', 'notset' ],
                            default=Config.LOG_LEVEL.value['default'])
        parser.add_argument('--system-core',
                            help='reserve one of the core for the QCG-PJM',
                            default=False, action='store_true')
        parser.add_argument('--disable-nl',
                            help='disable custom launching method',
                            default=Config.DISABLE_NL.value['default'], action='store_true')
        parser.add_argument('--governor',
                            help='run manager in the governor mode, where jobs will be scheduled to execute to the dependant managers',
                            default=Config.GOVERNOR.value['default'], action='store_true')
        parser.add_argument('--parent',
                            help='address of the parent manager, current instance will receive jobs from the parent manaqger',
                            default=Config.PARENT_MANAGER.value['default'])
        parser.add_argument('--id',
                            help='optional manager instance identifier - will be generated automatically when not defined',
                            default=Config.MANAGER_ID.value['default'])
        parser.add_argument('--tags',
                            help='optional manager instance tags separated by commas',
                            default=Config.MANAGER_TAGS.value['default'])

        self.__args = parser.parse_args(args)

        if self.__args.governor or self.__args.parent:
            # imply '--net' in case of hierarchy scheduling - required for inter-manager communication
            self.__args.net = True

        if self.__args.net_port or self.__args.net_port_min or self.__args.net_port_max:
            # imply '--net' if port or one of the range has been defined
            self.__args.net = True    
        
            if not self.__args.net_port_min:
                self.__args.net_port_min = int(Config.ZMQ_PORT_MIN_RANGE.value['default'])

            if not self.__args.net_port_max:
                self.__args.net_port_max = int(Config.ZMQ_PORT_MAX_RANGE.value['default'])

        if self.__args.net:
            # set default values for port min & max if '--net' has been defined
            if not self.__args.net_port_min:
                self.__args.net_port_min = int(Config.ZMQ_PORT_MIN_RANGE.value['default'])

            if not self.__args.net_port_max:
                self.__args.net_port_max = int(Config.ZMQ_PORT_MAX_RANGE.value['default'])

        if self.__args.file and not self.__args.file_path:
            # set default file path if interface has been enabled but path not defined
            self.__args.file_path = Config.FILE_PATH.value['default']

        if self.__args.file_path:
            # enable file interface if path has been defined
            self.__args.file = True

        manager_id = self.__args.id
        if not manager_id:
            manager_id = self.__generateDefaultManagerId()

        manager_tags = [ manager_id ]
        if self.__args.tags:
            manager_tags.extend(self.__args.tags.split(','))

        if not self.__args.net and not self.__args.file:
            raise InvalidArgument("no interface enabled - finishing")

        self.__conf = {
            Config.EXECUTOR_WD: self.__args.wd,
            Config.EXECUTION_NODES: self.__args.nodes,
            Config.ENVIRONMENT_SCHEMA: self.__args.envschema,
            Config.FILE_PATH: self.__args.file_path,
            Config.ZMQ_PORT: self.__args.net_port,
            Config.ZMQ_PORT_MIN_RANGE: self.__args.net_port_min,
            Config.ZMQ_PORT_MAX_RANGE: self.__args.net_port_max,
            Config.REPORT_FORMAT: self.__args.report_format,
            Config.REPORT_FILE: self.__args.report_file,
            Config.LOG_LEVEL: self.__args.log,
            Config.SYSTEM_CORE: self.__args.system_core,
            Config.DISABLE_NL: self.__args.disable_nl,
            Config.GOVERNOR: self.__args.governor,
            Config.PARENT_MANAGER: self.__args.parent,
            Config.MANAGER_ID: manager_id,
            Config.MANAGER_TAGS: manager_tags,
        }

        self.__wd = Config.EXECUTOR_WD.get(self.__conf)

        self.__setupAuxDir(self.__conf)
        self.__setupLogging(self.__conf)
        self.__setupReports(self.__conf)
        self.__setupEventLoop()

        self.__ifaces = []
        if self.__args.file:
            iface = FileInterface()
            iface.setup(self.__conf)
            self.__ifaces.append(iface)

        if self.__args.net:
            iface = ZMQInterface()
            iface.setup(self.__conf)
            self.__ifaces.append(iface)

        if self.__args.governor:
            self.__setupGovernorManager(self.__args.parent)
        else:
            self.__setupDirectManager(self.__args.parent)


    def __generateDefaultManagerId(self):
        return '{}.{}'.format(socket.gethostname(), random.randrange(10000))


    def __setupGovernorManager(self, parentManager):
        logging.info('starting governer manager ...')
        self.__manager = GovernorManager(self.__conf, parentManager)
        self.__notifId = self.__manager.registerNotifier(self.__jobNotify, self.__manager)

        self.__receiver = Receiver(self.__manager.getHandlerInstance(), self.__ifaces, None)

        if self.__receiver.getZmqAddress():
            addressFile = Config.ADDRESS_FILE.get(self.__conf)
            self.__addressFile = addressFile if isabs(addressFile) else join(self.auxDir, addressFile)

            if exists(self.__addressFile):
                os.remove(self.__addressFile)

            with open(self.__addressFile, 'w') as f:
                f.write(self.__receiver.getZmqAddress())

            logging.debug('address interface written to the {} file...'.format(self.__addressFile))


    def __setupDirectManager(self, parentManager):
        logging.info('starting direct manager ...')
        self.__manager = DirectManager(self.__conf, parentManager)
        self.__notifId = self.__manager.registerNotifier(self.__jobNotify, self.__manager)

        self.__receiver = Receiver(self.__manager.getHandlerInstance(), self.__ifaces, self.__manager.resources)

        if self.__receiver.getZmqAddress():
            addressFile = Config.ADDRESS_FILE.get(self.__conf)
            self.__addressFile = addressFile if isabs(addressFile) else join(self.auxDir, addressFile)

            if exists(self.__addressFile):
                os.remove(self.__addressFile)

            with open(self.__addressFile, 'w') as f:
                f.write(self.__receiver.getZmqAddress())

            logging.debug('address interface written to the {} file...'.format(self.__addressFile))


    def __setupReports(self, config):
        self.__jobReporter = getReporter(Config.REPORT_FORMAT.get(config))

        jobReportFile = Config.REPORT_FILE.get(config)
        self.__jobReportFile = jobReportFile if isabs(jobReportFile) else join(self.auxDir, jobReportFile)

        if exists(self.__jobReportFile):
            os.remove(self.__jobReportFile)


    def __setupAuxDir(self, config):
        """
        This method should be called before all other '__setup' methods, as it sets the destination for the
        auxiliary files directory.
        """
        wdir = Config.EXECUTOR_WD.get(self.__conf)

        self.auxDir = join(wdir, '.qcgpjm')
        if not os.path.exists(self.auxDir):
            os.makedirs(self.auxDir)

        self.__conf[Config.AUX_DIR] = self.auxDir


    def __setupLogging(self, config):
        self.__logFile = join(self.auxDir, 'service.log')

        if exists(self.__logFile):
            os.remove(self.__logFile)

        rootLogger = logging.getLogger()
        handler = logging.FileHandler(filename=self.__logFile, mode='a', delay=False)
        handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
        rootLogger.addHandler(handler)
        rootLogger.setLevel(logging._nameToLevel.get(Config.LOG_LEVEL.get(config).upper()))

        logging.info('service {} version {} started {} @ {} (with tags {})'.format(Config.MANAGER_ID.get(config),
            qcg.__version__, str(datetime.now()), socket.gethostname(), ','.join(Config.MANAGER_TAGS.get(config))))
        logging.info('log level set to: {}'.format(Config.LOG_LEVEL.get(config).upper()))


    def __setupEventLoop(self):
        if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())


    @profile
    async def __stopInterfaces(self, receiver):
        while not receiver.isFinished():
            await asyncio.sleep(0.5)

        try:
            response = await receiver.generateStatusResponse()

            statusFile = Config.FINAL_STATUS_FILE.get(self.__conf)
            statusFile = statusFile if isabs(statusFile) else join(self.auxDir, statusFile)

            if exists(statusFile):
                os.remove(statusFile)

            with open(statusFile, 'w') as f:
                f.write(response.toJSON())
        except Exception as e:
            logging.warning("failed to write final status: {}".format(str(e)))

        logging.info("stopping receiver ...")
        receiver.stop()


    @profile
    def __jobNotify(self, jobId, state, manager):
        if self.__jobReportFile and self.__jobReporter:
            if state.isFinished():
                with open(self.__jobReportFile, 'a') as f:
                    job = manager.jobList.get(jobId)
                    self.__jobReporter.reportJob(job, f)

                    
    def getIfaces(self, iface_class=None):
        """
        Return list of configured interaces.

        Args:
            iface_class - class of interface, if not defined all configured interfaces are returned.
        """
        if iface_class:
            return [iface for iface in self.__ifaces if isinstance(iface, iface_class)]
        else:
            return self.__ifaces


    @profile
    def start(self):
        self.__receiver.run()

        self.__manager.setupInterfaces()

        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(self.__stopInterfaces(self.__receiver)))

        if self.__manager:
            self.__manager.stop()

        usage = self.get_rusage()
        logging.info('service resource usage: {}'.format(str(usage.get('service', {}))))
        logging.info('jobs resource usage: {}'.format(str(usage.get('jobs', {}))))

        asyncio.get_event_loop().close()


    def get_rusage(self):
        service_ru = resource.getrusage(resource.RUSAGE_SELF)
        jobs_ru = resource.getrusage(resource.RUSAGE_CHILDREN)

        return { 'service': service_ru, 'jobs': jobs_ru }


class QCGPMServiceProcess(Process):

    def __init__(self, args=[], queue=None):
        """
        Start QCGPM Service as a separate process.

        Args:
            args (str[]) - command line arguments
            queue (Queue) - the communication queue
        """
        super(QCGPMServiceProcess, self).__init__()

        self.args = args
        self.queue = queue


    def run(self):
        try:
            print('starting qcgpm service ...')
            self.service = QCGPMService(self.args)

            if self.queue:
                print('communication queue defined ...')
                zmq_ifaces = self.service.getIfaces(ZMQInterface)
                print('sending configuration through communication queue ...')
                self.queue.put({'zmq_addresses': [str(iface.real_address) for iface in zmq_ifaces]})
            else:
                print('communication queue not defined')

            print('starting qcgpm service inside process ....')
            self.service.start()
        except Exception as e:
            print('Error: %s\n' % (str(e)))
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    try:
        QCGPMService().start()
    except Exception as e:
        sys.stderr.write('Error: %s\n' % (str(e)))
        traceback.print_exc()
        sys.exit(1)
