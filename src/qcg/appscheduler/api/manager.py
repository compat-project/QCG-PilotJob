import re
import zmq
import json
import os
import time
import sys
import logging
import queue
from os.path import exists

import multiprocessing as mp

from qcg.appscheduler.api import errors


class Manager:
    DEFAULT_ADDRESS_ENV = "QCG_PM_ZMQ_ADDRESS"
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"

    DEFAULT_POLL_DELAY = 2


    def __init__(self, address = None, cfg = { }):
        """
        Create Manager object.

        Manages a single QCG-PJM connection.

        Args:
            address (str) - the address of the PJM manager to connect to in the form:
                 [proto://]host[:port]
              the default values for 'proto' and 'port' are respectively - 'tcp' and '5555'; if 'address'
              is not defined the following procedure will be performed:
                a) if the environment contains QCG_PM_ZMQ_ADDRESS - the value of this var will be used,
                  else
                b) the tcp://127.0.0.1:5555 default address will be used
            cfg (dict) - the configuration; currently the following keys are supported:
              'poll_delay' - the delay between following status polls in wait methods
              'log_file' - the location of the log file
              'log_level' - the log level ('DEBUG'); by default the log level is set to INFO
        
        """
        self.__zmqCtx = zmq.Context()
        self.__zmqSock = None
        self.__connected = False

        self.__pollDelay = Manager.DEFAULT_POLL_DELAY

        self.__setupLogging(cfg)

        if address is None:
            if Manager.DEFAULT_ADDRESS_ENV in os.environ:
                address = os.environ[Manager.DEFAULT_ADDRESS_ENV]
                logging.debug('found zmq address of pm: %s' % (address))
            else:
                address = Manager.DEFAULT_ADDRESS

        if 'poll_delay' in cfg:
            self.__pollDelay = cfg['poll_delay']

        self.__address = self.__parseAddress(address)
        self.__connect()


    def __parseAddress(self, address):
        """
        Validate the QCG PJM address.

        Args:
            address (str) - the address to validate; if address is not in the complete form, it will be
              extended with the default values (protocol, port)
        """
        if not re.match(r'\w*://', address):
            # append default protocol
            address = "%s://%s" % (Manager.DEFAULT_PROTO, address)

        if not re.match(r'.*:\d+', address):
            # append default port
            address = "%s:%s" % (address, Manager.DEFAULT_PORT)

        return address


    def __setupLogging(self, cfg):
        """
        Setup the logging.
        The log file and the log level are set.

        Args:
            cfg (dict) - see constructor.
        """
        self.__logFile = cfg.get('log_file', 'api.log')
        print('log file set to {}'.format(self.__logFile))

        if exists(self.__logFile):
            os.remove(self.__logFile)

        self.__rootLogger = logging.getLogger()
        self.__logHandler = logging.FileHandler(filename=self.__logFile, mode='a', delay=False)
        self.__logHandler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
        self.__rootLogger.addHandler(self.__logHandler)

        level = logging.INFO
        if 'log_level' in cfg:
            if cfg['log_level'].lower() == 'debug':
                level = logging.DEBUG

        self.__rootLogger.setLevel(level)


    def __disconnect(self):
        """
        Close connection to the QCG-PJM

        Raises:
            ConnectionError - if there was an error during closing the connection.
        """
        try:
            if self.__connected:
                self.__zmqSock.close()
                self.__connected = False
        except Exception as e:
            raise errors.ConnectionError('Failed to disconnect - %s' % (e.args[0]))


    def __connect(self):
        """
        Connect to the QCG-PJM
        The connection is made to the address defined in the constructor.

        Raises:
            ConnectionError - in case of error during establishing connection.
        """
        self.__disconnect()

        logging.info("connecting to the PJM @ %s" % (self.__address))
        try:
            self.__zmqSock = self.__zmqCtx.socket(zmq.REQ)
            self.__zmqSock.connect(self.__address)
            self.__connected = True
            logging.info("connection established")
        except Exception as e:
            raise errors.ConnectionError('Failed to connect to %s - %s' % (self.__address, e.args[0]))


    def __assureConnected(self):
        """
        Check if connection has been successfully opened.

        Raises:
            ConnectionError - if connection has not been established yet
        """
        if not self.__connected:
            raise errors.ConnectionError('Not connected')


    def __validateResponse(self, response):
        """
        Validate the response from the QCG PJM.
        This method checks the format of the response and exit code.

        Returns:
            dict - response data

        Raises:
            InternalError - in case the response format is invalid
            ConnectionError - in case of non zero exit code
        """
        if not isinstance(response, dict) or 'code' not in response:
            raise errors.InternalError('Invalid reply from the service')

        if response['code'] != 0:
            if 'message' in response:
                raise errors.ConnectionError('Request failed - %s' % response['message'])

            raise errors.ConnectionError('Request failed')

        if 'data' not in response:
            raise errors.InternalError('Invalid reply from the service')

        return response['data']


    def __sendAndValidateResult(self, data):
        """
        Send syncronically request to the QCG-PJM and validate response
        The input data is encoded in the JSON format and send to the QCG PJM. After receiving response,
        it is validated.

        Args:
            data - data to send as a JSON document

        Returns:
            data - received data from the QCG PJM service

        Raises:
            see __assureConnected, __validateResponse
        """
        self.__assureConnected()

        msg = str.encode(json.dumps( data ))
        logging.debug("sending (in process {}): {}".format(os.getpid(), msg))
        self.__zmqSock.send(msg)
        logging.debug("data send, waiting for response")

        reply = bytes.decode(self.__zmqSock.recv())

        logging.debug("got reply: %s" % reply)
        return self.__validateResponse(json.loads(reply))


    def resources(self):
        """
        Return available resources.
        Return information about current resource status of QCG PJM.

        Returns:
            data - data in format described in 'resourceInfo' method of QCG PJM.

        Raises:
            see __sendAndValidateResult
        """
        return self.__sendAndValidateResult({
            "request": "resourcesInfo"
        })


    def submit(self, jobs):
        """
        Submit a jobs.
        
        Args:
            jobs (Jobs) - a job description list
            
        Returns:
            list - a list of submitted job names

        Raises:
            InternalError - in case of unexpected result format
            see __sendAndValidateResult
        """
        data = self.__sendAndValidateResult({
            "request": "submit",
            "jobs": jobs.orderedJobs()
        })

        if 'submitted' not in data or 'jobs' not in data:
            raise errors.InternalError('Missing response data')

        return data['jobs']


    def list(self):
        """
        List all the jobs.
        Return a list of all job names along with their status and additional data currently registered
        in the QCG PJM.

        Returns:
            list - list of jobs with additional data in format described in 'listJobs' method in QCG PJM.

        Raises:
            InternalError - in case of unexpected result format
            see __sendAndValidateResult
        """
        data = self.__sendAndValidateResult({
            "request": "listJobs"
        })

        if 'jobs' not in data:
            raise errors.InternalError('Rquest failed - missing jobs data')

        return data['jobs']
#        jobs = {}
#        for job in data['jobs']:
#            jMessages = None
#            inQueue = sys.maxsize
#
#            # optional
#            if 'messages' in job:
#                jMessages = job['messages']
#
#            # optional
#            if 'inQueue' in job:
#                inQueue = job['inQueue']
#
#            jobs[job['name']] = {
#                'name': job['name'],
#                'status': job['status'],
#                'messages': jMessages,
#                'inQueue': inQueue
#            }
#
#        return StatusResult(jobs)


    def status(self, names):
        """
        Return current status of jobs.
        
        Args:
            names (list, str) - list of job names

        Returns:
            list - a list of job's status in the format described in 'jobStatus' method of QCG PJM.

        Raises:
            see __sendAndValidateResult
        """
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        return self.__sendAndValidateResult({
            "request": "jobStatus",
            "jobNames": jNames
        })


    def info(self, names):
        """
        Return detailed information about jobs.

        Args:
            names (list, str) - a list of job names

        Returns:
            list - a list of job's detailed information in the format described in 'jobStatus' method of 
              QCG PJM.

        Raises:
            see __sendAndValidateResult
        """
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        return self.__sendAndValidateResult({
            "request": "jobInfo",
            "jobNames": jNames
        })


    def remove(self, names):
        """
        Remove jobs from registry.

        Args:
            names (list, str) - a list of job names

        Raises:
            see __sendAndValidateResult
        """
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        self.__sendAndValidateResult({
            "request": "removeJob",
            "jobNames": jNames
        })


    def cancel(self, names):
        """
        Cancel job.
        Method currenlty not supported.
        """
        raise errors.InternalError('Request not implemented')


    def finish(self):
        """
        Finish QCG PJM and disconnect.

        Raises:
            see __sendAndValidateResult, __disconnect
        """
        self.__assureConnected()

        self.__zmqSock.send(str.encode(json.dumps({
            "request": "finish"
        })))

        reply = bytes.decode(self.__zmqSock.recv())
        self.__validateResponse(json.loads(reply))

        self.__disconnect()


    def cleanup(self):
        """
        Clean up.
        """
        if self.__logHandler:
            self.__logHandler.close()
            self.__rootLogger.removeHandler(self.__logHandler)


    def wait4(self, names):
        """
        Wait for finish of specific jobs.
        This method waits until all specified jobs finish its execution (successfully or not).
        The QCG PJM is periodically polled about status of not finished jobs. The poll interval (2 sec by
        default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
        configuration of constructor.

        Args:
            names (list, str) - a list of job names

        Returns:
            dict - a map with job names and their terminal status

        Raises:
            InternalError - in case of unexpected response
            ConnectionError - in case of connection problems
            see status
        """
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        logging.info("waiting for finish of %d jobs" % len(jNames))

        result = { }
        notFinished = jNames
        while len(notFinished) > 0:
            try:
                cStatus = self.status(notFinished)

                notFinished = [ ]
                for jobName, jobData in cStatus['jobs'].items():
                    if 'status' not in jobData['data'] or jobData['status'] != 0 or 'data' not in jobData:
                        raise errors.InternalError("Missing job's %s data" % (jobName))

                    if not self.isStatusFinished(jobData['data']['status']):
                        notFinished.append(jobName)
                    else:
                        result[jobName] = jobData['data']['status']

                if len(notFinished) > 0:
                    logging.info("still %d jobs not finished" % len(notFinished))
                    time.sleep(self.__pollDelay)

            except Exception as e:
                raise errors.ConnectionError(e.args[0])

        logging.info("all jobs finished")

        return result



    def wait4all(self):
        """
        Wait for finish of all submited jobs.
        This method waits until all specified jobs finish its execution (successfully or not).
        See 'wait4'.
        """
        self.wait4(self.list().keys())


    def isStatusFinished(self, status):
        """
        Check if status of a job is a terminal status.

        Args:
            status (str) - a job status

        Returns:
            bool - true if a given status is a terminal status, false elsewhere.
        """
        return status in [ 'SUCCEED', 'FAILED', 'CANCELED', 'OMITTED' ]



class LocalManager(Manager):

    def __init__(self, server_args = [], cfg = {}):
        if not mp.get_context():
            mp.set_start_method('fork')

        try:
            from qcg.appscheduler.service import QCGPMServiceProcess
        except ImportError:
            raise errors.ServiceError('qcg.appscheduler library is not available')

        if not server_args:
            server_args = ['--net']
        elif not '--net' in server_args:
            server_args.append('--net')

        self.qcgpm_queue = mp.Queue()
        self.qcgpm_process = QCGPMServiceProcess(server_args, self.qcgpm_queue)
        print('manager process created')

        self.qcgpm_process.start()
        print('manager process started')

        try:
            self.qcgpm_conf = self.qcgpm_queue.get(block=True, timeout=2)
        except queue.Empty:
            raise errors.ServiceError('Service not started')

        print('got manager configuration: {}'.format(str(self.qcgpm_conf)))
        if not self.qcgpm_conf.get('zmq_addresses', None):
            raise errors.ConnectionError('Missing QCGPM network interface address')

        zmq_iface_address = self.qcgpm_conf['zmq_addresses'][0]
        print('manager zmq iface address: {}'.format(zmq_iface_address))

        super(LocalManager, self).__init__(zmq_iface_address, cfg)


    def wait4ManagerFinish(self):
        self.qcgpm_process.join()


    def stopManager(self):
        self.qcgpm_process.terminate()

