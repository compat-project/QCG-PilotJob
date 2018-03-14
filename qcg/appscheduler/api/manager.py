import re
import zmq
import json
import os
import time
import sys
import logging
from os.path import exists

from qcg.appscheduler.api import errors


class Manager:
    """
    Manages a single QCG-PJM connection.

    Attributes:
        address (str) - address of the QCG-PJM in form of: proto://host[:port]
    """
    DEFAULT_ADDRESS_ENV = "QCG_PM_ZMQ_ADDRESS"
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"

    DEFAULT_POLL_DELAY = 2


    """
    Create Manager object.

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
    def __init__(self, address = None, cfg = { }):
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


    """
    Validate the QCG PJM address.

    Args:
        address (str) - the address to validate; if address is not in the complete form, it will be
          extended with the default values (protocol, port)
    """
    def __parseAddress(self, address):
        if not re.match('\w*://', address):
            # append default protocol
            address = "%s://%s" % (Manager.DEFAULT_PROTO, address)

        if not re.match('.*:\d+', address):
            # append default port
            address = "%s:%s" % (address, Manager.DEFAULT_PORT)

        return address


    """
    Setup the logging.
    The log file and the log level are set.

    Args:
        cfg (dict) - see constructor.
    """
    def __setupLogging(self, cfg):
        self.__logFile = 'api.log'
        if 'log_file' in cfg:
            self.__logFile = cfg['log_file']

        if exists(self.__logFile):
            os.remove(self.__logFile)

        rootLogger = logging.getLogger()
        handler = logging.FileHandler(filename=self.__logFile, mode='a', delay=False)
        handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
        rootLogger.addHandler(handler)

        level = logging.INFO
        if 'log_level' in cfg:
            if cfg['log_level'].lower() == 'debug':
                level = logging.DEBUG

        rootLogger.setLevel(level)


    """
    Close connection to the QCG-PJM

    Raises:
        ConnectionError - if there was an error during closing the connection.
    """
    def __disconnect(self):
        try:
            if self.__connected:
                self.__zmqSock.close()
                self.__connected = False
        except Exception as e:
            raise errors.ConnectionError('Failed to disconnect - %s' % (e.args[0]))


    """
    Connect to the QCG-PJM
    The connection is made to the address defined in the constructor.

    Raises:
        ConnectionError - in case of error during establishing connection.
    """
    def __connect(self):
        self.__disconnect()

        logging.info("connecting to the PJM @ %s" % (self.__address))
        try:
            self.__zmqSock = self.__zmqCtx.socket(zmq.REQ)
            self.__zmqSock.connect(self.__address)
            self.__connected = True
            logging.info("connection established")
        except Exception as e:
            raise errors.ConnectionError('Failed to connect to %s - %s' % (self.__address, e.args[0]))


    """
    Check if connection has been successfully opened.

    Raises:
        ConnectionError - if connection has not been established yet
    """
    def __assureConnected(self):
        if not self.__connected:
            raise errors.ConnectionError('Not connected')


    """
    Validate the response from the QCG PJM.
    This method checks the format of the response and exit code.

    Returns:
        dict - response data

    Raises:
        InternalError - in case the response format is invalid
        ConnectionError - in case of non zero exit code
    """
    def __validateResponse(self, response):
        if not isinstance(response, dict) or 'code' not in response or 'data' not in response:
            raise errors.InternalError('Invalid reply from the service')

        if response['code'] != 0:
            if 'message' in response:
                raise errors.ConnectionError('Request failed - %s' % response['message'])

            raise errors.ConnectionError('Request failed')

        return response['data']


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
    def __sendAndValidateResult(self, data):
        self.__assureConnected()

        msg = str.encode(json.dumps( data ))
        logging.debug("snding: %s" % msg)
        self.__zmqSock.send(msg)

        reply = bytes.decode(self.__zmqSock.recv())

        logging.debug("got reply: %s" % reply)
        return self.__validateResponse(json.loads(reply))


    """
    Return available resources.
    Return information about current resource status of QCG PJM.

    Returns:
        data - data in format described in 'resourceInfo' method of QCG PJM.

    Raises:
        see __sendAndValidateResult
    """
    def resources(self):
        return self.__sendAndValidateResult({
            "request": "resourcesInfo"
        })


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
    def submit(self, jobs):
        d = jobs.formatDoc()

        data = self.__sendAndValidateResult({
            "request": "submit",
            "jobs": jobs.formatDoc()
        })

        if 'submitted' not in data or 'jobs' not in data:
            raise errors.InternalError('Missing response data')

        return data['jobs']


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
    def list(self):
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


    """
    Return current status of jobs.
    
    Args:
        names (list, str) - list of job names

    Returns:
        list - a list of job's status in the format described in 'jobStatus' method of QCG PJM.

    Raises:
        see __sendAndValidateResult
    """
    def status(self, names):
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        return self.__sendAndValidateResult({
            "request": "jobStatus",
            "jobNames": jNames
        })


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
    def info(self, names):
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        return self.__sendAndValidateResult({
            "request": "jobInfo",
            "jobNames": jNames
        })


    """
    Remove jobs from registry.

    Args:
        names (list, str) - a list of job names

    Raises:
        see __sendAndValidateResult
    """
    def remove(self, names):
        if isinstance(names, str):
            jNames = [ names ]
        else:
            jNames = list(names)

        self.__sendAndValidateResult({
            "request": "removeJob",
            "jobNames": jNames
        })


    """
    Cancel job.
    Method currenlty not supported.
    """
    def cancel(self, names):
        raise errors.InternalError('Request not implemented')


    """
    Finish QCG PJM and disconnect.

    Raises:
        see __sendAndValidateResult, __disconnect
    """
    def finish(self):
        self.__assureConnected()

        self.__zmqSock.send(str.encode(json.dumps({
            "request": "finish"
        })))

        reply = bytes.decode(self.__zmqSock.recv())
        self.__validateResponse(json.loads(reply))

        self.__disconnect()


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
    def wait4(self, names):
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



    """
    Wait for finish of all submited jobs.
    This method waits until all specified jobs finish its execution (successfully or not).
    See 'wait4'.
    """
    def wait4all(self):
        self.wait4(self.list().names())


    """
    Check if status of a job is a terminal status.

    Args:
        status (str) - a job status

    Returns:
        bool - true if a given status is a terminal status, false elsewhere.
    """
    def isStatusFinished(self, status):
        return status in [ 'SUCCEED', 'FAILED', 'CANCELED', 'OMITTED' ]
