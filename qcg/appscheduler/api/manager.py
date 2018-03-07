import re
import zmq
from zmq.asyncio import Context
import json
import os
from qcg.appscheduler.api import errors
import sys

from qcg.appscheduler.api.results import StatusResult


class Manager:
    """
    Manages a single QCG-PJM connection.

    Attributes:
        address (str) - address of the QCG-PJM in form of: host[:port]
    """

    DEFAULT_ADDRESS_ENV = "QCG_PJM_ADDRESS"
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"


    def __init__(self, address = None):
        self.__zmqCtx = Context.instance()
        self.__zmqSock = None
        self.__connected = False

        if address is None:
            if Manager.DEFAULT_ADDRESS_ENV in os.environ:
                address = os.environ[Manager.DEFAULT_ADDRESS_ENV]
            else:
                address = Manager.DEFAULT_ADDRESS

        self.__address = self.__parseAddress(self, address)
        self.__connect()


    def __parseAddress(self, address):
        if not re.match('\w*://', address):
            # append default protocol
            address = "%s://%s" % (Manager.DEFAULT_PROTO, address)

        if not re.match('.*:\d+', address):
            # append default port
            address = "%s:%s" % (address, Manager.DEFAULT_PORT)

        return address


    """
    Close connection to the QCG-PJM
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
    """
    def __connect(self):
        self.disconnect()

        try:
            self.__zmqSock = self.__zmqCtx.socket(zmq.REQ)
            self.__zmqSock.connect(self.__address)
            self.__connected = True
        except Exception as e:
            raise errors.ConnectionError('Failed to connect to %s - %s' % (self.__address, e.args[0]))


    """
    Check if connection has been successfully opened.
    """
    def __assureConnected(self):
        if not self.__connected:
            raise errors.ConnectionError('Not connected')


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
    """
    def __sendAndValidateResult(self, data):
        self.__assureConnected()
        self.__zmqSock.send(str.encode(json.dumps( data )))

        reply = bytes.decode(self.__zmqSock.recv())
        return self.__validateResponse(json.loads(reply))


    """
    Return available resources.
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
        SubmitResult - a list with job names along with their submission status
    """
    def submit(self, jobs):
        self.__sendAndValidateResult({
            "request": "submit",
            "jobs": jobs.getJobsFormat()
        })


    """
    List all the jobs.
    """
    def list(self):
        data = self.__sendAndValidateResult({
            "request": "listJobs"
        })

        if 'jobs' not in data:
            raise errors.InternalError('Rquest failed - missing jobs data')

        jobs = {}
        for job in data['data']['jobs']:
            jMessages = None
            inQueue = sys.maxsize

            # optional
            if 'messages' in job:
                jMessages = job['messages']

            # optional
            if 'inQueue' in job:
                inQueue = job['inQueue']

            jobs[job['name']] = {
                'name': job['name'],
                'status': job['status'],
                'messages': jMessages,
                'inQueue': inQueue
            }

        return StatusResult(jobs)


    """
    Check current status of job.
    """
    def jStatus(self, jName):
        return self.__sendAndValidateResult({
            "request": "jobStatus",
            "jobName": jName
        })


    """
    Remove job from registry.
    """
    def jDel(self, jName):
        self.__sendAndValidateResult({
            "request": "removeJob",
            "jobName": jName
        })


    """
    Cancel job.
    """
    def jCancel(self, jName):
        raise errors.InternalError('Request not implemented')


    """
    Finish job manager and disconnect.
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
    """
    def wait4(self):
        pass


    """
    Wait for finish of all submited jobs.
    """
    def wait4all(self):
        pass

