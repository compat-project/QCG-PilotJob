import asyncio
import logging
import sys
from asyncio import CancelledError
from enum import Enum

from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.request import RegisterReq, ListJobsReq, ResourcesInfoReq, FinishReq
from qcg.appscheduler.request import RemoveJobReq, ControlReq, StatusReq, NotifyReq
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, JobInfoReq, CancelJobReq
from qcg.appscheduler.response import Response, ResponseCode
from qcg.appscheduler.zmqinterface import ZMQInterface


class ResponseStatus(Enum):
    UNKNOWN = 1
    ERROR = 2
    SUCCESS = 3


class ValidateResponse:
    def __init__(self):
        self.result = ResponseStatus.UNKNOWN
        self.errorMessage = None
        self.request = None

    def error(self, errMsg):
        self.result = ResponseStatus.ERROR
        self.request = None
        self.errorMessage = errMsg

    def success(self, request):
        self.result = ResponseStatus.SUCCESS
        self.request = request
        self.errorMessage = 'Ok'

    def isError(self):
        return self.result == ResponseStatus.ERROR

    def isSuccess(self):
        return self.result == ResponseStatus.SUCCESS


class Receiver:

    def __init__(self, requestHandler, ifaces, resources):
        """
        Receiver listen on interfaces, validate requests and pass them to request handler (manager).

        Args:
            request_handler: manager that handles requests
            ifaces (Interface[]): list of interfaces
        """
        assert ifaces is not None and isinstance(ifaces, list) and len(ifaces) > 0

        self.__handler = requestHandler

        self.__ifaces = ifaces
        self.__tasks = []
        self.__reqEnv = {
            'resources': resources
        }

        self.zmq_address = self.__findZmqAddress()

        self.__handlers = {
            RegisterReq: self.__handler.handleRegisterReq,
            ControlReq: self.__handler.handleControlReq,
            SubmitReq: self.__handler.handleSubmitReq,
            JobStatusReq: self.__handler.handleJobStatusReq,
            JobInfoReq: self.__handler.handleJobInfoReq,
            CancelJobReq: self.__handler.handleCancelJobReq,
            RemoveJobReq: self.__handler.handleRemoveJobReq,
            ListJobsReq: self.__handler.handleListJobsReq,
            ResourcesInfoReq: self.__handler.handleResourcesInfoReq,
            FinishReq: self.__handler.handleFinishReq,
            StatusReq: self.__handler.handleStatusReq,
            NotifyReq: self.__handler.handleNotifyReq,
        }

        self.finished = False

        self.__handler.setReceiver(self)

    def __findZmqAddress(self):
        zmqiface = next((iface for iface in self.__ifaces if isinstance(iface, ZMQInterface)), None)
        if zmqiface:
            return zmqiface.real_address

        return None


    def getZmqAddress(self):
        return self.zmq_address


    def getInterfaces(self):
        return self.__ifaces


    def setFinish(self, finished):
        """
        Set finish flag.
        If set to TRUE the receiver should not accept any new requests and whole service should finish.
        The service will monitor this flag to know when receiver finished accepting requests.

        :param finished: the finished flag
        """
        self.finished = finished


    def isFinished(self):
        """
        Return value of finish flag.

        :return: the value of finish flag
        """
        return self.finished


    async def __listen(self, iface):
        """
        Handling an interface.
        """
        logging.info('Listener on interface {} started'.format(iface.__class__.__name__))

        while True:
            try:
                reqData = await iface.receive()

                if reqData is None:
                    # finishing listening - nothing more will come
                    logging.info('Finishing listening on interface {} due to EOD'.format(iface.__class__.__name__))
                    return

                logging.info('Interface {} received request: {}'.format(iface.__class__.__name__, str(reqData)))

                # validate request
                validResp = self.__validate(reqData)

                if validResp.isError():
                    logging.debug('validate request failed: result({}), errorMessage({}), request({})'.format(
                        str(validResp.result), str(validResp.errorMessage), str(validResp.request)))
                    response = Response.Error(validResp.errorMessage)
                else:
                    response = await self.__handleRequest(iface, validResp.request)

                logging.info('sending response: {}'.format(str(response.toDict())))
                await iface.reply(response.toJSON())
            except CancelledError:
                # listener was canceled - finished gracefully
                logging.info('Finishing listening on interface {} due to interrupt'.format(iface.__class__.__name__))
                return
            except:
                logging.exception('Failed to process request from interface {}'.format(iface.__class__.__name__))


    async def __handleRequest(self, iface, request):
        """
        Handle single request.
        The proper handler for given request type should be found (in handlers map) and
        called.

        Args:
            iface (Interface): interface which received request
            request (Request): parsed request data

        Returns:
            Response: response that should be returned to user
        """
        if request.__class__ not in self.__handlers:
            logging.error('Failed to handle request: unknown request class "{}"'.format(request.__class__.__name__))
            return Response.Error('Unknown request type "{}"'.format(request.__class__.__name__))
        else:
            try:
                logging.info('Handling {} request from {} interface'.format(
                    request.__class__.__name__, iface.__class__.__name__))

                return await self.__handlers[request.__class__](iface, request)
            except:
                logging.exception('Failed to process request: {}'.format(sys.exc_info()[0]))
                return Response.Error('Failed to process request: {}'.format(sys.exc_info()[0]))


    def __validate(self, reqData):
        """
        Validate incoming request.
        In this first step only form of JSON format is validated along with some basic checks.
        More specific validation should be provided on further steps.

        Args:
            reqData (dict): request data

        Returns:
            ValidateResponse: validation result
        """
        response = ValidateResponse()
        req = None

        try:
            req = Request.Parse(reqData, self.__reqEnv)
            response.success(req)
        except InvalidRequest as e:
            logging.error("Failed to parse request - invalid request")
            response.error('Invalid request: %s' % (e.args[0]))
        except Exception:
            logging.exception('Failed to parse request: {}'.format(sys.exc_info()[0]))
            response.error('Invalid request: {}'.format(sys.exc_info()[0]))

#        logging.info("Validated request with result: %s" % (response.result))

        # request is OK, but we have to validate request parameters, such as:
        #	cancelJob, statusJob job names exists or submitJob job name uniqness
        return response


    def run(self):
        """
        Start listening on interfaces.
        This method creates asynchronic tasks and returns. To stop created tasks, method 'stop'
        must be called.
        """
        # are old tasks should be stoped here ?
        self.__tasks = []

        for iface in self.__ifaces:
            try:
                logging.info('Starting listener on interface {}'.format(iface.__class__.__name__))
                task = asyncio.ensure_future(self.__listen(iface))
            except Exception as e:
                logging.exception('Failed to start listener for {} interface'.format(iface.__class__.__name__))
                task = None

            if task is not None:
                self.__tasks.append(task)

        logging.info('Successfully initialized {} interfaces'.format(len(self.__tasks)))


    def stop(self):
        """
        Stop all listening on interfaces.
        """
        logging.info('canceling {} listener tasks'.format(len(self.__tasks)))
        for task in self.__tasks:
            if task is not None:
                logging.info('canceling listener task')
                try:
                    task.cancel()
                except:
                    logging.warning('failed to cancel listener task: {}'.format(sys.exc_info()[0]))

        self.__tasks = []

    def generateStatusResponse(self):
        return self.__handler.generateStatusResponse()
