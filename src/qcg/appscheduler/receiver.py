import asyncio
import logging
from asyncio import CancelledError
from enum import Enum

from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.request import ListJobsReq, ResourcesInfoReq, FinishReq
from qcg.appscheduler.request import RemoveJobReq, ControlReq
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, JobInfoReq, CancelJobReq
from qcg.appscheduler.response import Response, ResponseCode


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

    def __init__(self, manager, ifaces):
        """
        Receiver listen on interfaces, valide requests and pass them to manager.
        Some requests, e.g. job status and some statistics may be handled without
        communication with manager.

        Args:
            manager (Manager): jobs manager
            ifaces (Interface[]): list of interfaces
        """
        assert ifaces is not None and isinstance(ifaces, list) and len(ifaces) > 0

        self.__manager = manager
        self.__ifaces = ifaces
        self.__tasks = []
        self.__reqEnv = {
            'resources': manager.resources
        }
        self.__handlers = {
            ControlReq: self.__handleControlReq,
            SubmitReq: self.__handleSubmitReq,
            JobStatusReq: self.__handleJobStatusReq,
            JobInfoReq: self.__handleJobInfoReq,
            CancelJobReq: self.__handleCancelJobReq,
            RemoveJobReq: self.__handleRemoveJobReq,
            ListJobsReq: self.__handleListJobsReq,
            ResourcesInfoReq: self.__handleResourcesInfoReq,
            FinishReq: self.__handleFinishReq
        }

        self.__finishTask = None
        self.isFinished = False


    async def __listen(self, iface):
        """
        Handling an interface.
        """
        logging.info("Listener on interface %s started" % (iface.__class__.__name__))

        while True:
            try:
                reqData = await iface.receive()

                if reqData is None:
                    # finishing listening - nothing more will come
                    logging.info("Finishing listening on interface %s due to EOD" % (iface.__class__.__name__))
                    return

                logging.info("Interface %s received request: %s" % ((iface.__class__.__name__), str(reqData)))

                # validate request
                validResp = self.__validate(reqData)

                logging.info("validate result: result(%s), errorMessage(%s), request(%s)" %
                             (str(validResp.result), str(validResp.errorMessage), str(validResp.request)))

                logging.info("validate error ? %s" % (str(validResp.isError())))

                if validResp.isError():
                    response = Response.Error(validResp.errorMessage)
                else:
                    response = await self.__handleRequest(iface, validResp.request)

                logging.info("got response: %s" % (str(response.toDict())))
                await iface.reply(response.toJSON())
            except CancelledError:
                # listener was canceled - finished gracefully
                logging.info("Finishing listening on interface %s due to interrupt" % (iface.__class__.__name__))
                return
            except Exception as e:
                logging.exception("Failed to process request from interface %s" % (iface.__class__.__name__))


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
            logging.error("Failed to handle request: unknown request class '%s'" %
                          (request.__class__.__name__))
            return Response.Error('Unknown request type')
        else:
            try:
                return await self.__handlers[request.__class__](iface, request)
            except:
                logging.exception('Failed to process request')
                return Response.Error('Failed to process request')


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
        logging.info("Validating request")

        response = ValidateResponse()
        req = None

        try:
            logging.info("Parsing request")
            req = Request.Parse(reqData, self.__reqEnv)
            logging.info("Request successfully parsed")
            response.success(req)
        except InvalidRequest as e:
            logging.error("Failed to parse request - invalid request")
            response.error('Invalid request: %s' % (e.args[0]))
        except Exception:
            logging.exception("Failed to parse request")
            response.error('Invalid request')

        logging.info("Validated request with result: %s" % (response.result))

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
                logging.info("Starting listener on interface %s" % (iface.__class__.__name__))
                task = asyncio.ensure_future(self.__listen(iface))
            except Exception as e:
                logging.exception("Failed to start listener for %s interface" % (iface.__class__.__name__))
                task = None

            if task is not None:
                self.__tasks.append(task)

        logging.info("Successfully initialized %d interfaces" % (len(self.__tasks)))


    def stop(self):
        """
        Stop all listening on interfaces.
        """
        logging.info("canceling %d listener tasks" % len(self.__tasks))
        for task in self.__tasks:
            if task is not None:
                logging.info("canceling listener task")
                task.cancel()

        self.__tasks = []


    async def __handleControlReq(self, iface, request):
        """
        Handlder for control commands.
        Control commands are used to configure system during run-time.

        Args:
            iface (Interface): interface which received request
            request (ControlReq): control request data

        Returns:
            Response: the response data
        """
        logging.info("Handling control request from %s iface" % (iface.__class__.__name__))

        if request.command == ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE:
            if self.__finishTask is not None:
                return Response.Error('Finish request already requested')

            self.__finishTask = asyncio.ensure_future(self.__waitForAllJobs())

        return Response.Ok('%s command accepted' % (request.command))


    async def __handleSubmitReq(self, iface, request):
        """
        Handlder for job submission.
        Before job will be submited the in-depth validation will be proviede, e.g.: job name
        uniqness.

        Args:
            iface (Interface): interface which received request
            request (SubmitJobReqest): submit request data

        Returns:
            Response: the response data
        """
        logging.info("Handling submit request from %s iface" % (iface.__class__.__name__))

        current_jobs = set()
        for job in request.jobs:
            # verify job name uniqness
            if self.__manager.jobList.exist(job.name) or job.name in current_jobs:
                return Response.Error('Job %s already exist' % (job.name))

            current_jobs.add(job.name)

        # enqueue job in the manager
        try:
            self.__manager.enqueue(request.jobs)
        except Exception as e:
            return Response.Error(str(e))

        jobs = len(request.jobs)
        data = {
            'submitted': jobs,
            'jobs': [ job.name for job in request.jobs ]
        }

        return Response.Ok('%d jobs submitted' % (len(request.jobs)), data = data)

    async def __handleJobStatusReq(self, iface, request):
        """
        Handler for job status checking.

        Args:
            iface (Interface): interface which received request
            request (JobStatusReq): job status request

        Returns:
            Response: the response data
        """
        logging.info("Handling job status request from %s iface" % (iface.__class__.__name__))

        result = { }

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    return Response.Error('Job %s doesn\'t exist' % (request.jobName))

                result[jobName] = { 'status': int(ResponseCode.OK), 'data': {
                    'jobName': jobName,
                    'status': str(job.strState())
                } }
            except Exception as e:
                result[jobName] = { 'status': int(ResponseCode.ERROR), 'message': e.args[0] }

        return Response.Ok(data = { 'jobs': result })


    async def __handleJobInfoReq(self, iface, request):
        """
        Handler for job info checking.

        Args:
            iface (Interface): interface which received request
            request (JobInfoReq): job status request

        Returns:
            Response: the response data
        """
        logging.info("Handling job info request from %s iface" % (iface.__class__.__name__))

        result = { }

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    return Response.Error('Job %s doesn\'t exist' % (request.jobName))

                jobData = {
                    'jobName': jobName,
                    'status': str(job.strState())
                }

                if job.messages is not None:
                    jobData['messages'] = job.messages

                if job.runtime is not None and len(job.runtime) > 0:
                    jobData['runtime'] = job.runtime

                if job.history is not None and len(job.history) > 0:
                    history_str = ''

                    for entry in job.history:
                        history_str = '\n'.join([history_str, "%s: %s" % (str(entry[1]), entry[0].name)])

                    jobData['history'] = history_str

                result[jobName] = { 'status': int(ResponseCode.OK), 'data': jobData }
            except Exception as e:
                result[jobName] = { 'status': int(ResponseCode.ERROR), 'message': e.args[0] }

        return Response.Ok(data = { 'jobs': result })


    async def __handleCancelJobReq(self, iface, request):
        logging.info("Handling cancel job from %s iface" % (iface.__class__.__name__))

        job = self.__manager.jobList.get(request.jobName)

        if job is None:
            return Response.Error('Job %s doesn\'t exist' % (request.jobName))

        return Response.Error('Cancel job is not supported')

    async def __handleRemoveJobReq(self, iface, request):
        logging.info("Handling remove jobs from %s iface" % (iface.__class__.__name__))

        removed = 0
        errors = { }

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    raise InvalidRequest('Job %s doesn\'t exist' % (jobName))

                if not job.state.isFinished():
                    raise InvalidRequest('Job %s not finished - can not be removed' % (jobName))

                self.__manager.jobList.remove(jobName)
                removed += 1
            except Exception as e:
                errors[jobName] = e.args[0]

        data = {
            'removed': removed,
        }

        if len(errors) > 0:
            data['errors'] = errors

        return Response.Ok(data = data)


    async def __handleListJobsReq(self, iface, request):
        logging.info("Handling list jobs info from %s iface" % (iface.__class__.__name__))

        jobNames = self.__manager.jobList.jobs()

        logging.info("got %s jobs from list" % (str(len(jobNames))))

        jobs = { }
        for jobName in jobNames:
            job = self.__manager.jobList.get(jobName)

            if job is None:
                return Response.Error('One of the job %s doesn\'t exist in registry' % (jobName))

            jobData = {
                'status': str(job.strState())
            }

            if job.messages is not None:
                jobData['messages'] = job.messages

            if job.getQueuePos() is not None:
                jobData['inQueue'] = job.getQueuePos()

            jobs[jobName] = jobData
        return Response.Ok(data={
            'length': len(jobNames),
            'jobs': jobs,
        })


    async def __handleResourcesInfoReq(self, iface, request):
        logging.info("Handling resources info from %s iface" % (iface.__class__.__name__))

        resources = self.__manager.resources
        return Response.Ok(data={
            'totalNodes': len(resources.nodes),
            'totalCores': resources.totalCores,
            'usedCores': resources.usedCores,
            'freeCores': resources.freeCores
        })

    async def __handleFinishReq(self, iface, request):
        logging.info("Handling finish service from %s iface" % (iface.__class__.__name__))
        delay = 2

        resources = self.__manager.resources

        if self.__finishTask is not None:
            return Response.Error('Finish request already requested')

        self.__finishTask = asyncio.ensure_future(self.__delayedFinish(delay))

        return Response.Ok(data={
            'when': '%ds' % delay,
        })


    async def __waitForAllJobs(self):
        logging.info("waiting for all jobs to finish (the new method)")

        while not self.__manager.allJobsFinished():
            await asyncio.sleep(0.2)

        self.isFinished = True



    async def __waitForAllJobsOriginal(self):
        logging.info("waiting for all jobs to finish (the old method)")

        notFinished = 1

        while notFinished > 0:
            notFinished = 0

            await asyncio.sleep(1)

            allJobs = self.__manager.jobList.jobs()

            for jName in allJobs:
                job = self.__manager.jobList.get(jName)

                if not job.state.isFinished():
                    logging.info("%s not finished yet" % (jName))
                    notFinished += 1;

            logging.info("%d/%d jobs not finished" % (notFinished, len(allJobs)))

        logging.info("all jobs finished")

        self.isFinished = True

    async def __delayedFinish(self, delay):
        logging.info("finishing in %s seconds" % delay)

        await asyncio.sleep(delay)
        self.isFinished = True
