import asyncio
import logging
import uuid
from datetime import datetime
import getpass
import socket
import os
import sys
import zmq
import json
import traceback
from string import Template

from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec, IllegalJobDescription
from qcg.appscheduler.errors import GovernorConnectionError, JobAlreadyExist
from qcg.appscheduler.executor import Executor
from qcg.appscheduler.joblist import JobList, JobState
from qcg.appscheduler.scheduler import Scheduler
import qcg.appscheduler.profile
from qcg.appscheduler.config import Config
from qcg.appscheduler.parseres import get_resources
from qcg.appscheduler.request import ControlReq
from qcg.appscheduler.request import JobStatusReq, JobInfoReq
from qcg.appscheduler.response import Response, ResponseCode
from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.iterscheduler import IterScheduler
from qcg.appscheduler.joblist import Job


class SchedulingJob:
    def __init__(self, manager, job):
        """
        Data necessary for scheduling job.
        """
        assert job is not None
        assert manager is not None

        self.__manager = manager
        self.job = job

        self.__isFeasible = True
        self.__afterJobs = set()

        if job.hasDependencies():
            for jobId in job.dependencies.after:
                if not self.__manager.jobList.exist(jobId):
                    raise IllegalJobDescription("Job {} - dependency job {} not registered".format(job.name, jobId))

                self.__afterJobs.add(jobId)

            self.checkDependencies()


    def checkDependencies(self):
        """
        Update dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        """
#        logging.info("updating dependencies of job %s ..." % self.job.name)

        if not self.isReady:
            finished = set()

            for jobId in self.__afterJobs:
                depJob = self.__manager.jobList.get(jobId)

                if depJob is None:
                    logging.warning("Dependency job %s not registered" % jobId)
                    self.__isFeasible = False
                    break

                if depJob.state.isFinished():
                    if depJob.state != JobState.SUCCEED:
                        self.__isFeasible = False
                        break
                    else:
                        finished.add(jobId)

            self.__afterJobs -= finished

            logging.debug("#{} dependency ({} feasible) jobs after update of job {}".format(
                len(self.__afterJobs), str(self.__isFeasible), self.job.name))


    @property
    def isFeasible(self):
        """
        Check if job can be executed.
        Job that dependency will never be satisfied (dependent jobs failed) should never be run.

        Returns:
            bool: does the job can be run in future
        """
        return self.__isFeasible


    @property
    def isReady(self):
        """
        Check if job can be scheduled and executed.
        Jobs with not met dependencies can not be scheduled.

        Returns:
            bool: is job ready for scheduling and executing
        """
        return len(self.__afterJobs) == 0


class JobStateCB:

    def __init__(self, cb, *args):
        self.cb = cb
        self.args = args


class DirectManager:

    def __init__(self, config={}, parentManager=None):
        """
        Manager of jobs to execution.
        The incoming jobs are scheduled and executed.

        Args:
            config - configuration
            receiver - the receiver class that contains information about interfaces
        """
        self.resources = get_resources(config)

        if Config.SYSTEM_CORE.get(config):
            self.resources.allocate4System()

        logging.info('available resources: {}'.format(self.resources))

        self.__executor = Executor(self, config, self.resources)
        self.__scheduler = Scheduler(self.resources)
        self.jobList = JobList()

        self.__scheduleQueue = []

        self.__jobStatesCbs = {}

        self.__receiver = None
        self.zmq_address = None

        self.managerId = Config.MANAGER_ID.get(config)
        self.managerTags = Config.MANAGER_TAGS.get(config)

        self.__parentManager = parentManager


    def setupInterfaces(self):
        """
        Initialize manager after all incoming interfaces has been started.
        """
        if self.__parentManager:
            try:
                self.registerInParent()
                self.registerNotifier(self.__notifyParentWithJob)
            except:
                logging.error('Failed to register manager in parent governor manager: {}'.format(sys.exc_info()[0]))
                raise


    def setZmqAddress(self, zmq_address):
        self.zmq_address = zmq_address


    def getHandlerInstance(self):
        return DirectManagerHandler(self)

    def stop(self):
        if self.__executor:
            self.__executor.stop()


    def allJobsFinished(self):
        return len(self.__scheduleQueue) == 0 and self.__executor.allJobsFinished()


    @profile
    def __scheduleLoop(self):
        """
        Do schedule loop.
        Get jobs from schedule queue, check if they have workflow dependency meet and if yes,
        try to create allocation. The allocated job's are sent to executor.
        """
        newScheduleQueue = []

        logging.debug("scheduling loop with %d jobs in queue" % (len(self.__scheduleQueue)))

        for idx, schedJob in enumerate(self.__scheduleQueue):
            if not self.resources.freeCores:
                newScheduleQueue.extend(self.__scheduleQueue[idx:])
                break

            schedJob.checkDependencies()

            if not schedJob.isFeasible:
                # job will never be ready
                logging.debug("job %s not feasible - omitting" % (schedJob.job.name))
                self.__changeJobState(schedJob.job, JobState.OMITTED)
                schedJob.job.clearQueuePos()
            else:
                if schedJob.isReady:
                    logging.debug("job %s is ready" % (schedJob.job.name))
                    # job is ready - try to find resources
                    try:
                        allocation = self.__scheduler.allocateJob(schedJob.job.resources)

                        if allocation is not None:
                            schedJob.job.clearQueuePos()

                            logging.debug("found resources for job %s" % (schedJob.job.name))

                            # allocation has been created - execute job
                            self.__changeJobState(schedJob.job, JobState.SCHEDULED)

                            asyncio.ensure_future(self.__executor.execute(allocation, schedJob.job))
                        else:
                            logging.debug("missing resources for job %s" % (schedJob.job.name))
                            # missing resources
                            self.__appendToScheduleQueue(newScheduleQueue, schedJob)
                    except (NotSufficientResources, InvalidResourceSpec) as e:
                        # jobs will never schedule
                        logging.warning("Job %s scheduling failed - %s" % (schedJob.job.name, str(e)))
                        self.__changeJobState(schedJob.job, JobState.FAILED, str(e))
                        schedJob.job.clearQueuePos()
                else:
                    self.__appendToScheduleQueue(newScheduleQueue, schedJob)

        self.__scheduleQueue = newScheduleQueue


    def __changeJobState(self, job, state, errorMsg=None):
        """
        Invoked to change job status.
        Any notification should be called from this method.

        Args:
            job (Job): job that changed status
            status (JobState): target job state
        """
        job.state = state

        if errorMsg is not None:
            job.appendMessage(errorMsg)

        self.__fireJobStateNotifies(job.name, state)


    def jobExecuting(self, job):
        """
        Invoked to signal starting job execution.

        Args:
            job (Job): job that started executing
        """
        self.__changeJobState(job, JobState.EXECUTING)


    def jobFinished(self, job, allocation, exitCode, errorMsg):
        """
        Invoked to signal job finished.
        Allocation made for the job should be released.

        Args:
            job (Job): job that finished
            allocation (Allocation): allocation created for the job
            exitCode (int): job exit code
            errorMsg (str): an optional error message
        """
        state = JobState.SUCCEED

        if exitCode != 0:
            state = JobState.FAILED

        self.__changeJobState(job, state, errorMsg)
        self.__scheduler.releaseAllocation(allocation)
        self.__scheduleLoop()


    def __fireJobStateNotifies(self, jobId, state):
        """
        Create task with callback functions call registered for job state changes.
        A new asyncio task is created which call all registered callbacks in not defined order.

        Args:
            jobId (str): job identifier
            state (JobState): new job status
        """
        if len(self.__jobStatesCbs) > 0:
            logging.debug("notifies callbacks about %s job status change %s" % (jobId, state))
            self.__processTask = asyncio.ensure_future(self.__callCallbacks(
                jobId, state, self.__jobStatesCbs.values()
            ))


    async def __callCallbacks(self, jobId, state, cbs):
        """
        Call job state change callback function with given arguments.

        Args:
            jobId (str): job identifier
            state (JobState): new job status
            cbs ([]function): callback functions
        """
        if cbs is not None:
            for cb in cbs:
                try:
                    cb.cb(jobId, state, *cb.args)
                except Exception as e:
                    logging.exception("Callback function failed: %s" % (str(e)))


    def unregisterNotifier(self, id):
        """
        Unregister callback function for job state changes.

        Args:
            id (str): the callback function identifier returned by 'registerNotifier' function

        Returns:
            bool: true if function unregistered successfully, and false if given identifier
               has not been found
        """
        if id in self.__jobStatesCbs:
            del self.__jobStatesCbs[id]
            return True

        return False


    def registerNotifier(self, jobStateCb, *args):
        """
        Register callback function for job state changes.
        The registered function will be called for all job state changes.

        Args:
            jobStateCb (function): should accept two arguments - job name and new state

        Returns:
            str: identifier of registered callback, which can be used to unregister
              callback or None if callback function is missing or is invalid
        """
        if jobStateCb is not None:
            id = uuid.uuid4()
            self.__jobStatesCbs[id] = JobStateCB(jobStateCb, *args)

            return id

        return None


    def enqueue(self, jobs):
        """
        Enqueue job to execution.

        Args:
            job (Job): job description

        Raises:
            JobAllreadyExist: when job with the same name was enqued earlier.
        """
        if jobs is not None:
            for job in jobs:
                self.jobList.add(job)
                self.__appendToScheduleQueue(self.__scheduleQueue, SchedulingJob(self, job))

            self.__scheduleLoop()


    def __appendToScheduleQueue(self, queue, schedJob):
        queue.append(schedJob)
        schedJob.job.setQueuePos(len(queue) - 1)


    def getExecutor(self):
        return self.__executor


    def __getParentSocket(self):
        parentSocket = zmq.asyncio.Context.instance().socket(zmq.REQ)
        parentSocket.connect(self.__parentManager)

        return parentSocket


    async def __sendParentRequestWithValidAsync(self, request):
        out_socket = None

        try:
            out_socket = self.__getParentSocket()

            await out_socket.send_json(request)
            msg = await out_socket.recv_json()
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(msg.get('message', '')))

            return msg
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    # ignore errors during cleanup
                    pass


    def __sendParentRequestWithValidSync(self, request):
        out_socket = None

        try:
            out_socket = self.__getParentSocket()

            asyncio.get_event_loop().run_until_complete(out_socket.send_json(request))
            msg = asyncio.get_event_loop().run_until_complete(out_socket.recv_json())
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(msg.get('message', '')))

            return msg
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    # ignore errors during cleanup
                    pass


    def __notifyParentWithJob(self, jobId, state):
        if self.__parentManager and state.isFinished():
            try:
                asyncio.ensure_future(self.__sendParentRequestWithValidAsync({ 'request': 'notify',
                    'entity': 'job',
                    'params': {
                        'name': jobId,
                        'state': state.name
                    }}))
            except Exception:
                logging.error('failed to send job notification to the parent manager: {}'.format(sys.exc_info()))
                logging.error(traceback.format_exc())


    def registerInParent(self):
        """
        Register manager in parent governor manager.
        We are not using asynchronous method because we need to have a "instant" result if registration went OK
        (in case of problems exception is raised).
        """
        self.__sendParentRequestWithValidSync({ 'request': 'register',
                  'entity': 'manager',
                  'params': {
                    'id': self.managerId,
                    'address': self.zmq_address,
                    'resources': self.resources.toDict(),
                    'tags': self.managerTags,
                  }
                })


class DirectManagerHandler:

    def __init__(self, manager):
        """
        Direct execution mode handler for manager.
        In this mode, the manager will try to execute all incoming tasks on available resources,
        without submiting them to other managers. Also, if defined, the notifications about
        tasks completion will be sent to the parent manager (the managers governor).

        :param manager: the manager class
        """
        self.__manager = manager

        self.__finishTask = None
        self.__receiver = None

        self.startTime = datetime.now()


    def setReceiver(self, receiver):
        self.__receiver = receiver
        if self.__receiver:
            self.__manager.setZmqAddress(self.__receiver.getZmqAddress())


    async def handleRegisterReq(self, iface, request):
        return Response.Error('Register manager request not supported in this kind of manager (direct)')


    async def handleControlReq(self, iface, request):
        """
        Handlder for control commands.
        Control commands are used to configure system during run-time.

        Args:
            iface (Interface): interface which received request
            request (ControlReq): control request data

        Returns:
            Response: the response data
        """
        if request.command == ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE:
            if self.__finishTask is not None:
                return Response.Error('Finish request already requested')

            self.__finishTask = asyncio.ensure_future(self.__waitForAllJobs())

        return Response.Ok('%s command accepted' % (request.command))


    async def handleSubmitReq(self, iface, request):
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
       # enqueue job in the manager
        try:
            jobs = self.__prepareJobs(request.jobReqs)
            self.__manager.enqueue(jobs)

            data = {
                'submitted': len(jobs),
                'jobs': [job.name for job in jobs]
            }

            return Response.Ok('{} jobs submitted'.format(len(jobs)), data=data)
        except Exception as e:
            logging.error('Submit error: {}'.format(sys.exc_info()))
            logging.error(traceback.format_exc())
            return Response.Error(str(e))


    def __prepareJobs(self, reqJobs):
        resources = self.__manager.resources

        req_job_names = set()

        newJobs = []
        for req in reqJobs:
            reqJob = req['req']
            vars = req['vars']

            haveIterations = False
            start = 0
            end = 1

            # look for 'iterate' directive
            if 'iterate' in reqJob:
                haveIterations = True

                (start, end) = reqJob['iterate'][0:2]

                vars['uniq'] = str(uuid.uuid4())
                vars['its'] = end - start
                vars['it_start'] = start
                vars['it_stop'] = end

                del reqJob['iterate']

            numCoresPlans = []
            # look for 'split-into' in resources->numCores
            if 'resources' in reqJob and 'numCores' in reqJob['resources']:
                if 'split-into' in reqJob['resources']['numCores']:
                    numCoresPlans = IterScheduler.GetScheduler('split-into').Schedule(reqJob['resources']['numCores'],
                            vars['its'], resources.totalCores)
                elif 'scheduler' in reqJob['resources']['numCores']:
                    Scheduler = IterScheduler.GetScheduler(reqJob['resources']['numCores']['scheduler'])
                    del reqJob['resources']['numCores']['scheduler']
                    numCoresPlans = Scheduler.Schedule(reqJob['resources']['numCores'],
                            vars['its'], resources.totalCores)

            numNodesPlans = []
            # look for 'split-into' in resources->numNodes
            if 'resources' in reqJob and 'numNodes' in reqJob['resources']:
                if 'split-into' in reqJob['resources']['numNodes']:
                    numNodesPlans = IterScheduler.GetScheduler('split-into').Schedule(reqJob['resources']['numNodes'],
                            vars['its'], resources.totalNodes)
                elif 'scheduler' in reqJob['resources']['numNodes']:
                    Scheduler = IterScheduler.GetScheduler(reqJob['resources']['numNodes']['scheduler'])
                    del reqJob['resources']['numNodes']['scheduler']
                    numNodesPlans = Scheduler.Schedule(reqJob['resources']['numNodes'],
                            vars['its'], resources.totalNodes)

            # default value for missing 'resources' definition
            if 'resources' not in reqJob:
                reqJob['resources'] = { 'numCores': { 'exact': 1 } }

            for idx in range(start, end):
                if haveIterations:
                    vars['it'] = idx

                try:
                    reqJob_vars = self.__replaceVariables(reqJob, vars)

                    varsStep2 = {
                        'jname': reqJob_vars['name']
                    }

                    reqJob_vars = self.__replaceVariables(reqJob_vars, varsStep2)

                    if numCoresPlans is not None and len(numCoresPlans) > idx - start:
                        reqJob_vars['resources']['numCores'] = numCoresPlans[idx - start]

                    if numNodesPlans is not None and len(numNodesPlans) > idx - start:
                        reqJob_vars['resources']['numNodes'] = numNodesPlans[idx - start]

                    # verify job name uniqness
                    if self.__manager.jobList.exist(reqJob_vars['name']) or \
                            reqJob_vars['name'] in req_job_names:
                        raise JobAlreadyExist('Job {} already exist'.format(reqJob_vars['name']))

                    newJobs.append(Job(**reqJob_vars))
                    req_job_names.add(reqJob_vars['name'])
                except JobAlreadyExist as e:
                    raise e
                except Exception as e:
                    logging.exception('Wrong submit request')
                    raise InvalidRequest('Wrong submit request - problem with variables') from e

        return newJobs


    def __replaceVariables(self, data, vars):
        if vars is not None and len(vars) > 0:
            return json.loads(Template(json.dumps(data)).safe_substitute(vars))
        else:
            return data


    async def handleJobStatusReq(self, iface, request):
        """
        Handler for job status checking.

        Args:
            iface (Interface): interface which received request
            request (JobStatusReq): job status request

        Returns:
            Response: the response data
        """
        result = {}

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    return Response.Error('Job %s doesn\'t exist' % (request.jobName))

                result[jobName] = {'status': int(ResponseCode.OK), 'data': {
                    'jobName': jobName,
                    'status': str(job.strState())
                }}
            except Exception as e:
                result[jobName] = {'status': int(ResponseCode.ERROR), 'message': e.args[0]}

        return Response.Ok(data={'jobs': result})


    async def handleJobInfoReq(self, iface, request):
        """
        Handler for job info checking.

        Args:
            iface (Interface): interface which received request
            request (JobInfoReq): job status request

        Returns:
            Response: the response data
        """
        result = {}

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    return Response.Error('Job {} doesn\'t exist'.format(request.jobName))

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

                result[jobName] = {'status': int(ResponseCode.OK), 'data': jobData}
            except Exception as e:
                result[jobName] = {'status': int(ResponseCode.ERROR), 'message': e.args[0]}

        return Response.Ok(data={'jobs': result})


    async def handleCancelJobReq(self, iface, request):
        job = self.__manager.jobList.get(request.jobName)

        if job is None:
            return Response.Error('Job %s doesn\'t exist' % (request.jobName))

        return Response.Error('Cancel job is not supported')


    async def handleRemoveJobReq(self, iface, request):
        removed = 0
        errors = {}

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

        return Response.Ok(data=data)


    async def handleListJobsReq(self, iface, request):
        job_names = self.__manager.jobList.jobs()

        logging.info("got %s jobs from list" % (str(len(job_names))))

        jobs = {}
        for jobName in job_names:
            job = self.__manager.jobList.get(jobName)

            if job is None:
                return Response.Error('One of the job %s doesn\'t exist in registry' % (jobName))

            job_data = {
                'status': str(job.strState())
            }

            if job.messages is not None:
                job_data['messages'] = job.messages

            if job.getQueuePos() is not None:
                job_data['inQueue'] = job.getQueuePos()

            jobs[jobName] = job_data
        return Response.Ok(data={
            'length': len(job_names),
            'jobs': jobs,
        })

    async def handleResourcesInfoReq(self, iface, request):
        resources = self.__manager.resources
        return Response.Ok(data={
            'totalNodes': len(resources.nodes),
            'totalCores': resources.totalCores,
            'usedCores': resources.usedCores,
            'freeCores': resources.freeCores
        })

    async def handleFinishReq(self, iface, request):
        delay = 2

        if self.__finishTask is not None:
            return Response.Error('Finish request already requested')

        self.__finishTask = asyncio.ensure_future(self.__delayedFinish(delay))

        return Response.Ok(data={
            'when': '%ds' % delay
        })


    async def generateStatusResponse(self):
        nSchedulingJobs = nFailedJobs = nFinishedJobs = nExecutingJobs = 0
        jobNames = self.__manager.jobList.jobs()
        for jobName in jobNames:
            job = self.__manager.jobList.get(jobName)

            if job is None:
                logging.warning('missing job\'s {} data '.format(jobName))
            else:
                if job.state in [JobState.QUEUED, JobState.SCHEDULED]:
                    nSchedulingJobs += 1
                elif job.state in [JobState.EXECUTING]:
                    nExecutingJobs += 1
                elif job.state in [JobState.FAILED, JobState.OMITTED]:
                    nFailedJobs += 1
                elif job.state in [JobState.CANCELED, JobState.SUCCEED]:
                    nFinishedJobs += 1

        resources = self.__manager.resources
        return Response.Ok(data={
            'System': {
                'Uptime': str(datetime.now() - self.startTime),
                'Zmqaddress': self.__receiver.getZmqAddress(),
                'Ifaces': [iface.name() for iface in self.__receiver.getInterfaces()] \
                    if self.__receiver and self.__receiver.getInterfaces() else [],
                'Host': socket.gethostname(),
                'Account': getpass.getuser(),
                'Wd': os.getcwd(),
                'PythonVersion': sys.version.replace('\n', ' '),
                'Python': sys.executable,
                'Platform': sys.platform,
            }, 'Resources': {
                'TotalNodes': len(resources.nodes),
                'TotalCores': resources.totalCores,
                'UsedCores': resources.usedCores,
                'FreeCores': resources.freeCores,
            }, 'JobStats': {
                'TotalJobs': len(jobNames),
                'InScheduleJobs': nSchedulingJobs,
                'FailedJobs': nFailedJobs,
                'FinishedJobs': nFinishedJobs,
                'ExecutingJobs': nExecutingJobs,
            }})


    async def handleStatusReq(self, iface, request):
        return await self.generateStatusResponse()


    async def handleNotifyReq(self, iface, request):
        return Response.Error('Operation not supported')


    async def __waitForAllJobs(self):
        logging.info("waiting for all jobs to finish (the new method)")

        while not self.__manager.allJobsFinished():
            await asyncio.sleep(0.2)

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.warning('Failed to set finish flag due to lack of receiver access')


    async def __delayedFinish(self, delay):
        logging.info("finishing in %s seconds" % delay)

        await asyncio.sleep(delay)

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.warning('Failed to set finish flag due to lack of receiver access')
