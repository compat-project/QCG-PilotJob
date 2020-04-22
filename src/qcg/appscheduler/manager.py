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
from qcg.appscheduler.joblist import JobList, JobState, JobResources
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

    # how many iterations should be resolved at each scheduling step
    ITERATIONS_SPLIT = 100

    def __init__(self, manager, job):
        """
        Data necessary for scheduling job.

        Dependencies.
        The SchedulingJob contains two set of dependencies - the common for all subjobs, and specific for each
        subjob (these dependencies are stored in SchedulingIteration). The execution of individual subjob might start
        only after all common dependencies has been meet, and those specific for each subjob.
        """
        assert job is not None
        assert manager is not None

        self.__manager = manager
        self.job = job

        # does the job chance to meet dependencies
        self.__isFeasible = True

        # list of dependant jobs (without individual for each subjob) - common for all iterations
        self.__afterJobs = set()

        # list of dependant individual subjobs for each subjob - specific for each iteration
        self.__afterIterationJobs = set()

        # data of iteration subjobs
        self.__iterationSubJobs = []

        # flag for iterative jobs
        self.__hasIterations = self.job.isIterative()

        # total number of iterations
        self.__totalIterations = self.job.getIteration().iterations() if self.__hasIterations else 1

        # number of currently solved iterations
        self.__currentSolvedIterations = 0

        # general dependencies
        if job.hasDependencies():
            for jobId in job.dependencies.after:
                if '${it}' in jobId:
                    self.__afterIterationJobs.add(jobId)
                else:
                    self.__afterJobs.add(jobId)

            self.checkDependencies()

       # job resources
        # in case of job iterations, there are schedulers which generates # of cores/nodes specific for each iteration
        self.__resCoresGenerator = None
        jobres = self.job.resources
        if self.__hasIterations and jobres.hasCores() and jobres.cores.scheduler is not None:
            self.__resCoresGenerator = IterScheduler.GetScheduler(jobres.cores.scheduler['name'])(
                jobres.cores.toDict(), self.__totalIterations, self.__manager.resources.totalCores,
                **jobres.cores.scheduler.get('params', {})).generate()
            logging.debug('generated cores scheduler {} for job {}'.format(jobres.cores.scheduler['name'], self.job.name))

        self.__resNodesGenerator = None
        if self.__hasIterations and jobres.hasNodes() and jobres.nodes.scheduler is not None:
            self.__resNodesGenerator = IterScheduler.GetScheduler(jobres.nodes.scheduler["name"])(
                jobres.nodes.toDict(), self.__totalIterations, self.__manager.resources.totalNodes,
                **jobres.nodes.scheduler.get("params", {})).generate()
            logging.debug('generated nodes scheduler {} for job {}'.format(jobres.nodes.scheduler['name'], self.job.name))

        # compute minResCores
        self.__minResCores = jobres.getMinimumNumberOfCores()
        logging.debug('minimum # of cores for job {} is {}'.format(self.job.name, self.__minResCores))

        # generate only part of whole set of iterations
        self.setupIterations()


    def checkDependencies(self):
        """
        Update dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        """
        finished = set()

        # check job dependencies
        for jobId in self.__afterJobs:
            jobName = jobId
            jobIt = None

            if ":" in jobName:
                jobName, jobIt = self.__manager.jobList.parse_jobname(jobName)

            depJob = self.__manager.jobList.get(jobName)
            if depJob is None:
                logging.warning("Dependency job \'{}\' not registered".format(jobId))
                self.__isFeasible = False
                break

            depJobState = depJob.getState(jobIt)
            if depJobState.isFinished():
                if depJob.getState() != JobState.SUCCEED:
                    self.__isFeasible = False
                    break
                else:
                    finished.add(jobId)

        self.__afterJobs -= finished

        if self.__iterationSubJobs and self.__afterIterationJobs:
            # check single iteration job dependencies
            toRemove = []
            for iterationJob in self.__iterationSubJobs:
                iterationJob.checkDependencies()

                if not iterationJob.isFeasible():
                    self.__manager.__changeJobState(self.job, iteration=iterationJob.iteration,
                                                    state=JobState.OMITTED)
                    logging.debug('iteration {} not feasible - removing'.format(iterationJob.name))
                    toRemove.append(iterationJob)

            if toRemove:
                map(lambda job: self.__iterationSubJobs.remove(job), toRemove)

        logging.debug("#{} dependency ({} feasible) jobs after update of job {}".format(
            len(self.__afterJobs), str(self.__isFeasible), self.job.name))


    def setupIterations(self):
        """
        Resolve next part of iterations.
        """
        niters = min(self.__totalIterations - self.__currentSolvedIterations, SchedulingJob.ITERATIONS_SPLIT)
        logging.debug('solving {} iterations in job {}'.format(niters, self.job.name))
        for iteration in range(self.__currentSolvedIterations, self.__currentSolvedIterations + niters):
            # prepare resources, dependencies
            subJobIteration = iteration + self.job.getIteration().start if self.__hasIterations else None
            subJobResources = self.job.resources

            if self.__resCoresGenerator or self.__resNodesGenerator:
                jobResources = subJobResources.toDict()

                if self.__resCoresGenerator:
                    jobResources['numCores'] = next(self.__resCoresGenerator)

                if self.__resNodesGenerator:
                    jobResources['numNodes'] = next(self.__resNodesGenerator)

                subJobResources = JobResources(**jobResources)

            subJobAfter = None
            if self.__afterIterationJobs:
                subJobAfter = set()
                for jobName in self.__afterIterationJobs:
                    subJobAfter.add(jobName.replace('${it}', subJobIteration))

            self.__iterationSubJobs.append(SchedulingIteration(self, subJobIteration, subJobResources, subJobAfter))

        self.__currentSolvedIterations += niters
        logging.debug('{} currently iterations solved in job {}'.format(self.__currentSolvedIterations, self.job.name))


    def isFeasible(self):
        """
        Check if job can be executed.
        Job that dependency will never be satisfied (dependent jobs failed) should never be run.

        Returns:
            bool: does the job can be run in future
        """
        return self.__isFeasible


    def isReady(self):
        """
        Check if job can be scheduled and executed.
        Jobs with not met dependencies can not be scheduled.

        Returns:
            bool: is job ready for scheduling and executing
        """
        return len(self.__afterJobs) == 0


    def getReadyIteration(self, prevIteration=None):
        """
        Return SchedulingIteration describing next ready iteration.

        :param prevIteration: if defined the next iteration should be after specified one

        :return:
            SchedulingIteration - next ready iteration to allocate resources and execute
            None - none of iteration is ready to execute
        """
        if self.isReady():
            if self.__hasIterations and not self.__iterationSubJobs and \
                    self.__currentSolvedIterations < self.__totalIterations:
                self.setupIterations()

            startPos = 0

            if prevIteration:
                startPos = self.__iterationSubJobs.index(prevIteration) + 1

            logging.debug('job {} is ready, looking for next to ({}) iteration, startPos set to {}'.format(
                self.job.name, prevIteration, startPos))

            repeats = 2
            while repeats > 0:
                for i in range(startPos, len(self.__iterationSubJobs)):
                    iterationJob = self.__iterationSubJobs[i]
                    if iterationJob.isReady():
                        return iterationJob

                if self.__currentSolvedIterations < self.__totalIterations and repeats > 1:
                    startPos = len(self.__iterationSubJobs)
                    self.setupIterations()
                    repeats = repeats - 1
                else:
                    break

        return None


    def hasMoreIterations(self):
        """
        Check if job has more pending iterations.

        :return:
            True - there are pending iterations
            False - no more iterations, all iterations already scheduled
        """
        return self.__iterationSubJobs or self.__currentSolvedIterations < self.__totalIterations


    def removeIteration(self, iterationJob):
        """
        Called by the manager when iteration returned by the getReadyIteration has been allocated resources and
        will be executed or it's resorce requirements exceedes available resources. This iteration should not be
        returned another time by the getReadyIteration.
        """
        logging.debug('iteration {} processed - removing from list'.format(iterationJob.name))
        self.__iterationSubJobs.remove(iterationJob)


    def getIterationsMinResourceRequirements(self):
        """
        The function returns a minimum number of cores that any iteration in the job requires. Such information
        can optimize scheduler.

        :return: minCores - the minimum required number of cores by the iterations
        """
        return self.__minResCores


class SchedulingIteration:
    def __init__(self, schedulingJob, iteration, resources, afterSubJobs):
        # link to the parent job
        self.__schedulingJob = schedulingJob

        # iteration identifier
        self.__iteration = iteration

        # resource requirements
        self.__resources = resources

        # individual subjob dependencies
        self.__afterSubJobs = afterSubJobs

        # name of the subjob
        self.__name = self.__schedulingJob.job.name + (':{}'.format(self.__iteration) \
                                                           if not self.__iteration is None else '')

        # does the subjob has chance to execute
        self.__isFeasible = True

        if self.__afterSubJobs:
            self.checkDependencies()

    def checkDependencies(self):
        """
        Update individual subjob dependency state.
        Check all dependent jobs and update job's ready (and possible feasible) status.
        """
        if self.__afterSubJobs:
            finished = set()

            for jobId in self.__afterSubJobs:
                jobName = jobId
                jobIt = None

                if ":" in jobName:
                    jobName, jobIt = self.__manager.jobList.parse_jobname(jobName)

                depJob = self.__manager.jobList.get(jobName)
                if depJob is None:
                    logging.warning("Dependency {} job not registered - {} not feasible".format(jobName, self.name))
                    self.__isFeasible = False
                    break

                depJobState = depJob.getState(jobIt)
                if depJobState.isFinished():
                    if depJob.getState() != JobState.SUCCEED:
                        self.__isFeasible = False
                        break
                    else:
                        finished.add(jobId)

            self.__afterSubJobs -= finished

            logging.debug("#{} dependency ({} feasible) jobs after update of job {}".format(
                len(self.__afterSubJobs), str(self.__isFeasible), self.name))

    def isFeasible(self):
        """
        Return the subjob 'health' status - does the subjob has a chance to execute.

        :return:
            True - yes
            False - subjob will never execute it should change status to OMMITED
        """
        return self.__isFeasible

    def isReady(self):
        """
        Return the subjob readiness status - is it ready for execution (the all dependencies has been met).

        :return:
            True - the job may be executed
            False - not all dependent jobs has been finished
        """
        return not self.__afterSubJobs

    @property
    def name(self):
        return self.__name

    @property
    def job(self):
        return self.__schedulingJob.job

    @property
    def iteration(self):
        return self.__iteration

    @property
    def resources(self):
        return self.__resources


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

        self.zmq_address = None

        self.managerId = Config.MANAGER_ID.get(config)
        self.managerTags = Config.MANAGER_TAGS.get(config)

        self.__parentManager = parentManager


    async def setupInterfaces(self):
        """
        Initialize manager after all incoming interfaces has been started.
        """
        if self.__parentManager:
            try:
                logging.info('registering in parent manager {} ...'.format(self.__parentManager))
                await self.registerInParent()
                self.registerNotifier(self.__notifyParentWithJob)
            except:
                logging.error('Failed to register manager in parent governor manager: {}'.format(sys.exc_info()[0]))
                raise
        else:
            logging.info('no parent manager set')


    def setZmqAddress(self, zmq_address):
        self.zmq_address = zmq_address


    def getHandlerInstance(self):
        return DirectManagerHandler(self)


    async def stop(self):
        if self.__executor:
            await self.__executor.stop()


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

        logging.debug("scheduling loop with {} jobs in queue".format(len(self.__scheduleQueue)))

        for idx, schedJob in enumerate(self.__scheduleQueue):
            if not self.resources.freeCores:
                newScheduleQueue.extend(self.__scheduleQueue[idx:])
                break

            minResCores = schedJob.getIterationsMinResourceRequirements()
            if not minResCores is None and minResCores > self.resources.freeCores:
                logging.debug('minimum # of cores {} for job {} exceeds # of free cores {}'.format(minResCores,
                    schedJob.job.name, self.resources.freeCores))
                self.__appendToScheduleQueue(newScheduleQueue, schedJob)
                continue

            schedJob.checkDependencies()

            if not schedJob.isFeasible():
                # job will never be ready
                logging.debug("job {} not feasible - omitting".format(schedJob.job.name))
                self.__changeJobState(schedJob.job, iteration=None, state=JobState.OMITTED)
                schedJob.job.clearQueuePos()
            else:
                prevIteration = None
                while self.resources.freeCores:
                    if not minResCores is None and minResCores > self.resources.freeCores:
                        logging.debug('minimum # of cores {} for job {} exceeds # of free cores {}'.format(minResCores,
                            schedJob.job.name, self.resources.freeCores))
                        break

                    jobIteration = schedJob.getReadyIteration(prevIteration)

                    if jobIteration:
                        # job is ready - try to find resources
                        logging.debug("job {} is ready".format(jobIteration.name))
                        try:
                            allocation = self.__scheduler.allocateJob(jobIteration.resources)
                            if allocation:
                                schedJob.removeIteration(jobIteration)
                                prevIteration = None

                                logging.debug("found resources for job {}".format(jobIteration.name))

                                # allocation has been created - execute job
                                self.__changeJobState(schedJob.job, iteration=jobIteration.iteration,
                                                      state=JobState.SCHEDULED)

                                asyncio.ensure_future(self.__executor.execute(allocation, jobIteration))
                            else:
                                # missing resources
                                logging.debug("missing resources for job {}".format(jobIteration.name))
                                prevIteration = jobIteration
                        except (NotSufficientResources, InvalidResourceSpec) as e:
                            # jobs will never schedule
                            logging.warning("Job {} scheduling failed - {}".format(jobIteration.name, str(e)))
                            schedJob.removeIteration(jobIteration)
                            prevIteration = None
                            self.__changeJobState(schedJob.job, iteration=jobIteration.iteration,
                                                  state=JobState.FAILED, errorMsg=str(e))
                    else:
                        break

                if schedJob.hasMoreIterations():
                    logging.warning("Job {} preserved in scheduling queue".format(schedJob.job.name))
                    self.__appendToScheduleQueue(newScheduleQueue, schedJob)

        self.__scheduleQueue = newScheduleQueue


    def __changeJobState(self, job, iteration, state, errorMsg=None):
        """
        Invoked to change job status.
        Any notification should be called from this method.

        Args:
            job (ExecutingJob): job that changed status
            iteration (int): job iteration index
            status (JobState): target job state
            errorMsg (string): optional error messages
        """
        parentJobChangedStatus = job.setState(state, iteration, errorMsg)

        self.__fireJobStateNotifies(job.name, iteration, state)
        if parentJobChangedStatus:
            logging.debug("parent job {} status changed to {} - notifing".format(job.name, parentJobChangedStatus.name))
            self.__fireJobStateNotifies(job.name, None, state)


    def jobExecuting(self, jobIteration):
        """
        Invoked to signal starting job iteration execution.

        Args:
            jobIteration (SchedulingIteration): job iteration that started executing
        """
        self.__changeJobState(jobIteration.job, iteration=jobIteration.iteration, state=JobState.EXECUTING)


    def jobFinished(self, jobIteration, allocation, exitCode, errorMsg):
        """
        Invoked to signal job finished.
        Allocation made for the job should be released.

        Args:
            jobIteration (SchedulingIteration): job iteration that finished
            allocation (Allocation): allocation created for the job
            exitCode (int): job exit code
            errorMsg (str): an optional error message
        """
        state = JobState.SUCCEED

        if exitCode != 0:
            state = JobState.FAILED

        self.__changeJobState(jobIteration.job, iteration=jobIteration.iteration, state=state, errorMsg=errorMsg)
        self.__scheduler.releaseAllocation(allocation)
        self.__scheduleLoop()


    def __fireJobStateNotifies(self, jobId, iteration, state):
        """
        Create task with callback functions call registered for job state changes.
        A new asyncio task is created which call all registered callbacks in not defined order.

        Args:
            jobId (str): job identifier
            state (JobState): new job status
        """
        if len(self.__jobStatesCbs) > 0:
            logging.debug("notifies callbacks about {} job status change {}".format(jobId if iteration is None else
                '{}:{}'.format(jobId, iteration), state))
            asyncio.ensure_future(self.__callCallbacks(
                jobId, iteration, state, self.__jobStatesCbs.values()
            ))


    async def __callCallbacks(self, jobId, iteration, state, cbs):
        """
        Call job state change callback function with given arguments.

        Args:
            jobId (str): job identifier
            iteration (int): job iteration index
            state (JobState): new job status
            cbs ([]function): callback functions
        """
        if cbs is not None:
            for cb in cbs:
                try:
                    cb.cb(jobId, iteration, state, *cb.args)
                except Exception as e:
                    logging.exception("Callback function failed: {}".format(str(e)))


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
        parentSocket.setsockopt(zmq.LINGER, 0)

        return parentSocket


    def __getParentSyncSocket(self):
        parentSocket = zmq.Context.instance().socket(zmq.REQ)
        parentSocket.connect(self.__parentManager)
        parentSocket.setsockopt(zmq.LINGER, 0)

        return parentSocket


    async def __sendParentRequestWithValidAsyncTimeout(self, request, timeout):
        out_socket = None

        try:
            out_socket = self.__getParentSocket()

            await out_socket.send_json(request)
            msg = await asyncio.wait_for(out_socket.recv_json(), timeout)
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(msg.get('message', '')))

            return msg
        except:
            raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(str(sys.exc_info())))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    # ignore errors during cleanup
                    logging.debug('failed to close socket: {}'.format(str(sys.exc_info())))


    async def __sendParentRequestWithValidAsync(self, request):
        out_socket = None

        try:
            out_socket = self.__getParentSocket()

            await out_socket.send_json(request)
            msg = await asyncio.wait_for(out_socket.recv_json(), 5)
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to send message to parent manager: {}'.format(msg.get('message', '')))

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
            out_socket = self.__getParentSyncSocket()

            out_socket.send_json(request)
            logging.debug('sent register request: {}'.format(str(request)))

            msg = out_socket.recv_json()

#            poller = zmq.Poller()
#            poller.register(out_socket, zmq.POLLIN)
#            if poller.poll(5 * 1000):  # 10s timeout in milliseconds
#                poller.unregister(out_socket)
#                msg = out_socket.recv_json()
#            else:
#                poller.unregister(out_socket)
#                poller = None
#                raise TimeoutError('Timeout registering in parent manager {}'.format(self.__parentManager))
            logging.debug('got register response: {}'.format(str(msg)))
            if not msg['code'] == 0:
                raise GovernorConnectionError('Failed to register manager instance in governor: {}'.format(msg.get('message', '')))

            return msg
        finally:
            if out_socket:
                try:
                    logging.debug('closing sync socket: {}'.format(str(out_socket)))
                    out_socket.close()
                except:
                    # ignore errors during cleanup
                    pass


    def __notifyParentWithJob(self, jobId, iteration, state):
        if self.__parentManager and state.isFinished() and iteration is None:
            # notify parent only about whole jobs, not a single iterations
            # send also the job attributes which are necessary to identify the job in governor manager
            try:
                job = self.jobList.get(jobId)
                reqData = { 'request': 'notify',
                            'entity': 'job',
                            'params': {
                                'name': jobId,
                                'state': state.name,
                                'attributes': job.attributes
                            }
                }
                asyncio.ensure_future(self.__sendParentRequestWithValidAsync(reqData))
            except Exception:
                logging.error('failed to send job notification to the parent manager: {}'.format(sys.exc_info()))
                logging.error(traceback.format_exc())


    async def registerInParent(self):
        """
        Register manager in parent governor manager.
        """
        await self.__sendParentRequestWithValidAsyncTimeout({ 'request': 'register',
            'entity': 'manager',
            'params': {
            'id': self.managerId,
            'address': self.zmq_address,
            'resources': self.resources.toDict(),
            'tags': self.managerTags,
          }
        }, 5)


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

        return Response.Ok('{} command accepted'.format(request.command))


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
#            logging.error(traceback.format_exc())
            return Response.Error(str(e))


    def __prepareJobs(self, reqJobs):
        resources = self.__manager.resources

        req_job_names = set()

        newJobs = []
        for req in reqJobs:
            reqJob = req['req']
            vars = req['vars']

            vars['jname'] = self.__replaceVariablesInString(reqJob['name'], vars)

            if any((c in vars['jname'] for c in ['$', '{', '}', '(', ')', '\'', '"', ' ', '\t', '\n'])):
                raise InvalidRequest('Job identifier \'({})\' contains invalid characters or unknown variables'.format(
                    vars['jname']))

            # default value for missing 'resources' definition
            if 'resources' not in reqJob:
                reqJob['resources'] = { 'numCores': { 'exact': 1 } }

            try:
                reqJob_vars = self.__replaceVariables(reqJob, vars)

                # verify job name uniqness
                if self.__manager.jobList.exist(reqJob_vars['name']) or \
                        reqJob_vars['name'] in req_job_names:
                    raise JobAlreadyExist('Job {} already exist'.format(reqJob_vars['name']))

                newJob = Job(**reqJob_vars)
                # validate resource requirements
                if not resources.checkMaximumJobRequirements(newJob.resources):
                    raise InvalidRequest('Not enough resources for job {}'.format(vars['jname']))

                newJobs.append(newJob)
                req_job_names.add(reqJob_vars['name'])
            except InvalidRequest:
                raise
            except JobAlreadyExist as e:
                raise e
            except Exception as e:
                logging.exception('Wrong submit request: {}'.format(str(e)))
                raise InvalidRequest('Wrong submit request: {}'.format(str(e))) from e

        # verify job dependencies
        #TODO: code to verify job dependencies

        return newJobs


    def __replaceVariables(self, data, vars):
        if vars is not None and len(vars) > 0:
            return json.loads(Template(json.dumps(data)).safe_substitute(vars))
        else:
            return data

    def __replaceVariablesInString(self, string, vars):
        if vars is not None and len(vars) > 0:
            return Template(string).safe_substitute(vars)
        else:
            return string


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
                    return Response.Error('Job \'{}\' doesn\'t exist'.format(request.jobName))

                result[jobName] = {'status': int(ResponseCode.OK), 'data': {
                    'jobName': jobName,
                    'status': str(job.getStateStr())
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
                    'status': str(job.getStateStr())
                }

                if job.isIterative():
                    jobData['iterations'] = { 'start': job.getIteration().start,
                                              'stop': job.getIteration().stop,
                                              'total': job.getIteration().iterations(),
                                              'finished': job.getIteration().iterations() - job.getSubjobsNotfinished(),
                                              'failed': job.getSubjobsFailed() }

                    if request.includeChilds:
                        jobData['childs'] = [ ]
                        for idx, subJob in enumerate(job.getSubjobs()):
                            info = { 'iteration': idx + job.getIteration().start,
                                     'state': subJob.getState().name }

                            subruntime = subJob.getRuntime()
                            if subruntime is not None and len(subruntime) > 0:
                                info['runtime'] = subruntime

                            jobData['childs'].append(info)

                if job.getMessages() is not None:
                    jobData['messages'] = job.getMessages()

                jruntime = job.getRuntime()
                if jruntime is not None and len(jruntime) > 0:
                    jobData['runtime'] = jruntime

                jhistory = job.getHistory()
                if jhistory is not None and len(jhistory) > 0:
                    history_str = ''

                    for entry in jhistory:
                        history_str = '\n'.join([history_str, "{}: {}".format(str(entry[1]), entry[0].name)])

                    jobData['history'] = history_str

                result[jobName] = {'status': int(ResponseCode.OK), 'data': jobData}
            except Exception as e:
                result[jobName] = {'status': int(ResponseCode.ERROR), 'message': e.args[0]}

        return Response.Ok(data={'jobs': result})


    async def handleCancelJobReq(self, iface, request):
        job = self.__manager.jobList.get(request.jobName)

        if job is None:
            return Response.Error('Job \'{}\' doesn\'t exist'.format(request.jobName))

        return Response.Error('Cancel job is not supported')


    async def handleRemoveJobReq(self, iface, request):
        removed = 0
        errors = {}

        for jobName in request.jobNames:
            try:
                job = self.__manager.jobList.get(jobName)

                if job is None:
                    raise InvalidRequest('Job \'{}\' doesn\'t exist'.format(jobName))

                if not job.getState().isFinished():
                    raise InvalidRequest('Job \'{}\' not finished - can not be removed'.format(jobName))

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

        logging.info("got {} jobs from list".format(str(len(job_names))))

        jobs = {}
        for jobName in job_names:
            job = self.__manager.jobList.get(jobName)

            if job is None:
                return Response.Error('One of the job \'{}\' doesn\'t exist in registry'.format(jobName))

            job_data = {
                'status': str(job.getStateStr())
            }

            if job.getMessages() is not None:
                job_data['messages'] = job.getMessages()

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
                if job.getState() in [JobState.QUEUED, JobState.SCHEDULED]:
                    nSchedulingJobs += 1
                elif job.getState() in [JobState.EXECUTING]:
                    nExecutingJobs += 1
                elif job.getState() in [JobState.FAILED, JobState.OMITTED]:
                    nFailedJobs += 1
                elif job.getState() in [JobState.CANCELED, JobState.SUCCEED]:
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

        logging.info("detected all jobs finished")
        self.stopReceiver()


    async def __delayedFinish(self, delay):
        logging.info("finishing in {} seconds".format(delay))

        await asyncio.sleep(delay)

        self.stopReceiver()


    def stopReceiver(self):
        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.warning('Failed to set finish flag due to lack of receiver access')
