import asyncio
import sys
import logging
import zmq
import traceback
import math
import copy
import os
import shutil
import socket
import getpass
from datetime import datetime

from qcg.appscheduler.request import RemoveJobReq, ControlReq, StatusReq
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, JobInfoReq, CancelJobReq
from qcg.appscheduler.response import Response, ResponseCode
from qcg.appscheduler.resources import Resources
from qcg.appscheduler.errors import InvalidRequest, JobAlreadyExist, InternalError
from qcg.appscheduler.joblist import JobState
from qcg.appscheduler.config import Config
from qcg.appscheduler.slurmres import in_slurm_allocation, parse_slurm_resources


class GlobalJob:

    def __init__(self, name, status, job_parts):
        """
        Job information stored at GovernorManager

        :param name (str):  job name
        :param status (JobStatus): current job status
        :param job_parts (GlobalJobPart[]): instances of the job parts
        """
        self.name = name
        self.status = status

        self.job_parts = { job_part.id: job_part for job_part in job_parts }
        self.unfinished = len(self.job_parts)

    def __str__(self):
        return '{} in {} state (with {} managers)'.format(self.name, self.status.name, len(self.job_parts))

    def update_part_status(self, part_id, state):
        """
        Set new job part state.
        In this method we compute also the new global job state.

        :return True if status has been updated, False otherwise
        """
        job_part = self.job_parts.get(part_id)
        if not job_part:
            return False

        job_part.state = state
        if state.isFinished():
            # updating global status
            self.unfinished = self.unfinished - 1
            if self.unfinished == 0:
                nfailed_parts = sum([1 if job_part.state in [ JobState.FAILED, JobState.OMITTED, JobState.CANCELED]
                                     else 0 for job_part in self.job_parts.values()])
                self.status = JobState.SUCCEED if nfailed_parts == 0 else JobState.FAILED

        return True

class GlobalJobPart:

    def __init__(self, id, local_job_name, it_start, it_stop, status, manager_instance):
        """
        Part of the global iterated job.

        :param id (str): job part identifier
        :param local_job_name (str): name of the job in the local manager instance
        :param it_start (int): iteration start (None in case of noniterated job)
        :param it_end (int): iteration stop (None in case of noniterated job)
        :param status (JobStatus): status of this part of job
        :param manager_instance (ManagerInstance): address of the manager instance that schedules & executes part of the job
        """
        self.id = id
        self.local_job_name = local_job_name
        self.it_start = it_start
        self.it_stop = it_stop
        self.status = status
        self.manager_instance = manager_instance


class PartitionManager:

    def __init__(self, mid, nodeName, startNode, endNode, workDir, governorAddress, auxDir):
        """
        The instance of partition manager.
        The partition manager is an instance of manager that controls part of the allocation (the nodes range
        'startNode'-'endNode' of the Slurm allocation). The instance is launched from governor manager to run
        jobs submited by governor on it's part of the allocation.

        :param mid (str): partition manager identifier
        :param nodeName (str): the node where partition manager should be launched
        :param startNode (int): the start node of the allocation the manager should control
        :param endNode (int): the end node of the allocation the manager should control
        :param workDir (str): the working directory of partition manager
        :param governorAddress (str): the address of the governor manager where partition manager should register
        :param auxDir (str): the auxiliary directory for partition manager
        """
        self.mid = mid
        self.nodeName = nodeName
        self.startNode = startNode
        self.endNode = endNode
        self.workDir = workDir
        self.governorAddress = governorAddress
        self.auxDir = auxDir

        self.stdoutP = None
        self.stderrP = None

        self.process = None

        self.__partitionManagerCmd = [ sys.executable, '-m', 'qcg.appscheduler.service' ]
        self.__defaultPartitionManagerArgs = [ '--report-format', 'json', '--system-core' ]


    async def launch(self):
        logging.info('launching partition manager {} on node {} to control node numbers {}-{}'.format(
            self.mid, self.nodeName, self.startNode, self.endNode))

        slurm_args = ['-J', str(self.mid), '-w', self.nodeName, '--oversubscribe', '--overcommit', '-N', '1', '-n', '1', '-D', self.workDir]

        manager_args = [ *self.__defaultPartitionManagerArgs,
                             '--id', self.mid, '--parent', self.governorAddress,
                             '--slurm-limit-nodes-range-begin', str(self.startNode),
                             '--slurm-limit-nodes-range-end', str(self.endNode) ]

        if logging.root.level == logging.DEBUG:
            manager_args.extend(['--log', 'debug'])

        self.stdoutP = asyncio.subprocess.DEVNULL
        self.stderrP = asyncio.subprocess.DEVNULL

        if logging.root.level == logging.DEBUG:
            self.stdoutP = open(os.path.join(self.auxDir, 'part-manager-{}-stdout.log'.format(self.mid)), 'w')
            self.stderrP = open(os.path.join(self.auxDir, 'part-manager-{}-stderr.log'.format(self.mid)), 'w')

        logging.debug('running partition manager process with args: {}'.format(
            ' '.join([ shutil.which('srun') ] + slurm_args + self.__partitionManagerCmd + manager_args)))

        self.process = await asyncio.create_subprocess_exec(
            shutil.which('srun'), *slurm_args, *self.__partitionManagerCmd, *manager_args,
            stdout=self.stdoutP, stderr=self.stderrP)


    def terminate(self):
        if self.process:
            logging.info('terminating partition manager {} @ {}'.format(self.mid, self.startNode))
            self.process.terminate()


class ManagerInstance:

    def __init__(self, mid, resources, address):
        """
        The information about QCG manager instance available for higher level manager.

        :param id: manager identifier
        :param resources: manager's last reported resources
        :param address: manager's ZMQ input interface address
        """
        self.id = mid
        self.resources = resources
        self.address = address

        self.submitted_jobs = 0
        self.socket = None


    def stop(self):
        """
        Cleanup before finish.
        All resources should be freed.
        """
        self.closeSocket()


    def getSocket(self):
        """
        Return ZMQ socket connected to manager instance.
        :return: connected ZMQ socket
        """
        if not self.socket:
            self.socket = zmq.asyncio.Context.instance().socket(zmq.REQ)
            self.socket.connect(self.address)

        return self.socket


    def closeSocket(self):
        """
        Return ZMQ socket.
        """
        if self.socket:
            try:
                self.socket.close()
            except:
                logging.warning('failed to close manager {} socket: {}'.format(self.address, sys.exc_info()))
            self.socket = None


    async def submitJobs(self, reqJobs):
        """
        Send submit request to the manager instance

        :param req: the submit request
        :return: dict with:
           'njobs' - number of submitted jobs
           'names' - list of submited job names
        """
        await self.getSocket().send_json(
            {'request': 'submit',
             'jobs': reqJobs
            }
        )
        msg = await self.getSocket().recv_json()
        if not msg['code'] == 0:
            raise InvalidRequest(
                'Failed to submit jobs: {}'.format(msg.get('message', '')))

        self.submitted_jobs += msg.get('data', {}).get('submitted', 0)
        return { 'njobs': msg.get('data', {}).get('submitted', 0),
                 'names': msg.get('data', {}).get('jobs', []) }


class TotalResources:
    def __init__(self):
        """
        The information about total resources available in all instances.
        """
        self.totalNodes = 0
        self.totalCores = 0
        self.usedCores = 0
        self.freeCores = 0

    def zero(self):
        """
        Reset values.
        """
        self.totalNodes = 0
        self.totalCores = 0
        self.usedCores = 0
        self.freeCores = 0

    def append(self, instance_resources):
        self.totalNodes += len(instance_resources.nodes)
        self.totalCores += instance_resources.totalCores
        self.usedCores += instance_resources.usedCores
        self.freeCores += instance_resources.freeCores


class GovernorManager:

    def __init__(self, config={}, parentManager=None):
        """
        Manager of jobs to execution but without direct access to the resources.
        The GovernorManager schedules jobs to other, dependant managers.
        """
        self.managers = dict()
        self.totalResources = TotalResources()
        self.jobs = dict()

        self.__finishTask = None

        self.__receiver = None
        self.zmq_address = None

        self.__config = config
        self.__createPartitions = Config.SLURM_PARTITION_NODES.get(config)

        self.__parentManager = parentManager
        self.__partitionManagers = [ ]

        # the minimum number of registered managers the scheduling can start
        self.__minShedulingManagers = 0

        # the maximum number of buffered jobs until, when reached the submit request will be responsed with error
        self.__maxBufferedJobs = 1000

        # the buffer for submitted jobs
        self.__submitReqsBuffer = [ ]

        # the task that will schedule buffered submit requests when minimum required number of managers registers
        self.__scheduleBufferedJobsTask = None

        # the maximum nubmer of seconds after start to wait for partition managers to register
        self.__waitForRegisterTimeout = 10

        self.startTime = datetime.now()


    async def setupInterfaces(self):
        """
        Initialize manager after all incoming interfaces has been started.
        """
        if self.__parentManager:
            try:
                await self.registerInParent(self.__parentManager)
            except:
                logging.error('Failed to register manager in parent governor manager: {}'.format(sys.exc_info()[0]))
                raise

        if self.__createPartitions:
            await self.__launchPartitionManagers(self.__createPartitions)


    async def registerInParent(self):
        logging.error('Governing managers can not currenlty register in parent managers')
        raise InternalError('Governing managers can not currenlty register in parent managers')


    async def __launchPartitionManagers(self, partitionNodes):
        logging.info('setup allocation split into partitions by {} nodes'.format(partitionNodes))

        if partitionNodes < 1:
            logging.error('Failed to partition resources - partition size must be greater or equal 1')
            raise InvalidRequest('Failed to partition resources - partition size must be greater or equal 1')

        # get slurm resources - currently only slurm is supported
        if not in_slurm_allocation():
            logging.error('Failed to partition resources - partitioning resources is currently available only within slurm allocation')
            raise InvalidRequest('Failed to partition resources - partitioning resources is currently available only within slurm allocation')

        if not self.zmq_address:
            logging.error('Failed to partition resources - missing zmq interface address')
            raise InternalError('Failed to partition resources - missing zmq interface address')

        slurm_resources = parse_slurm_resources(self.__config)

        if slurm_resources.totalNodes < 1:
            raise InvalidRequest('Failed to partition resources - allocation contains no nodes')

        npartitions = math.ceil(slurm_resources.totalNodes / partitionNodes)
        logging.info('{} partitions will be created (in allocation containing {} total nodes)'.format(
            npartitions, slurm_resources.totalNodes))

        # launch partition manager in the same directory as governor
        partition_manager_wdir = Config.EXECUTOR_WD.get(self.__config)
        partition_manager_auxdir = Config.AUX_DIR.get(self.__config)

        logging.debug('partition managers working directory {}'.format(partition_manager_wdir))

        for part_idx in range(npartitions):
            logging.debug('creating partition manager {} configuration'.format(part_idx))

            part_node = slurm_resources.nodes[part_idx * partitionNodes]

            logging.debug('partition manager node {}'.format(part_node.name))

            self.__partitionManagers.append(PartitionManager('partition-{}'.format(part_idx), part_node.name,
                part_idx * partitionNodes, min(part_idx * partitionNodes + partitionNodes, slurm_resources.totalNodes),
                partition_manager_wdir, self.zmq_address, partition_manager_auxdir))

            logging.debug('partition manager {} configuration created'.format(part_idx))

        self.__minShedulingManagers = len(self.__partitionManagers)

        logging.info('created partition managers configuration and set minimum scheduling managers to {}'.format(self.__minShedulingManagers))

        asyncio.ensure_future(self.managePartitionsManagers())

        # launch task in the background that schedule buffered submit requests
        self.__scheduleBufferedJobsTask = asyncio.ensure_future(self.__scheduleBufferedJobs())


    async def managePartitionsManagers(self):
        """
        Start and manage partitions manager.
        Ensure that all started partitions manager registered in governor.
        """
        try:
            # start all partition managers
            try:
                logging.info('starting all partition managers ...')
                await asyncio.gather(*[manager.launch() for manager in self.__partitionManagers])
            except:
                logging.error('one of partition manager failed to start - killing the rest')
                # if one of partition manager didn't start - stop all instances and set governor manager finish flag
                self.__terminatePartitionManagersAndFinish()
                raise InternalError('Partition managers not started')

            logging.info('all ({}) partition managers started successfully'.format(len(self.__partitionManagers)))

            # check if they registered in assumed time
            logging.debug('waiting {} secs for partition manager registration'.format(self.__waitForRegisterTimeout))

            startWaitingForRegistration = datetime.now()
            while len(self.managers) < len(self.__partitionManagers):
                await asyncio.sleep(0.2)

                if (datetime.now() - startWaitingForRegistration).total_seconds() > self.__waitForRegisterTimeout:
                    # timeout exceeded
                    logging.error('not all partition managers registered - only {} on {} total'.format(
                        len(self.managers), len(self.__partitionManagers)))
                    self.__terminatePartitionManagersAndFinish()
                    raise InternalError('Partition managers not registered')

            logging.info('available resources: {} ({} used) cores on {} nodes'.format(
                self.totalResources.totalCores, self.totalResources.usedCores, self.totalResources.totalNodes))

            logging.info('all partition managers registered')
        except:
            logging.error('setup of partition managers failed: {}'.format(str(sys.exc_info())))


    def __terminatePartitionManagers(self):
        for manager in self.__partitionManagers:
            try:
                manager.terminate()
            except:
                logging.warning('failed to terminate manager: {}'.format(sys.exc_info()))
                logging.error(traceback.format_exc())


    def __terminatePartitionManagersAndFinish(self):
        self.__terminatePartitionManagers()

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.error('Failed to set finish flag due to lack of receiver access')


    def getHandlerInstance(self):
        return self


    def setReceiver(self, receiver):
        self.__receiver = receiver
        if self.__receiver:
            self.zmq_address = self.__receiver.getZmqAddress()


    async def stop(self):
        """
        Cleanup before finish.
        """
        for manager in self.managers.values():
            manager.stop()

        # kill all partition managers
        self.__terminatePartitionManagers()

        # stop submiting buffered tasks
        if self.__scheduleBufferedJobsTask:
            try:
                self.__scheduleBufferedJobsTask.cancel()
            except:
                logging.warning('failed to cancel buffered jobs scheduler task: {}'.format(sys.exc_info()[0]))

            try:
                await self.__scheduleBufferedJobsTask
            except asyncio.CancelledError:
                logging.debug('listener canceled')


    def updateTotalResources(self):
        self.totalResources.zero()

        for m in self.managers.values():
            self.totalResources.append(m.resources)


    def registerNotifier(self, jobStateCb, *args):
        # TODO: do weed need this functionality for governor manager ???
        pass


    async def __scheduleBufferedJobs(self):
        """
        Take all buffered jobs (already validated) and schedule them on available resources.
        """
        while True:
            try:
                await asyncio.sleep(0.1)

                while self.__minShedulingManagers <= len(self.managers) and len(self.__submitReqsBuffer) > 0:
                    logging.info('the minimum number of managers has been achieved - scheduling buffered jobs')
                    try:
                        # do not remove request from buffer before jobs will be added to the jobs database
                        # this would provide to the finish of manager (no jobs in db, no jobs in buffer)
                        jobReq = self.__submitReqsBuffer[0]
                        await self.__scheduleJobs(jobReq)
                        # now we can safely remove request from buffer as the jobs are added to the db
                        self.__submitReqsBuffer.pop(0)
                    except:
                        logging.error('error during scheduling buffered submit requests: {}'.format(sys.exc_info()))
                        logging.error(traceback.format_exc())
            except asyncio.CancelledError:
                logging.info('finishing submitting buffered jobs task')
                return


    async def __waitForAllJobs(self):
        """
        Wait until all submitted jobs finish and signal receiver to finish.
        """
        logging.info('waiting for all jobs to finish')

        while not self.allJobsFinished():
            await asyncio.sleep(0.2)

        logging.info('All ({}) jobs finished, no buffered jobs ({})'.format(len(self.jobs), len(self.__submitReqsBuffer)))

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.error('Failed to set finish flag due to lack of receiver access')


    def allJobsFinished(self):
        """
        Check if all submitted jobs finished.
        :return: True if all submitted jobs finished, otherwise False
        """
        try:
            return next((job for job in self.jobs.values() if not job.status.isFinished()), None) == None and \
                len(self.__submitReqsBuffer) == 0
        except:
            logging.error('error counting jobs: {}'.format(sys.exc_info()))
            logging.error(traceback.format_exc())
            return False


    async def handleRegisterReq(self, iface, request):
        if request.params['id'] in self.managers:
            return Response.Error('Manager with id "{}" already registered')

        if not request.params['address']:
            return Response.Error('Missing registry entity address')

        try:
            instanceResources = Resources.fromDict(request.params['resources'])
            self.managers[request.params['id']] = ManagerInstance(request.params['id'],
                                                                  instanceResources,
                                                                  request.params['address'])
            self.totalResources.append(instanceResources)
        except:
            return Response.Error('Failed to register manager: {}'.format(sys.exc_info()[0]))

        logging.info('{}th manager instance {} @ {} with resources ({}) registered successfully'.format(
            len(self.managers), request.params['id'], request.params['address'], request.params['resources']))

        return Response.Ok(data={'id': request.params['id']})


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
        if request.command != ControlReq.REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE:
            return Response.Error('Not supported command "{}" of finish control request'.format(request.command))

        if self.__finishTask is not None:
            return Response.Error('Finish request already requested')

        self.__finishTask = asyncio.ensure_future(self.__waitForAllJobs())
        return Response.Ok('%s command accepted' % (request.command))


    async def handleSubmitReq(self, iface, request):
        if self.__minShedulingManagers <= len(self.managers) and self.totalResources.totalCores == 0:
            return Response.Error('Error: no resources available')

        if len(self.__submitReqsBuffer) >= self.__maxBufferedJobs:
            return Response.Error('Error: submit buffer overflow (currently {} buffered jobs) - try submit later'.format(\
                len(self.__submitReqsBuffer)))

        try:
            # validate jobs
            self.__validateJobReqs(request.jobReqs)
        except Exception as e:
            logging.error('Submit error: {}'.format(sys.exc_info()))
            logging.error(traceback.format_exc())
            return Response.Error(str(e))


        try:
            if self.__minShedulingManagers > len(self.managers):
                # we don't have all (partition) managers registered - buffer jobs
                self.__submitReqsBuffer.append(request.jobReqs)
                logging.debug('buffering submit request, current buffer size: {}'.format(len(self.__submitReqsBuffer)))

                return Response.Ok('{} jobs buffered'.format(len(request.jobReqs)), data =
                    { 'buffered': len(request.jobReqs) })
            else:
                # submit at once

                # split jobs equally between all available managers
                (nJobs, jobNames) = await self.__scheduleJobs(request.jobReqs)

                data = {
                    'submitted': len(jobNames),
                    'jobs': jobNames
                }

                return Response.Ok('{} jobs submitted'.format(len(jobNames)), data=data)
        except Exception as e:
            logging.error('Submit error: {}'.format(sys.exc_info()))
            logging.error(traceback.format_exc())
            return Response.Error(str(e))


    def __validateJobReqs(self, jobReqs):
        req_job_names = set()

        for req in jobReqs:
            jobReq = req['req']

            if jobReq['name'] in self.jobs or jobReq['name'] in req_job_names:
                raise JobAlreadyExist('Job {} already exist'.format(jobReq['name']))


    async def __scheduleJobs(self, jobReqs):
        nReqJobs = 0
        reqJobNames = []

        for req in jobReqs:
            jobReq = req['req']
            jobVars = req['vars']

            job_name = jobReq['name']

            if 'iteration' in jobReq:
                # iteration job, split between all available managers
                job_its = jobReq['iteration']
                start = job_its.get('start', 0)
                end = job_its.get('stop')
                iterations = end - start

                currIterStart = start

                submit_reqs = []

                splitPart = int(math.ceil(iterations / len(self.managers)))

                job_parts = [ ]
                for idx, m in enumerate(self.managers.values()):
                    currIterEnd = min(currIterStart + splitPart, end)

                    currJobReq = copy.deepcopy(jobReq)
                    iter_id = str(idx)
                    currJobReq.setdefault('attributes', {}).update({
                        'parent_job_id': job_name,
                        'parent_job_part_id': iter_id
                    })

                    currJobReq['iteration'] = { 'start': currIterStart, 'stop': currIterEnd }

                    submit_reqs.append(m.submitJobs([ currJobReq ]))

                    job_parts.append({ 'id': iter_id, 'start': currIterStart, 'stop': currIterEnd, 'manager': m})

                    currIterStart = currIterEnd
                    if currIterEnd == end:
                        break

                submit_results = await asyncio.gather(*submit_reqs)
                for idx, result in enumerate(submit_results):
                    job_parts[idx]['local_name'] = result['names'][0]

                self.__appendNewJob(job_name, job_parts)

                nReqJobs = nReqJobs + 1
                reqJobNames.append(job_name)
            else:
                # single job, send to the manager with lest number of submitted jobs
                manager = min(self.managers.values(), key=lambda m: m.submitted_jobs)

                logging.debug('sending job {} to manager {} ({})'.format(jobReq['name'], manager.id, manager.address))
                iter_id = "0"
                jobReq.setdefault('attributes', {}).update({
                    'parent_job_id': job_name,
                    'parent_job_part_id': iter_id
                })
                submit_result = await manager.submitJobs([ jobReq ])

                logging.debug('submit result: {}'.format(str(submit_result)))
                self.__appendNewJob(job_name, [{'id': iter_id, 'start': None, 'stop': None, 'local_name': submit_result['names'][0],
                                                'manager': manager}])

                nReqJobs = nReqJobs + 1
                reqJobNames.append(job_name)

        return (nReqJobs, reqJobNames)


    def __appendNewJob(self, job_name, job_parts):
        """
        Add new job to the global registry

        :param job_name: job name
        :param job_parts: information about job parts
        """
        parts = []
        for job_part in job_parts:
            parts.append(GlobalJobPart(job_part['id'], job_part['local_name'], job_part['start'], job_part['stop'],
                                       JobState.QUEUED, job_part['manager']))
        self.jobs[job_name] = GlobalJob(job_name, JobState.QUEUED, parts)


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
                job = self.jobs.get(jobName)

                if job is None:
                    return Response.Error('Job %s doesn\'t exist' % (request.jobName))

                result[jobName] = {'status': int(ResponseCode.OK), 'data': {
                    'jobName': jobName,
                    'status': str(job.status.name)
                }}
            except Exception as e:
                logging.warning('error to get job status: {}'.format(str(e)))
                logging.warning(traceback.format_exc())
                result[jobName] = {'status': int(ResponseCode.ERROR), 'message': e.args[0]}

        return Response.Ok(data={'jobs': result})


    async def handleJobInfoReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

    async def handleCancelJobReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

    async def handleRemoveJobReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

    async def handleListJobsReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

    async def handleResourcesInfoReq(self, iface, request):
        return Response.Ok(data={
            'totalNodes': self.totalResources.totalNodes,
            'totalCores': self.totalResources.totalCores,
            'usedCores': self.totalResources.usedCores,
            'freeCores': self.totalResources.freeCores
        })

    async def handleFinishReq(self, iface, request):
        delay = 2

        if self.__finishTask is not None:
            return Response.Error('Finish request already requested')

        self.__finishTask = asyncio.ensure_future(self.__delayedFinish(delay))

        return Response.Ok(data={
            'when': '%ds' % delay
        })

    async def handleStatusReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')


    async def handleNotifyReq(self, iface, request):
        global_job_id = request.params.get('attributes', {}).get('parent_job_id')
        global_job_part_id = request.params.get('attributes', {}).get('parent_job_part_id')

        if global_job_id is None or global_job_part_id is None:
            return Response.Error('Unknown job notify data {}'.format(str(request.params)))

        job = self.jobs.get(global_job_id, None)
        if not job:
            logging.warning('job notified {} not exist'.format(global_job_id))
            return Response.Error('Job {} unknown'.format(global_job_id))

        newstate = request.params.get('state', 'UNKNOWN')
        if not newstate in JobState.__members__:
            logging.warning('notification for job {} contains unknown state {}'.format(global_job_id, newstate))
            return Response.Error('Job\'s {} state {} unknown'.format(global_job_id, newstate))

        if job.update_part_status(global_job_part_id, JobState[newstate]):
            logging.debug('job state {} successfully update to {}'.format(global_job_id, str(newstate)))
            return Response.Ok('job {} updated'.format(global_job_id))
        else:
            return Response.Error('Failed to update job\'s {} part {} status to {}'.format(global_job_id,
                global_job_part_id, str(newstate)))


    async def __delayedFinish(self, delay):
        logging.info("finishing in %s seconds" % delay)

        await asyncio.sleep(delay)

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.error('Failed to set finish flag due to lack of receiver access')


    async def generateStatusResponse(self):
                nSchedulingJobs = nFailedJobs = nFinishedJobs = nExecutingJobs = 0

                for job in self.jobs.values():
                    if job.status in [JobState.QUEUED, JobState.SCHEDULED]:
                        nSchedulingJobs += 1
                    elif job.status in [JobState.EXECUTING]:
                        nExecutingJobs += 1
                    elif job.status in [JobState.FAILED, JobState.OMITTED]:
                        nFailedJobs += 1
                    elif job.status in [JobState.CANCELED, JobState.SUCCEED]:
                        nFinishedJobs += 1

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
                        'TotalNodes': self.totalResources.totalNodes,
                        'TotalCores': self.totalResources.totalCores,
                        'UsedCores': self.totalResources.usedCores,
                        'FreeCores': self.totalResources.freeCores,
                    }, 'JobStats': {
                        'TotalJobs': len(self.jobs),
                        'InScheduleJobs': nSchedulingJobs,
                        'FailedJobs': nFailedJobs,
                        'FinishedJobs': nFinishedJobs,
                        'ExecutingJobs': nExecutingJobs,
                    }})
