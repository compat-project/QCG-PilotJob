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

    def __init__(self, id, status, manager_instance):
        """
        Job information stored at higher level QCG managernode_local_agent_cmd.

        :param id:  job identifier
        :param status: current job status
        :param manager_instance: address of the manager instance that schedules & executes job
        """
        self.id = id
        self.status = status
        self.manager_instance = manager_instance

    def __str__(self):
        return '{} in {} state (from {} manager)'.format(self.id, self.status.name, self.manager_instance)


class PartitionManager:

    def __init__(self, mid, nodeName, startNode, endNode, workDir, governorAddress, auxDir):
        """
        The instance of partition manager.
        The partition manager is an instance of manager that controls part of the allocation (the nodes range
        'startNode'-'endNode' of the Slurm allocation). The instance is launched from governor manager to run
        jobs submited by governor on it's part of the allocation.

        :param mid: partition manager identifier
        :param nodeName: the node where partition manager should be launched
        :param startNode: the start node of the allocation the manager should control
        :param endNode: the end node of the allocation the manager should control
        :param workDir: the working directory of partition manager
        :param governorAddress: the address of the governor manager where partition manager should register
        :param auxDir: the auxiliary directory for partition manager
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
        self.__defaultPartitionManagerArgs = [ ]


    async def launch(self):
        logging.info('launching partition manager {} on node {} to control node numbers {}-{}'.format(
            self.mid, self.nodeName, self.startNode, self.endNode))

        slurm_args = ['-w', self.nodeName, '-N', '1', '-n', '1', '-D', self.workDir]

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
        self.__minShedulingManagers = 1

        # the maximum number of buffered jobs until, when reached the submit request will be responsed with error
        self.__maxBufferedJobs = 1000

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


    def updateTotalResources(self):
        self.totalResources.zero()

        for m in self.managers.values():
            self.totalResources.append(m.resources)


    def registerNotifier(self, jobStateCb, *args):
        # TODO: do weed need this functionality for governor manager ???
        pass


    async def __waitForAllJobs(self):
        logging.info('waiting for all jobs to finish')

        while not self.allJobsFinished():
            await asyncio.sleep(0.2)

        logging.info('All ({}) jobs finished'.format(len(self.jobs)))

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.error('Failed to set finish flag due to lack of receiver access')


    def allJobsFinished(self):
        try:
            return next((job for job in self.jobs.values() if not job.status.isFinished()), None) == None
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
        if len(self.managers) == 0 or self.totalResources.totalCores == 0:
            return Response.Error('Error: no resources available')

        try:
            # validate jobs
            self.__validateJobReqs(request.jobReqs)

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

            if 'iterate' in jobReq:
                # iteration job, split between all available managers
                (start, end) = jobReq['iterate'][0:2]
                iterations = end - start

                currIterStart = start

                submit_reqs = []

                splitPart = int(math.ceil(iterations / len(self.managers)))

                managers_list = [ ]

                for m in self.managers.values():
                    currIterEnd = min(currIterStart + splitPart, end)

                    currJobReq = copy.deepcopy(jobReq)
                    currJobReq['iterate'] = [ currIterStart, currIterEnd ]

                    submit_reqs.append(m.submitJobs([ currJobReq ]))
                    managers_list.append(m)

                    currIterStart = currIterEnd
                    if currIterEnd == end:
                        break

                submit_results = await asyncio.gather(*submit_reqs)
                for idx, result in enumerate(submit_results):
                    m = managers_list[idx]

                    self.__appendNewJobs(result['names'], m)

                    nReqJobs += result['njobs']
                    reqJobNames.extend(result['names'])

            else:
                # single job, send to the manager with lest number of submitted jobs
                manager = min(self.managers.values(), key=lambda m: m.submitted_jobs)

                logging.debug('sending job {} to manager {} ({})'.format(jobReq['name'], manager.id, manager.address))
                submit_result = await manager.submitJobs([ jobReq ])

                self.__appendNewJobs(submit_result['names'], manager)

                nReqJobs += submit_result['njobs']
                reqJobNames.extend(submit_result['names'])

        return (nReqJobs, reqJobNames)


    def __appendNewJobs(self, jobNames, manager):
        """
        Add new jobs to the global registery
        :param jobNames: list of job names
        :param manager: manager instance the jobs has been sent to
        """
        for jobName in jobNames:
            self.jobs[jobName] = GlobalJob(jobName, JobState.QUEUED, manager)


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
        jname = request.params.get('name', 'UNKNOWN')
        job = self.jobs.get(jname, None)
        if not job:
            logging.warning('job notified {} not exist'.format(jname))
            return Response.Error('Job {} unknown'.format(jname))

        newstate = request.params.get('state', 'UNKNOWN')
        if not newstate in JobState.__members__:
            logging.warning('notification for job {} contains unknown state {}'.format(jname, newstate))
            return Response.Error('Job state {} unknown'.format(newstate))

        self.jobs[jname].status = JobState[newstate]
        logging.debug('job state {} successfully update to {}'.format(jname, newstate))
        return Response.Ok('job {} updated'.format(jname))


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

