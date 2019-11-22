import asyncio
import sys
import logging

from qcg.appscheduler.request import RemoveJobReq, ControlReq, StatusReq
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, JobInfoReq, CancelJobReq
from qcg.appscheduler.response import Response, ResponseCode
from qcg.appscheduler.resources import Resources


class GlobalJob:

    def __init__(self, id, status, manager_instance):
        """
        Job information stored at higher level QCG manager.

        :param id:  job identifier
        :param status: current job status
        :param manager_instance: address of the manager instance that schedules & executes job
        """
        self.id = id
        self.status = status
        self.manager_instance = manager_instance


class ManagerInstance:

    def __init__(self, mid, resources, address):
        """
        The information about QCG manager instance available for higher level manager.
        :param id:
        :param resources:
        :param address:
        """
        self.id = mid
        self.resources = resources
        self.address = address


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

        self.__parentManager = parentManager


    def setupInterfaces(self):
        """
        Initialize manager after all incoming interfaces has been started.
        """
        if self.__parentManager:
            try:
                self.registerInParent(self.__parentManager)
            except:
                logging.error('Failed to register manager in parent governor manager: {}'.format(sys.exc_info()[0]))
                raise


    def getHandlerInstance(self):
        return self


    def setReceiver(self, receiver):
        self.__receiver = receiver
        if self.__receiver:
            self.zmq_address = self.__receiver.getZmqAddress()


    def stop(self):
        """
        Cleanup before finish.
        """
        pass


    def updateTotalResources(self):
        self.totalResources.zero()

        for m in self.managers.values():
            self.totalResources.append(m.resources)


    def registerNotifier(self, jobStateCb, *args):
        # TODO: do weed need this functionality for governor manager ???
        pass


    async def __waitForAllJobs(self):
        logging.info('waiting for all jobs to finish')

        #TODO: implement mechanism


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
        if request.command != ControlReq.REQ_CONTROL_CMD_FINISHAFTERLLTASKSDONE:
            return Response.Error('Not supported command "{}" of finish control request'.format(request.command))

        if self.__finishTask is not None:
            return Response.Error('Finish request already requested')

        self.__finishTask = asyncio.ensure_future(self.__waitForAllJobs())


    async def handleSubmitReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

    async def handleJobStatusReq(self, iface, request):
        #TODO: implement mechanism
        return Response.Error('Currently not supported')

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


    async def __delayedFinish(self, delay):
        logging.info("finishing in %s seconds" % delay)

        await asyncio.sleep(delay)

        if self.__receiver:
            self.__receiver.setFinish(True)
        else:
            logging.warning('Failed to set finish flag due to lack of receiver access')
