import asyncio
import json
import logging
from enum import Enum, auto

from qcg.appscheduler.manager import Manager
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, CancelJobReq
from qcg.appscheduler.request import ListJobsReq, ResourcesInfoReq
from qcg.appscheduler.joblist import Job, JobResources, JobList, JobState
from qcg.appscheduler.response import Response
from qcg.appscheduler.errors import InvalidRequest


class ResponseStatus(Enum):
	UNKNOWN = auto()
	ERROR = auto()
	SUCCESS = auto()



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

	'''
	Receiver listen on interfaces, valide requests and pass them to manager.
	Some requests, e.g. job status and some statistics may be handled without
	communication with manager.

	Args:
		manager (Manager): jobs manager 
		ifaces (Interface[]): list of interfaces
	'''
	def __init__(self, manager, ifaces):
		assert ifaces is not None and isinstance(ifaces, list) and len(ifaces) > 0

		self.__manager = manager
		self.__ifaces = ifaces
		self.__jobList = JobList()
		self.__tasks = []
		self.__reqEnv = {
				'resources': manager.resources
				}
		self.__handlers = {
				SubmitReq:			self.__handleSubmitReq,
				JobStatusReq:		self.__handleJobStatusReq,
				CancelJobReq:		self.__handleCancelJobReq,
				ListJobsReq:		self.__handleListJobsReq,
				ResourcesInfoReq:	self.__handleResourcesInfoReq
				}

	
	'''
	Handling an interface.
	'''
	async def __listen(self, iface):
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


	'''
	Handle single request.
	The proper handler for given request type should be found (in handlers map) and
	called.

	Args:
		iface (Interface): interface which received request
		request (Request): parsed request data

	Returns:
		Response: response that should be returned to user
	'''
	async def __handleRequest(self, iface, request):
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


	'''
	Validate incoming request.
	In this first step only form of JSON format is validated along with some basic checks.
	More specific validation should be provided on further steps.

	Args:
		reqData (dict): request data

	Returns:
		ValidateResponse: validation result
	'''
	def __validate(self, reqData):
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


	'''
	Start listening on interfaces.
	This method creates asynchronic tasks and returns. To stop created tasks, method 'stop'
	must be called.
	'''
	def run(self):
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
		

	'''
	Stop all listening on interfaces.
	'''
	def stop(self):
		for task in self.__tasks:
			if task is not None:
				task.cancel()

		self.__tasks = []


	'''
	Handlder for job submission.
	Before job will be submited the in-depth validation will be proviede, e.g.: job name
	uniqness.

	Args:
		iface (Interface): interface which received request
		request (SubmitJobReqest): submit request data

	Returns:
		Response: the response data
	'''
	async def __handleSubmitReq(self, iface, request):
		logging.info("Handling submit request from %s iface" % (iface.__class__.__name__))

		for job in request.jobs:
			# verify job name uniqness
			if self.__jobList.exist(job.name):
				return Response.Error('Job %s already exist' % (job.name))

		for job in request.jobs:
			# add job to the job list
			self.__jobList.add(job)

		# enqueue job in the manager
		self.__manager.enqueue(request.jobs)

		return Response.Ok('%d jobs submited' % (len(request.jobs)))


	'''
	Handler for job status checking.

	Args:
		iface (Interface): interface which received request
		request (JobStatusReq): job status request

	Returns:
		Response: the response data
	'''
	async def __handleJobStatusReq(self, iface, request):
		logging.info("Handling job status request from %s iface" % (iface.__class__.__name__))

		job = self.__jobList.get(request.jobName)

		if job is None:
			return Response.Error('Job %s doesn\'t exist' % (request.jobName))

		return Response.Ok(data = {
			'jobName': request.jobName,
			'status': str(job.state)
			})


	async def __handleCancelJobReq(self, iface, request):
		logging.info("Handling cancel job from %s iface" % (iface.__class__.__name__))

		job = self.__jobList.get(request.jobName)

		if job is None:
			return Response.Error('Job %s doesn\'t exist' % (request.jobName))

		return Response.Error('Cancel job is not supported')


	async def __handleListJobsReq(self, iface, request):
		logging.info("Handling list jobs info from %s iface" % (iface.__class__.__name__))

		jobNames = self.__jobList.jobs()

		logging.info("got %s jobs from list" % (str(len(jobNames))))
		return Response.Ok(data = {
				'length': len(jobNames),
				'names': list(jobNames)
			})


	async def __handleResourcesInfoReq(self, iface, request):
		logging.info("Handling resources info from %s iface" % (iface.__class__.__name__))

		resources = self.__manager.resources
		return Response.Ok(data = {
				'totalNodes': len(resources.nodes),
				'totalCores': resources.totalCores,
				'usedCores': resources.usedCores,
				'freeCores': resources.freeCores
			})

