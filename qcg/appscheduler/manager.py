from qcg.appscheduler.joblist import Job, JobResources, JobList, JobState
from qcg.appscheduler.scheduler import Scheduler
from qcg.appscheduler.executor import Executor
from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec, IllegalJobDescription

import asyncio
import uuid
import logging
from asyncio.queues import Queue



class SchedulingJob:

	"""
	Data necessary for scheduling job.
	"""
	def __init__(self, manager, job):
		assert job is not None
		assert manager is not None

		self.__manager = manager
		self.job = job

		self.__isFeasible = True
		self.__afterJobs = set()

		if job.hasDependencies():
			for jobId in job.dependencies.after:
				if not self.__manager.jobList.exist(jobId):
					raise IllegalJobDescription("Dependency job %s not registered" % jobId)

				self.__afterJobs.add(jobId)

			self.checkDependencies()


	"""
	Update dependency state.
	Check all dependent jobs and update job's ready (and possible feasible) status.
	"""
	def checkDependencies(self):
		logging.info("updating dependencies of job %s ..." % self.job.name)

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

			logging.info("#%d dependency (%s feasible) jobs after update of job %s" % (len(self.__afterJobs), str(self.__isFeasible), self.job.name))


	"""
	Check if job can be executed.
	Job that dependency will never be satisfied (dependent jobs failed) should never be run.

	Returns:
		bool: does the job can be run in future
	"""
	@property
	def isFeasible(self):
		return self.__isFeasible


	"""
	Check if job can be scheduled and executed.
	Jobs with not met dependencies can not be scheduled.

	Returns:
		bool: is job ready for scheduling and executing
	"""
	@property
	def isReady(self):
		return len(self.__afterJobs) == 0
	

class JobStateCB:
	
	def __init__(self, cb, *args):
		self.cb = cb
		self.args = args


class Manager:
	
	"""
	Manager of jobs to execution.
	The incoming jobs are scheduled and executed.

	Args:
		resources (Resources): available resources
	"""
	def __init__(self, resources):
		assert resources != None

		self.resources = resources
		self.__scheduler = Scheduler(self.resources)
		self.__executor = Executor(self)
		self.jobList = JobList()

		self.__scheduleQueue = []

		self.__jobStatesCbs = {}


	"""
	Do schedule loop.
	Get jobs from schedule queue, check if they have workflow dependency meet and if yes,
	try to create allocation. The allocated job's are sent to executor.

	"""
	def __scheduleLoop(self):
		newScheduleQueue = []

		logging.info("scheduling loop with %d jobs in queue" % (len(self.__scheduleQueue)))

		for schedJob in self.__scheduleQueue:
			schedJob.checkDependencies()

			if not schedJob.isFeasible:
				# job will never be ready
				logging.info("job %s not feasible - omitting" % (schedJob.job.name))
				self.__changeJobState(schedJob.job, JobState.OMITTED)
			else:
				if schedJob.isReady:
					logging.info("job %s is ready" % (schedJob.job.name))
					# job is ready - try to find resources
					try:
						allocation = self.__scheduler.allocateJob(schedJob.job.resources)

						if allocation is not None:
							logging.info("found resources for job %s" % (schedJob.job.name))

							# allocation has been created - execute job
							self.__changeJobState(schedJob.job, JobState.EXECUTING)
							self.__executor.execute(allocation, schedJob.job)
						else:
							logging.info("missing resources for job %s" % (schedJob.job.name))
							# missing resources
							newScheduleQueue.append(schedJob)
					except (NotSufficientResources, InvalidResourceSpec) as e:
						# jobs will never schedule
						logging.warning("Job %s scheduling failed - %s" % (schedJob.job.name, str(e)))
				else:
					newScheduleQueue.append(schedJob)

		self.__scheduleQueue = newScheduleQueue


	"""
	Invoked to change job status.
	Any notification should be called from this method.

	Args:
		job (Job): job that changed status
		status (JobState): target job state
	"""
	def __changeJobState(self, job, state, errorMsg = None):
		job.state = state

		if errorMsg is not None:
			job.appendMessage(errorMsg)

		self.__fireJobStateNotifies(job.name, state)


	"""
	Invoked to signal job finished.
	Allocation made for the job should be released.

	Args:
		job (Job): job that finished
		allocation (Allocation): allocation created for the job
		exitCode (int): job exit code
		errorMsg (str): an optional error message
	"""
	def jobFinished(self, job, allocation, exitCode, errorMsg):
		state = JobState.SUCCEED

		if exitCode != 0:
			state = JobState.FAILED

		self.__changeJobState(job, state, errorMsg)
		self.__scheduler.releaseAllocation(allocation)
		self.__scheduleLoop()


	"""
	Create task with callback functions call registered for job state changes.
	A new asyncio task is created which call all registered callbacks in not defined order.

	Args:
		jobId (str): job identifier
		state (JobState): new job status
	"""
	def __fireJobStateNotifies(self, jobId, state):
		if len(self.__jobStatesCbs) > 0:
			logging.info("notifies callbacks about %s job status change %s" % (jobId, state))
			self.__processTask = asyncio.ensure_future(self.__callCallbacks(
				jobId, state, self.__jobStatesCbs.values()
				))


	"""
	Call job state change callback function with given arguments.

	Args:
		jobId (str): job identifier
		state (JobState): new job status
		cbs ([]function): callback functions
	"""
	async def __callCallbacks(self, jobId, state, cbs):
		if cbs is not None:
			for cb in cbs:
				try:
					cb.cb(jobId, state, *cb.args)
				except Exception as e:
					logging.exception("Callback function failed: %s" % (str(e)))


	"""
	Unregister callback function for job state changes.

	Args:
		id (str): the callback function identifier returned by 'registerNotifier' function

	Returns:
		bool: true if function unregistered successfully, and false if given identifier
		   has not been found
	"""
	def unregisterNotifier(self, id):
		if id in self.__jobStatesCbs:
			del self.__jobStatesCbs[id]
			return True

		return False


	"""
	Register callback function for job state changes.
	The registered function will be called for all job state changes.

	Args:
		jobStateCb (function): should accept two arguments - job name and new state

	Returns:
		str: identifier of registered callback, which can be used to unregister
		  callback or None if callback function is missing or is invalid
	"""
	def registerNotifier(self, jobStateCb, *args):
		if jobStateCb is not None:
			id = uuid.uuid4()
			self.__jobStatesCbs[id] = JobStateCB(jobStateCb, *args)

			return id

		return None


	"""
	Enqueue job to execution.

	Args:
		job (Job): job description

	Raises:
		JobAllreadyExist: when job with the same name was enqued earlier.
	"""
	def enqueue(self, jobs):
		if jobs is not None:
			for job in jobs:
				self.jobList.add(job)
				self.__scheduleQueue.append(SchedulingJob(self, job))

			self.__scheduleLoop()


	async def waitForFinish(self):
		while len(self.__scheduleQueue) > 0:
			await asyncio.sleep(1)
		await self.__executor.waitForUnfinished()


	def getExecutor(self):
		return self.__executor
