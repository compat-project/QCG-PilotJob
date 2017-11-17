from enum import Enum, auto
from datetime import datetime, timedelta
import json
import re

from qcg.appscheduler.errors import JobAlreadyExist, IllegalResourceRequirements, \
		IllegalJobDescription


class JobState(Enum):
	QUEUED = auto()
	EXECUTING = auto()
	SUCCEED = auto()
	FAILED = auto()
	CANCELED = auto()

	def isFinished(self):
		return self in [SUCCEED, FAILED, CANCELED]
	
	
class JobExecution:
	def __init__(self, exec, args = None, env = None, wd = None, \
			stdout = None, stderr = None):
		if exec is None:
			raise IllegalJobDescription("Job execution not defined")

		self.exec = exec

		self.args = []
		self.env = {}

		self.stdout = stdout
		self.stderr = stderr

		if args is not None:
			if not isinstance(args, list):
				raise IllegalJobDescription("Execution arguments must be an array")
			self.args = args

		if env is not None:
			if not isinstance(env, dict):
				raise IllegalJobDescription("Execution environment must be an dictionary")
			self.env = env

		self.wd = wd


	def toJSON(self):
		return json.dumps(self.__dict__, indent=2)


class ResourceSize:
	def __init__(self, exact = None, min = None, max = None):
		if exact is not None and (min is not None or max is not None):
			raise IllegalResourceRequirements(
					"Exact number of resources defined with min/max number")

		if max is not None and min is not None and min > max:
			raise IllegalResourceRequirements("Maximum number greater than minimal")

		if exact is None and min is None and max is None:
			raise IllegalResourceRequirements("No resources defined")

		if (exact is not None and exact < 0) or (min is not None and min < 0) or (max is not None and max < 0):
			raise IllegalResourceRequirements("Neative number of resources")
	
		self.__exact = exact
		self.__min = min
		self.__max = max

	@property
	def exact(self):
		return self.__exact

	@property
	def min(self):
		return self.__min

	@property
	def max(self):
		return self.__max

	@property
	def range(self):
		return (self.__min, self.__max)

	def isExact(self):
		return self.__exact is not None

	def toDict(self):
		return { 'exact': self.__exact, 'min': self.__min, 'max': self.__max }

	def toJSON(self):
		return json.dumps(self.toDict())


class JobResources:
	__wtRegex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

	def __parseWt(self, wt):
		parts = None

		try:
			parts = self.__wtRegex.match(wt)
		except:
			raise IllegalResourceRequirements("Wrong wall time format")


		if not parts:
			raise IllegalResourceRequirements("Wrong wall time format")

		try:
			parts = parts.groupdict()
			timeParams = { }
			for name, param in parts.items():
				if param:
					timeParams[name] = int(param)

			return timedelta(**timeParams)
		except:
			raise IllegalResourceRequirements("Wrong wall time format")


	def __init__(self, numCores = None, numNodes = None, wt = None):
		if numCores is None and numNodes is None:
			raise IllegalResourceRequirements("No resources defined")

		if numCores is not None:
			if isinstance(numCores, int):
				numCores = ResourceSize(numCores)
			elif isinstance(numCores, dict):
				numCores = ResourceSize(**numCores)
			elif not isinstance(numCores, ResourceSize):
				raise IllegalJobDescription("Wrong definition of number of cores (%s)" % (type(numCores).__name__))

		if numNodes is not None:
			if isinstance(numNodes, int):
				numNodes = ResourceSize(numNodes)
			elif isinstance(numNodes, dict):
				numNodes = ResourceSize(**numNodes)
			elif not isinstance(numNodes, ResourceSize):
				raise IllegalJobDescription("Wrong definition of number of nodes (%s)" % (type(numNodes).__name__))
		
		if wt is not None:
			self.wt = self.__parseWt(wt)
		else:
			self.wt = None

		self.numCores = numCores
		self.numNodes = numNodes

	def hasNodes(self):
		return self.numNodes is not None

	def hasCores(self):
		return self.numCores is not None

	@property
	def cores(self):
		return self.numCores

	@property
	def nodes(self):
		return self.numNodes

	def toDict(self):
		result = { }
		if self.hasCores():
			result['numCores'] = self.numCores.toDict()

		if self.hasNodes():
			result['numNodes'] = self.numNodes.toDict()

		return result

	def toJSON(self):
		return json.dumps( self.toDict(), indent=2)


class JobFiles:
	def __validateFileList(self, fileList, errorMessage):
		if not isinstance(fileList, list):
			raise IllegalJobDescription(errorMessage)

		for file in fileList:
			if not isinstance(file, str) and not isinstance(file, list):
				raise IllegalJobDescription(errorMessage)

			if isinstance(file, list):
				if (len(file) < 1 or len(file) > 2):
					raise IllegalJobDescription(errorMessage)

				if not isinstance(file[0], str) or (len(file) > 1 and not isinstance(file[1], str)):
					raise IllegalJobDescription(errorMessage)


	def __init__(self, stageIn = None, stageOut = None):
		self.stageIn = []
		self.stageOut = []
		
		if stageIn is not None:
			self.__validateFileList(stageIn, "Stage in element of array must be an name or 2-element touple")
			self.stageIn  = stageIn

		if stageOut is not None:
			self.__validateFileList(stageOut, "Stage out element of array must be an name or 2-element touple")
			self.stageOut = stageOut

	def toDict(self):
		return self.__dict__

	def toJSON(self):
		return json.dumps(self.toDict(), indent=2)


class JobDependencies:
	def __validateJobList(self, jobList, errorMessage):
		if not isinstance(jobList, list):
			raise IllegalJobDescription(errorMessage)

		for jobName in jobList:
			if not isinstance(jobName, str):
				raise IllegalJobDescription(errorMessage)


	def __init__(self, after = None):
		self.after = []

		if after is not None:
			if isinstance(after, str):
				after = [ after ]
			else:
				self.__validateJobList(after, "Dependency task's list must be an array of job names")

			self.after = after

	def hasDependencies(self):
		return self.after is not None and len(self.after) > 0

	def toDict(self):
		return self.__dict__

	def toJSON(self):
		return json.dumps(self.toDict(), indent=2)


class Job:
	def __init__(self, name, execution, resources, files = None, dependencies = None):
		if name is None:
			raise IllegalJobDescription("Job name not defined")
		self.__name = name

		if execution is None or not isinstance(execution, JobExecution):
			raise IllegalJobDescription("Job execution not defined or wrong type")
		self.__execution = execution

		if resources is None or not isinstance(resources, JobResources):
			raise IllegalJobDescription("Job resources not defined or wrong type")
		self.__resources = resources

		if files is not None and not isinstance(files, JobFiles):
			raise IllegalJobDescription("Job files wrong type")
		self.__files = files

		if dependencies is not None and not isinstance(dependencies, JobDependencies):
			raise IllegalJobDescription("Job dependencies wrong type")
		self.__dependencies = dependencies

		self.__history = []

		# history must be initialized before
		self.state = JobState.QUEUED

	@property
	def name(self):
		return self.__name
	
	@property
	def state(self):
		return self.__state

	@property
	def history(self):
		return self.__history

	@state.setter
	def state(self, state):
		assert isinstance(state, JobState), "Wrong state type"
		self.__history.append((state, datetime.now()))
		self.__state = state


	def hasDependencies(self):
		return self.__dependencies is not None and len(self.__dependencies.after) > 0


class JobList:
	def __init__(self):
		self.__jmap = {}
	

	def add(self, job):
		assert isinstance(job, Job), "Wrong job type"

		if self.exist(job.name):
			raise JobAlreadyExist(job.name)

		self.__jmap[job.name] = job


	def exist(self, jobName):
		return jobName in self.__jmap

	def get(self, jobName):
		return self.__jmap[jobName]

