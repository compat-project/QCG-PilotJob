from qcg.appscheduler.errors import *
from qcg.appscheduler.joblist import JobResources


class Scheduler:

	"""
	Create allocation with given number of cores.

	Args:
		cores (int): requested number of cores

	Returns:
		Allocation: created allocation 
		None: not enough free resources

	Raises:
		NotSufficientResources: when there are not enough resources avaiable
	"""
	"""
	def createAllocation(self, cores):
		alloc = self.__resources.createAllocation(cores)

		if alloc is not None:
			self.__activeAllocations.add(alloc)

		return alloc
	"""


	"""
	Create allocation with given number of cores.

	Args:
		min_cores (int): minimum requested number of cores
		max_cores (int): maximum requested number of cores

	Returns:
		Allocation: created allocation 
		None: not enough free resources

	Raises:
		NotSufficientResources: when there are not enough resources avaiable
	"""
	def createAllocation(self, min_cores, max_cores = None):
		alloc = self.__resources.createAllocation(min_cores, max_cores)

		if alloc is not None:
			self.__activeAllocations.add(alloc)

		return alloc


	"""
	Create allocation for job with given resources.

	Args:
		resources (JobResources): job's resource requirements

	Returns:
		Allocation: created allocation 
		None: not enough free resources

	Raises:
		NotSufficientResources: when there are not enough resources avaiable
	"""
	def allocateJob(self, resources):
#		alloc = self.__resources.createAllocation(resources)

		if alloc is not None:
			self.__activeAllocations.add(alloc)

		return alloc


	"""
	Release resources assigned for the specificated allocation.

	Args:
		alloc (Allocation): allocation to release

	Raises:
		InvalidAllocation: when the allocation is not registered in the scheduler (it
		  might be released earlier)
	"""
	def releaseAllocation(self, alloc):
		if alloc not in self.__activeAllocations:
			raise InvalidAllocation()

		self.__activeAllocations.remove(alloc)
		self.__resources.releaseAllocation(alloc)


	"""
	Resource orchestration.

	Args:
		resources (Resources): available resources

	Attributes:
		__resources (Resources): status of available resources
	"""
	def __init__(self, resources):
		assert resources != None

		self.__resources = resources
		self.__activeAllocations = set()


