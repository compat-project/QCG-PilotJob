from qcg.appscheduler.allocation import Allocation, NodeAllocation
from qcg.appscheduler.joblist import JobResources
from qcg.appscheduler.errors import *

class Node:
	def __init__(self, name = None, totalCores = 0, used = 0):
		self.__name = name
		self.__totalCores = totalCores
		self.__usedCores = used

	def __getName(self):
		return self.__name

	def __getTotalCores(self):
		return self.__totalCores

	def __setTotalCores(self, total):
		assert total >= 0 and total >= self.__usedCores
		self.__totalCores = total

	def __getUsedCores(self):
		return self.__usedCores

	def __setUsedCores(self, used):
		assert used > 0 and used <= total
		self.__usedCores = used

	def __getFreeCores(self):
		return self.__totalCores - self.__usedCores

	def __str__(self):
		return "%s %d (%d used)" % (self.__name, self.__totalCores, self.__usedCores)


	"""
	Allocate maximum number of cores on a node.

	Args:
		cores (int): maximum number of cores to allocate

	Returns:
		int: number of allocated cores
	"""
	def allocate(self, cores):
		allocated = min(cores, self.free)
		self.__usedCores += allocated

		return allocated


	"""
	Release specified number of cores on a node.

	Args:
		cores (int): number of cores to release

	Raises:
		InvalidResourceSpec: when number of cores to release exceeds number of of
		  used cores.
	"""
	def release(self, cores):
		if cores > self.__usedCores:
			raise InvalidResourceSpec()

		self.__usedCores -= cores


	name  = property(__getName, None, None, "name of the node")
	total = property(__getTotalCores, __setTotalCores, None, "total number of cores")
	used  = property(__getUsedCores, __setUsedCores, None, "number of allocated cores")
	free  = property(__getFreeCores, None, None, "number of available cores")



class Resources:

	def __init__(self, nodes = None):
		self.__nodes = nodes
		self.__totalCores = 0
		self.__usedCores = 0

#		print "initializing %d nodes" % len(nodes)
		self.__computeCores()

	def __computeCores(self):
		total, used = 0, 0
		if self.__nodes:
			for node in self.__nodes:
				total += node.total
				used += node.used

		self.__totalCores = total
		self.__usedCores = used

	def __getNodes(self):
		return self.__nodes

	def __getTotalCores(self):
		return self.__totalCores

	def __getUsedCores(self):
		return self.__usedCores

	def __getFreeCores(self):
		return self.__totalCores - self.__usedCores
	

	"""
	Create allocation with given number of cores.
	The cores will be allocated in a linear method.

	Args:
		cores (int): requested number of cores

	Returns:
		Allocation: created allocation 
		None: not enough free resources

	Raises:
		NotSufficientResources: when there are not enough resources avaiable
		InvalidResourceSpec: when the cores < 0
	"""
	"""
	def createAllocation(self, cores):
		if cores <= 0:
			raise InvalidResourceSpec()

		if self.__totalCores < cores:
			raise NotSufficientResources()

		if self.freeCores < cores:
			return None

		allocation = Allocation()
		allocatedCores = 0
		for node in self.__nodes:
			nodeCores = node.allocate(cores - allocatedCores)

			if nodeCores > 0:
				allocation.addNode(NodeAllocation(node, nodeCores))

				allocatedCores += nodeCores
				self.__usedCores += nodeCores

				if allocatedCores == cores:
					break

		# this should never happen
		if allocatedCores != cores:
			raise NotSufficientResources()

		return allocation
	"""


	"""
	Create allocation with maximum number of cores from given range.
	The cores will be allocated in a linear method.

	Args:
		min_cores (int): minimum requested number of cores
		max_cores (int): maximum requested number of cores

	Returns:
		Allocation: created allocation 
		None: not enough free resources

	Raises:
		NotSufficientResources: when there are not enough resources avaiable
		InvalidResourceSpec: when the min_cores < 0 or min_cores > max_cores
	"""
	def createAllocation(self, min_cores, max_cores = None):
		if max_cores == None:
			max_cores = min_cores

		if min_cores <= 0 or min_cores > max_cores:
			raise InvalidResourceSpec()

		if self.__totalCores < min_cores:
			raise NotSufficientResources()

		if self.freeCores < min_cores:
			return None

		allocation = Allocation()
		allocatedCores = 0
		for node in self.__nodes:
			nodeCores = node.allocate(max_cores - allocatedCores)

			if nodeCores > 0:
				allocation.addNode(NodeAllocation(node, nodeCores))

				allocatedCores += nodeCores
				self.__usedCores += nodeCores

				if allocatedCores == max_cores:
					break

		# this should never happen
		if allocatedCores < min_cores or allocatedCores > max_cores:
			self.releaseAllocation(allocation)
			raise NotSufficientResources()

		return allocation


	"""
	Relase allocated resources.

	Args:
		alloc (Allocation): allocation to release

	Raises:
		InvalidResourceSpec: when number of cores to release on a node is greater 
		  than number of used cores.
	"""
	def releaseAllocation(self, alloc):
		for node in alloc.nodeAllocations:
			node.node.release(node.cores)
			self.__usedCores -= node.cores


	"""
	Allocate cores on a given node.

	Args:
		node (Node): on which node alloc cores
		cores (int): number of nodes

	Raises:
		NotSufficientResources: where there are not enough resources available
	"""
	def allocCores(self, node, cores):
		# !!! should we check if node belongs to the resources ???
		if node.free < cores:
			raise NotSufficientResources()

		node.alloc(cores)


	def __str__(self):
		header = '%d (%d used) cores\n' % (self.__totalCores, self.__usedCores)
		return header + '\n'.join([str(node) for node in self.__nodes])
#		if self.__nodes:
#			for node in self.__nodes:
#				result.join("\n%s" % node)
#		return result


	nodes = property(__getNodes, None, None, "list of a nodes")
	totalCores = property(__getTotalCores, None, None, "total number of cores")
	usedCores = property(__getUsedCores, None, None, "used number of cores")
	freeCores = property(__getFreeCores, None, None, "free number of cores")

