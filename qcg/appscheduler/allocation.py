

class NodeAllocation:

	"""
	Resource allocation on a single node, contains information about a node,
	along with the number of cores allocated.

	Args:
		node (Node): a node definition
		cores (int): number of allocated cores from this node

	Attributes:
		__node: a node definition
		__cores: number of allocated cores
	"""
	def __init__(self, node, cores):
		assert node 
		assert cores > 0

		self.__node = node
		self.__cores = cores

	"""
	Return number of allocated cores on a node.

	Returns:
		int: number of cores
	"""
	def __getCores(self):
		return self.__cores

	"""
	Return node informations.

	Returns:
		Node: a node information
	"""
	def __getNode(self):
		return self.__node

	"""
	Return a human readable description.

	Returns:
		string: a human readable description
	"""
	def __str__(self):
		return "%d @ %s" % (self.__cores, self.__node.name)
	

	cores = property(__getCores, None, None, "number of cores @ the node")
	node = property(__getNode, None, None, "node")



class Allocation:

	"""
	Resource allocation splited (possible) among many nodes.

	Args:
	
	Attributes:
		__nodes (NodeAllocation[]): list of a single node allocation
		__cores (int): total number of cores on all allocations
	"""
	def __init__(self):
		self.__nodes = []
		self.__cores = 0

	"""
	Add a node allocation.

	Args:
		nodeAllocation (NodeAllocation): description of an allocation on a single
			node
	"""
	def addNode(self, nodeAllocation):
		assert nodeAllocation

		self.__nodes.append(nodeAllocation)
		self.__cores += nodeAllocation.cores

	"""
	Compute total number of cores in an allocation.
	"""
	def __updateCores(self):
		cores = 0
		for node in self.__nodes:
			cores += node.cores
		self.__cores = cores

	"""
	Return total number of cores of an allocation

	Returns:
		int: number of cores
	"""
	def __getCores(self):
		return self.__cores

	"""
	Return a list of node allocations

	Returns:
		NodeAllocation[]: list of node allocations
	"""
	def __getNodeAllocations(self):
		return self.__nodes

	"""
	Return a human readable string

	Returns:
		string: human readable string
	"""
	def __str__(self):
		header = "%d cores @ %d nodes\n" % (self.__cores, len(self.__nodes))
		return header + '\n'.join([str(node) for node in self.__nodes])


	cores = property(__getCores, None, None, "number of cores")
	nodeAllocations = property(__getNodeAllocations, None, None, "nodes")
