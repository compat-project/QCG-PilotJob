import unittest


import context
from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.scheduler import Scheduler
from qcg.appscheduler.allocation import NodeAllocation, Allocation


class TestScheduler(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass

	def createLocalResources(self):
		node_names=['local1', 'local2', 'local3']
		cores_num=[2, 2, 4]

		self.__nnodes = len(node_names)
		self.__ncores = sum(cores_num)

		if len(node_names) != len(cores_num):
			raise Exception("failed to parse local env: number of nodes (%d) mismatch number of cores (%d)" % (len(nodes), len(cores)))
		
		nodes = []
		for i in range(0, len(node_names)):
			nodes.append(Node(node_names[i], cores_num[i], 0))

		return Resources(nodes)
	
	
	def test_SchedulerSimple(self):
		res = self.createLocalResources()
		cores = 2

		sched=Scheduler(res)
		allocation = sched.allocateCores(cores)

		self.assertEqual(allocation.cores, cores)
		self.assertEqual(res.usedCores, cores)
		self.assertEqual(res.freeCores, self.__ncores - cores)


	def test_SchedulerMultiAllocations(self):
		res = self.createLocalResources()
		cores = [ 2, 2, 3, 1 ]
		allocated = 0
		allocations = []

		sched=Scheduler(res)

		for idx in range(len(cores)):
			allocations.append(sched.allocateCores(cores[idx]))

			allocated += cores[idx]

			self.assertEqual(allocations[idx].cores, cores[idx])
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)

		for idx in range(len(cores)):
			sched.releaseAllocation(allocations[idx])

			allocated -= cores[idx]
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)
	
		self.assertEqual(res.usedCores, 0)
		self.assertEqual(res.freeCores, self.__ncores)


	def test_SchedulerRangeAllocations(self):
		res = self.createLocalResources()
		cores = [
				{ 'min': 2, 'max': 3 },
				{ 'min': 2, 'max': 3 },
				{ 'min': 1, 'max': 1 } ]
		allocated = 0
		allocations = []

		sched=Scheduler(res)

		for idx in range(len(cores)):
			core_spec = cores[idx]

			allocations.append(sched.allocateCores(core_spec['min'], core_spec['max']))

			allocated += core_spec['max']

			self.assertEqual(allocations[idx].cores, core_spec['max'])
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)

		for idx in range(len(cores)):
			core_spec = cores[idx]

			sched.releaseAllocation(allocations[idx])

			allocated -= core_spec['max']
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)
	
		self.assertEqual(res.usedCores, 0)
		self.assertEqual(res.freeCores, self.__ncores)


	def test_SchedulerNotEnoughResources(self):
		res = self.createLocalResources()
		cores = [ 3, 3, 1, 1 ]
		allocated = 0
		allocations = []

		sched=Scheduler(res)

		for idx in range(len(cores)):
			allocations.append(sched.allocateCores(cores[idx]))

			allocated += cores[idx]

			self.assertEqual(allocations[idx].cores, cores[idx])
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)

		self.assertIsNone(sched.allocateCores(1))

		try:
			sched.allocateCores(self.__ncores + 1)
			self.fail("Allocation created with wrong resource requirements")
		except NotSufficientResources:
			pass
		except Exception:
			self.fail("Wrong exception")

		try:
			sched.allocateCores(-2)
			self.fail("Allocation created with wrong resource requirements")
		except InvalidResourceSpec:
			pass
		except Exception:
			self.fail("Wrong exception at invalid resource specification")

		for idx in range(len(cores)):
			sched.releaseAllocation(allocations[idx])

			allocated -= cores[idx]
			self.assertEqual(res.usedCores, allocated)
			self.assertEqual(res.freeCores, self.__ncores - allocated)
	
		self.assertEqual(res.usedCores, 0)
		self.assertEqual(res.freeCores, self.__ncores)

