import unittest


import context
from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.scheduler import Scheduler
from qcg.appscheduler.allocation import NodeAllocation, Allocation
from qcg.appscheduler.joblist import JobResources, ResourceSize


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
	
	
	def test_CoresExact(self):
		res = self.createLocalResources()
		cores = 2

		sched=Scheduler(res)

		jobRes = JobResources(numCores = ResourceSize(exact = cores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, cores)
		self.assertEqual(res.usedCores, cores)
		self.assertEqual(res.freeCores, self.__ncores - cores)
		sched.releaseAllocation(allocation)

		cores = 6
		jobRes = JobResources(numCores = ResourceSize(exact = cores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, cores)
		self.assertEqual(res.usedCores, cores)
		self.assertEqual(res.freeCores, self.__ncores - cores)
		self.assertTrue(len(allocation.nodeAllocations) == 3)
		sched.releaseAllocation(allocation)


	def test_CoresRange(self):
		res = self.createLocalResources()
		minCores = 2
		maxCores = 4
		used = 0

		sched=Scheduler(res)

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, maxCores)
		used += maxCores
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == 2)


		minCores = 2
		maxCores = 3

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)

		self.assertEqual(allocation.cores, maxCores)
		used += maxCores
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == 1)

		minCores = 1
		maxCores = 3

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, minCores)
		used += minCores
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == 1)

		minCores = 1
		maxCores = 3

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNone(allocation)


	def test_CoresRangeMax(self):
		res = self.createLocalResources()

		sched=Scheduler(res)

		try:
			minCores = self.__ncores + 2
			maxCores = minCores + 2
			jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
			allocation = sched.allocateJob(jobRes)
			self.fail("Allocation created with wrong resource requirements")
		except NotSufficientResources:
			pass
		except Exception:
			self.fail("Wrong exception")

		minCores = 1
		maxCores = 40
		used = 0
		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, self.__ncores)
		used += self.__ncores
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == self.__nnodes)


	def test_CoresRangeMin(self):
		res = self.createLocalResources()
		minCores = 1
		used = 0

		sched=Scheduler(res)

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = self.__ncores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, self.__ncores)
		used += self.__ncores
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == self.__nnodes)


	def test_CoresRangeEqual(self):
		res = self.createLocalResources()
		minCores = 4
		maxCores = 4
		used = 0

		sched=Scheduler(res)

		jobRes = JobResources(numCores = ResourceSize(min = minCores, max = maxCores))
		allocation = sched.allocateJob(jobRes)

		self.assertIsNotNone(allocation)
		self.assertEqual(allocation.cores, minCores)
		used += minCores 
		self.assertEqual(res.usedCores, used)
		self.assertEqual(res.freeCores, self.__ncores - used)
		self.assertTrue(len(allocation.nodeAllocations) == 2)


	def test_NodesSimple(self):
		res = self.createLocalResources()
		nnodes = 2

		sched=Scheduler(res)

		jobRes = JobResources(numNodes = ResourceSize(nnodes))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation)
		self.assertTrue(len(allocation.nodeAllocations) == 2)


	def test_NodesRange(self):
		res = self.createLocalResources()
		sched=Scheduler(res)

		min_nodes = 1
		max_nodes = 2
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation)
		self.assertTrue(len(allocation.nodeAllocations) == 2)

		min_nodes = 1
		max_nodes = 2
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation)
		self.assertTrue(len(allocation.nodeAllocations) == 1)


	def test_NodesRangeMax(self):
		res = self.createLocalResources()
		sched=Scheduler(res)

		try:
			min_nodes = self.__nnodes + 1
			max_nodes = min_nodes + 2
			jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
			allocation = sched.allocateJob(jobRes)
			self.fail("Allocation created with wrong resource requirements")
		except NotSufficientResources:
			pass
		except Exception:
			self.fail("Wrong exception")

		min_nodes = 1
		max_nodes = 6
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation)
		self.assertTrue(len(allocation.nodeAllocations) == self.__nnodes)
		self.assertEqual(res.usedCores, self.__ncores)
		self.assertEqual(res.freeCores, 0)

		min_nodes = 1
		max_nodes = 6
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation = sched.allocateJob(jobRes)
		self.assertIsNone(allocation)


	def test_NodesRangeEqual(self):
		res = self.createLocalResources()
		sched=Scheduler(res)

		min_nodes = 1
		max_nodes = 1
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation1 = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation1)
		self.assertTrue(len(allocation1.nodeAllocations) == 1)
		self.assertEqual(res.usedCores, 2)

		min_nodes = 1
		max_nodes = 1
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation2 = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation2)
		self.assertTrue(len(allocation2.nodeAllocations) == 1)
		self.assertEqual(res.usedCores, 4)

		sched.releaseAllocation(allocation1)
		sched.releaseAllocation(allocation2)

		min_nodes = 3
		max_nodes = 3
		jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
		allocation3 = sched.allocateJob(jobRes)
		self.assertIsNotNone(allocation3)
		self.assertTrue(len(allocation3.nodeAllocations) == 3)

		sched.releaseAllocation(allocation3)

		try:
			min_nodes = 4
			max_nodes = 4
			jobRes = JobResources(numNodes = ResourceSize(min = min_nodes, max = max_nodes))
			allocation4 = sched.allocateJob(jobRes)
		except NotSufficientResources:
			self.assertEqual(res.usedCores, 0)
			self.assertEqual(res.freeCores, self.__ncores)
		except Exception:
			self.fail("Wrong exception")


	def test_NodesRangeInvalid(self):
		res = self.createLocalResources()
		sched=Scheduler(res)

		try:
			min_nodes = 1
			jobRes = JobResources(numNodes = ResourceSize(min = min_nodes))
			allocation1 = sched.allocateJob(jobRes)
			self.fail("Allocation created with wrong resource requirements")
		except InvalidResourceSpec:
			self.assertEqual(res.usedCores, 0)
			self.assertEqual(res.freeCores, self.__ncores)
		except Exception:
			self.fail("Wrong exception")

		try:
			max_nodes = 1
			jobRes = JobResources(numNodes = ResourceSize(max = max_nodes))
			allocation1 = sched.allocateJob(jobRes)
			self.fail("Allocation created with wrong resource requirements")
		except InvalidResourceSpec:
			self.assertEqual(res.usedCores, 0)
			self.assertEqual(res.freeCores, self.__ncores)
		except Exception:
			self.fail("Wrong exception")


