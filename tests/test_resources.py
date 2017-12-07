import unittest


import context
from qcg.appscheduler.resources import Node, Resources
from appschedulertest import AppSchedulerTest


class TestResources(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()

	def tearDown(self):
		pass


	def createLocalResources(self):
		node_names=['local1', 'local2', 'local3']
		cores_num=[2, 2, 4]

		self.__nnodes = len(node_names)
		self.__ncores = sum(cores_num)

		if len(node_names) != len(cores_num):
			raise Exception("failed to parse slurm env: number of nodes (%d) mismatch number of cores (%d)" % (len(nodes), len(cores)))
		
		nodes = []
		for i in range(0, len(node_names)):
			nodes.append(Node(node_names[i], cores_num[i], 0))

		return Resources(nodes)
	
	
	def test_localResources(self):
		res = self.createLocalResources()

		self.assertEqual(len(res.nodes), self.__nnodes)
		self.assertEqual(res.totalCores, self.__ncores)
		self.assertEqual(res.usedCores, 0)
		self.assertEqual(res.freeCores, self.__ncores)

