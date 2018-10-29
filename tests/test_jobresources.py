import unittest
import json

import context
from datetime import timedelta
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies
from appschedulertest import AppSchedulerTest


class TestJobResources(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()

	def tearDown(self):
		self.closeLogging()


	def test_JobResourcesInitCores(self):
		jr = JobResources(1)

		self.assertEqual(jr.numCores.exact, 1)
		self.assertIsNone(jr.numNodes)

	def test_JobResourcesInitCoresByStruct(self):
		jr = JobResources(ResourceSize(1))

		self.assertEqual(jr.numCores.exact, 1)
		self.assertIsNone(jr.numCores.min)
		self.assertIsNone(jr.numCores.max)
		self.assertIsNone(jr.numNodes)

	def test_JobResourcesInitCoresByStructValid(self):
		try:
			jr = JobResources(ResourceSize(exact = 1, min = 2))
			self.fail("Non proper initialization passed")
		except Exception as e:
			pass

	def test_JobResourcesInitCoresByStruct2(self):
		jr = JobResources(ResourceSize(min = 1, max = 3))

		self.assertIsNone(jr.numCores.exact)
		self.assertEqual(jr.numCores.min, 1)
		self.assertEqual(jr.numCores.max, 3)
		self.assertIsNone(jr.numNodes)

	def test_JobResourcesInitCoresByStruct3(self):
		jr = JobResources(ResourceSize(min = 1, max = 3), ResourceSize(3))

		self.assertIsNone(jr.numCores.exact)
		self.assertEqual(jr.numCores.min, 1)
		self.assertEqual(jr.numCores.max, 3)
		self.assertIsNone(jr.numNodes.min)
		self.assertIsNone(jr.numNodes.max)
		self.assertEqual(jr.numNodes.exact, 3)

	def test_JobResourcesInitNodes(self):
		jr = JobResources(numNodes = 2)

		self.assertEqual(jr.numNodes.exact, 2)
		self.assertIsNone(jr.numCores)

	def test_JobResourcesImportFromJSONDefault(self):
		jr_json = """{
			"numCores": {
				"exact": 1
			},
			"numNodes": {
				"exact": 2
			}
		}"""

		jr = JobResources(**json.loads(jr_json))

		self.assertEqual(jr.numCores.exact, 1)
		self.assertIsNone(jr.numCores.min)
		self.assertIsNone(jr.numCores.max)
		self.assertEqual(jr.numNodes.exact, 2)
		self.assertIsNone(jr.numNodes.max)
		self.assertIsNone(jr.numNodes.min)


	def test_JobResourcesImportExportEquality(self):
		jr = JobResources(numNodes = ResourceSize(max = 2, min = 1))
		jr_json = jr.toJSON()
		jr_copy = JobResources(**json.loads(jr_json))
		jr_json2 = jr_copy.toJSON()

		self.assertEqual(jr_json, jr_json2)


	def test_JobResourcesWT(self):
		jr = JobResources(numCores = 1, wt = "5s")
		self.assertEqual(jr.wt.total_seconds(), 5)

		jr = JobResources(numCores = 1, wt = "2h5s")
		self.assertEqual(jr.wt.total_seconds(), timedelta(hours = 2, seconds = 5).total_seconds())

		jr = JobResources(numCores = 1, wt = "122h90m5s")
		self.assertEqual(jr.wt.total_seconds(), timedelta(hours = 122, minutes = 90, seconds = 5).total_seconds())

if __name__ == '__main__':
	unittest.main()
