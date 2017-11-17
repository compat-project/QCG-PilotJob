import unittest
import json

import context
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.joblist import JobFiles, JobDependencies, IllegalJobDescription


class TestJob(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_JobInit(self):
		jName = 'job1'
		j = Job(jName, JobExecution('/bin/date'), JobResources(numCores = 2))

		self.assertIsNotNone(j)
		self.assertEqual(j.name, jName)
		self.assertEqual(j.state, JobState.QUEUED)
		self.assertEqual(len(j.history), 1)
		self.assertEqual(j.hasDependencies(), False)

		j.state = JobState.EXECUTING
		self.assertEqual(j.state, JobState.EXECUTING)
		self.assertEqual(len(j.history), 2)


if __name__ == '__main__':
	unittest.main()
