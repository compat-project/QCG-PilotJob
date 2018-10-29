import unittest
import json

import context
from qcg.appscheduler.joblist import JobList, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.joblist import JobFiles, JobDependencies, JobState
from qcg.appscheduler.joblist import IllegalJobDescription, JobAlreadyExist
from appschedulertest import AppSchedulerTest


class TestJobList(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()

	def tearDown(self):
		self.closeLogging()


	def test_JobListInit(self):
		jl = JobList()

		try:
			jl.add('job1')
			self.fail("Invalid object added to job list")
		except AssertionError:
			pass

		jName1 = 'job1'
		j1 = Job(jName1, JobExecution('/bin/date'), JobResources(numCores = 2))
		jl.add(j1)
		self.assertTrue(jl.get(jName1) is not None)
		self.assertTrue(jl.exist(jName1))

		try:
			jl.add(j1)
			self.fail('Non-uniqe job added to job list')
		except JobAlreadyExist:
			pass

		self.assertTrue(jl.get(jName1) is not None)
		self.assertTrue(jl.exist(jName1))

		jName2 = 'job2'
		jl.add(Job('job2', JobExecution('/bin/date'), JobResources(numCores = 1)))
		self.assertTrue(jl.get(jName2) is not None)
		self.assertTrue(jl.exist(jName2))



if __name__ == '__main__':
	unittest.main()
