import unittest
import json

import context
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies, IllegalJobDescription


class TestJobDependencies(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_JobDependenciesInit(self):
		jd = JobDependencies([ 'job1', 'job2' ])

		self.assertEqual(len(jd.after), 2)

	def test_JobDependenciesInit2(self):
		jd = JobDependencies()

		self.assertEqual(len(jd.after), 0)

	def test_JobDependenciesInit3(self):
		jd = JobDependencies( 'job1' )

		self.assertEqual(len(jd.after), 1)

	def test_JobDependenciesInit4(self):
		try:
			jd = JobDependencies([ 4 ])
			self.fail("Invalid construction (job name not a name) accepted")
		except IllegalJobDescription:
			pass

	def test_JobDependenciesInit5(self):
		try:
			jd = JobDependencies([ 'in1', 'in2', [ 'in3-input', 'in3', 'in3-error' ]])
			self.fail("Invalid construction (job name not a name) accepted")
		except IllegalJobDescription:
			pass

	def test_JobDependenciesInit6(self):
		try:
			jd = JobDependencies([ [ 'job1' ] ])
			self.fail("Invalid construction (job name not a name) accepted")
		except IllegalJobDescription:
			pass

	def test_JobDependenciesImportFromJSONDefault(self):
		jd_json = """{
		  "after": [
			"job1",
			"job2"
		  ]
		}"""

		jd = JobDependencies(**json.loads(jd_json))

		self.assertEqual(len(jd.after), 2)

	def test_JobDependenciesImportExportEquality(self):
		jd = JobDependencies(after = [ 'job1', 'job2' ])
		jd_json = jd.toJSON()
		jd_copy = JobDependencies(**json.loads(jd_json))
		jd_json2 = jd_copy.toJSON()

		self.assertEqual(jd_json, jd_json2)


if __name__ == '__main__':
	unittest.main()
