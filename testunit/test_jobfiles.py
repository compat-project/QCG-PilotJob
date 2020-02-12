import unittest
import json

import context
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies, IllegalJobDescription
from appschedulertest import AppSchedulerTest


class TestJobFiles(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()

	def tearDown(self):
		self.closeLogging()


	def test_JobFilesInit(self):
		jf = JobFiles([ 'in1', 'in2', [ 'in3-input', 'in3' ]])

		self.assertEqual(len(jf.stageIn), 3)
		self.assertEqual(len(jf.stageOut), 0)

	def test_JobFilesInit2(self):
		jf = JobFiles(stageOut = [ 'in1', 'in2', [ 'in3-input', 'in3' ]])

		self.assertEqual(len(jf.stageOut), 3)
		self.assertEqual(len(jf.stageIn), 0)

	def test_JobFilesInit3(self):
		jf = JobFiles([ 'in1', 'in2', [ 'in3-input', 'in3' ]], [ 'out1' ])

		self.assertEqual(len(jf.stageIn), 3)
		self.assertEqual(len(jf.stageOut), 1)

	def test_JobFilesInit4(self):
		try:
			jf = JobFiles([ 'in1', 'in2', [ 'in3-input', 'in3', 'in3-error' ]])
			self.fail("Invalid construction (three arguments for single file) accepted")
		except IllegalJobDescription:
			pass

	def test_JobFilesInit5(self):
		try:
			jf = JobFiles(stageOut = [ 'in1', 'in2', [ 'in3-input', 'in3', 'in3-error' ]])
			self.fail("Invalid construction (three arguments for single file) accepted")
		except IllegalJobDescription:
			pass

	def test_JobFilesInit6(self):
		try:
			jf = JobFiles(stageOut = [ 'in1', 'in2', 4])
			self.fail("Invalid construction (not a file name) accepted")
		except IllegalJobDescription:
			pass

	def test_JobFilesImportFromJSONDefault(self):
		jf_json = """{
		  "stageIn": [
			"in1",
			"in2",
			[
			  "in3-input",
			  "in3"
			]
		  ]
		}"""

		jf = JobFiles(**json.loads(jf_json))

		self.assertEqual(len(jf.stageIn), 3)
		self.assertEqual(len(jf.stageOut), 0)

	def test_JobFilesImportExportEquality(self):
		jf = JobFiles(stageOut = [ 'file1', [ 'file2', 'dst_file2' ] ])
		jf_json = jf.toJSON()
		jf_copy = JobFiles(**json.loads(jf_json))
		jf_json2 = jf_copy.toJSON()

		self.assertEqual(jf_json, jf_json2)


if __name__ == '__main__':
	unittest.main()
