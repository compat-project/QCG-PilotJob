import unittest
import json

import context
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies


class TestJobExecution(unittest.TestCase):

	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_JobExecutionExportToJSONLength(self):
		je = JobExecution('/bin/hostname')
		j_json = je.toJSON()
		self.assertTrue(len(j_json) > 0)

	def test_JobExecutionImportFromJSONDefault(self):
		exec = '/bin/hostname'

		je_json = """{
		  "exec": "%s"
		}""" % (exec)

		je = JobExecution(**json.loads(je_json))

		self.assertEqual(exec, je.exec)
		self.assertIsNone(je.stdout)
		self.assertIsNone(je.stderr)
		self.assertIsNone(je.wd)
		self.assertIsInstance(je.args, list)
		self.assertIsInstance(je.env, dict)

	def test_JobExecutionImportExportEquality(self):
		je = JobExecution('/bin/hostname')

		je_json = je.toJSON()
		je_copy = JobExecution(**json.loads(je_json))
		je_json2 = je_copy.toJSON()

		self.assertEqual(je_json, je_json2)


	def test_JobExecutionImportFromJSONComplex(self):
		exec = '/bin/date'
		stdout = "path/to/stdout"
		stderr = "path/to/stderr"
		wd = "path/to/wd"

		je_json = """{
		  "exec": "%s",
		  "args": [ "arg1", "arg2" ],
		  "env": { "var1": "val1", "var2": "val2" },
		  "stdout": "%s",
		  "stderr": "%s",
		  "wd": "%s"
		}""" % (exec, stdout, stderr, wd)

		je = JobExecution(** json.loads(je_json))

		self.assertEqual(exec, je.exec)
		self.assertEqual(je.stdout, stdout)
		self.assertEqual(je.stderr, stderr)
		self.assertEqual(je.wd, wd)
		self.assertIsInstance(je.args, list)
		self.assertTrue(len(je.args) == 2)
		for arg in je.args:
			self.assertIsInstance(arg, str)
			self.assertIsNotNone(arg)
		self.assertIsInstance(je.env, dict)
		for env_n, env_v in je.env.items():
			self.assertIsInstance(env_n, str)
			self.assertIsInstance(env_v, str)
			self.assertIsNotNone(env_n)
			self.assertIsNotNone(env_v)


if __name__ == '__main__':
	unittest.main()
