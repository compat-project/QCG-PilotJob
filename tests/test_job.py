import unittest
import json
import string

import context
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.joblist import JobFiles, JobDependencies, IllegalJobDescription
from qcg.appscheduler.joblist import Job, JobExecution, JobResources, ResourceSize

from appschedulertest import AppSchedulerTest


class TestJob(AppSchedulerTest):

	def setUp(self):
		pass

	def tearDown(self):
		pass


	def compareJobs(self, j, j_copy):
		self.assertEqual(j.name, j_copy.name)
		self.assertIsNotNone(j_copy.execution)
		self.assertEqual(j.execution.exec, j_copy.execution.exec)
		self.assertEqual(j.execution.stdin, j_copy.execution.stdin)
		self.assertEqual(j.execution.stdout, j_copy.execution.stdout)
		self.assertEqual(j.execution.stderr, j_copy.execution.stderr)
		self.assertEqual(j.execution.wd, j_copy.execution.wd)
		self.assertEqual(len(j.execution.args), len(j_copy.execution.args))
		self.assertEqual(j.execution.args, j_copy.execution.args)
		self.assertEqual(len(j.execution.env), len(j_copy.execution.env))
		self.assertEqual(j.execution.env, j_copy.execution.env)
		self.assertEqual(j.resources.hasNodes(), j_copy.resources.hasNodes())
		self.assertEqual(j.resources.hasCores(), j_copy.resources.hasCores())
		if j.resources.hasNodes():
			self.assertEqual(j.resources.nodes.exact, j_copy.resources.nodes.exact)
			self.assertEqual(j.resources.nodes.min, j_copy.resources.nodes.min)
			self.assertEqual(j.resources.nodes.max, j_copy.resources.nodes.max)
		if j.resources.hasCores():
			self.assertEqual(j.resources.cores.exact, j_copy.resources.cores.exact)
			self.assertEqual(j.resources.cores.min, j_copy.resources.cores.min)
			self.assertEqual(j.resources.cores.max, j_copy.resources.cores.max)
		self.assertEqual(j.hasFiles(), j_copy.hasFiles())
		if j.hasFiles():
			self.assertEqual(len(j.files.stageIn), len(j_copy.files.stageIn))
			self.assertEqual(j.files.stageIn, j_copy.files.stageIn)
			self.assertEqual(len(j.files.stageOut), len(j_copy.files.stageOut))
			self.assertEqual(j.files.stageOut, j_copy.files.stageOut)
		self.assertEqual(j.hasDependencies(), j_copy.hasDependencies())
		if j.hasDependencies():
			self.assertEqual(len(j.dependencies.after), len(j_copy.dependencies.after))
			self.assertEqual(j.dependencies.after, j_copy.dependencies.after)


	def test_JobInitAndJson(self):
		jName = 'job1'
		j = Job(jName, JobExecution('/bin/date'), JobResources(numCores = 2))

		self.assertIsNotNone(j)
		self.assertIsNotNone(j.execution)
		self.assertEqual(j.name, jName)
		self.assertEqual(j.state, JobState.QUEUED)
		self.assertEqual(len(j.history), 1)
		self.assertEqual(j.hasDependencies(), False)

		j.state = JobState.EXECUTING
		self.assertEqual(j.state, JobState.EXECUTING)
		self.assertEqual(len(j.history), 2)

		j_json = j.toJSON()
		j_copy = Job(**json.loads(j_json))
		self.assertIsNotNone(j_copy)
		j_json2 = j_copy.toJSON()

		self.assertEqual(j_json, j_json2)

		self.compareJobs(j, j_copy)
		

	def test_JobImport(self):
		jDesc = '''{
    "name": "msleep2",
    "execution": {
      "exec": "/usr/bin/sleep",
      "args": [
        "5s"
      ],
      "env": {},
      "wd": "/home/kieras/app-scheduler/src/repo/test-sandbox/sleep2.sandbox",
      "stdout": "sleep2.stdout",
      "stderr": "sleep2.stderr"
    },
    "resources": {
      "numCores": {
        "exact": 2
      }
    }
  }'''

		j = Job(**json.loads(jDesc))
		self.assertIsNotNone(j)

		jDesc_copy = j.toJSON()
		# compare with ignoring white characters
		self.compareIgnoringWhiteSpaces(jDesc, jDesc_copy)


	def __compareIgnoringWhiteSpaces(self, str1, str2):
		self.assertEqual(str1.translate(str.maketrans(dict.fromkeys(string.whitespace))),
					     str2.translate(str.maketrans(dict.fromkeys(string.whitespace))))


	def test_JobComplex(self):
		jDesc = '''{
    "name": "mscript",
    "execution": {
      "exec": "/usr/bin/bash",
      "args": [
        "/home/kieras/app-scheduler/src/repo/test-sandbox/script.sh"
      ],
      "env": {
	  	"VAR1": "VAR1_VALUE"
	  },
      "wd": "/home/kieras/app-scheduler/src/repo/test-sandbox/script.sandbox",
      "stdout": "script.stdout",
      "stderr": "script.stderr"
    },
    "resources": {
      "numCores": {
        "exact": 2
      }
    },
	"files": {
	  "stageIn": [
	  	"in1",
		"in2"
	  ],
	  "stageOut": [
		"out1",
		"out2",
		"out3"
	  ]
	},
	"dependencies": {
	  "after": [
		  "job1",
		  "job2"
	  ]
	}
  }'''

		j = Job(**json.loads(jDesc))
		self.assertIsNotNone(j)

		jDesc_copy = j.toJSON()
		# compare with ignoring white characters
		self.__compareIgnoringWhiteSpaces(jDesc, jDesc_copy)

		self.assertEqual(j.name, "mscript")
		self.assertEqual(j.execution.exec, "/usr/bin/bash")
		self.assertEqual(len(j.execution.args), 1)
		self.assertEqual(j.execution.args[0], "/home/kieras/app-scheduler/src/repo/test-sandbox/script.sh")
		self.assertEqual(j.execution.wd, "/home/kieras/app-scheduler/src/repo/test-sandbox/script.sandbox")
		self.assertEqual(len(j.execution.env), 1)
		self.assertEqual(j.execution.env['VAR1'], 'VAR1_VALUE')
		self.assertEqual(j.resources.hasCores(), True)
		self.assertEqual(j.resources.cores.exact, 2)
		self.assertEqual(j.resources.hasNodes(), False)
		self.assertEqual(j.hasDependencies(), True)
		self.assertEqual(len(j.dependencies.after), 2)
		self.assertEqual(j.hasFiles(), True)
		self.assertEqual(len(j.files.stageIn), 2)
		self.assertEqual(len(j.files.stageOut), 3)

