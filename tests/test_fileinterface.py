import unittest
import json
import logging
import context
import asyncio
import shutil
import os
import datetime
from datetime import timedelta
from os.path import join, exists, abspath

from qcg.appscheduler.fileinterface import FileInterface
from qcg.appscheduler.joblist import Job, JobExecution, JobResources, ResourceSize
from appschedulertest import AppSchedulerTest


class TestFileInterface(AppSchedulerTest):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())
		self.testSandbox = 'test-sandbox'
		self.testJobFile = join(self.testSandbox, 'test-jobs.json')
		self.setupLogging()


	def tearDown(self):
		self.closeLogging()


	def __prepareEnvironment(self):
		if exists(self.testSandbox):
			shutil.rmtree(self.testSandbox)
		os.makedirs(self.testSandbox)

		self.scriptFile = abspath(join(self.testSandbox, 'script.sh'))
		if exists(self.scriptFile):
			os.remove(self.scriptFile)

		with open(self.scriptFile, 'w') as f:
			f.write('''
#!/bin/bash

echo "*** environment ***"
env

echo "*** info ***"
echo "host: `hostname --fqdn`"
echo "cwd: `pwd`"
echo "date: `date`"
echo "account: `id`"
echo "taskset: `taskset -p $$`"
			''')

	def __generateJobsFile(self):
		with open(self.testJobFile, 'w') as f:
			jobs = []
			for job in self.jobs:
				jobs.append(job.toDict())
			f.write(json.dumps(jobs, indent=2))


	def __setupJobs(self):
		self.jobs = [
			Job('msleep1',
				JobExecution(
					'/usr/bin/sleep',
					args = [ '5s' ],
					wd = abspath(join(self.testSandbox, 'sleep1.sandbox')),
					stdout = 'sleep1.stdout',
					stderr = 'sleep1.stderr',
					),
				JobResources(
					numCores = ResourceSize(2)
					)),
			Job('msleep2',
				JobExecution(
					'/usr/bin/sleep',
					args = [ '5s' ],
					wd = abspath(join(self.testSandbox, 'sleep2.sandbox')),
					stdout = 'sleep2.stdout',
					stderr = 'sleep2.stderr',
					),
				JobResources(
					numCores = ResourceSize(2)
					)),
			Job('mscript',
				JobExecution(
					'/usr/bin/bash',
					args = [ self.scriptFile ],
					wd = abspath(join(self.testSandbox, 'script.sandbox')),
					stdout = 'script.stdout',
					stderr = 'script.stderr',
					),
				JobResources(
					numCores = ResourceSize(2)
					)),
			Job('msleep3',
				JobExecution(
					'/usr/bin/sleep',
					args = [ '5s' ],
					wd = abspath(join(self.testSandbox, 'sleep3.sandbox')),
					stdout = 'sleep3.stdout',
					stderr = 'sleep3.stderr',
					),
				JobResources(
					numCores = ResourceSize(1)
					))
				]


	def test2_FileInterfaceSetup(self):
		self.__prepareEnvironment()
		self.__setupJobs()		
		self.__generateJobsFile()

		asyncio.get_event_loop().close()

