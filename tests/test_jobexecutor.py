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

from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.scheduler import Scheduler
from qcg.appscheduler.allocation import NodeAllocation, Allocation
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies, Job
from qcg.appscheduler.executor import Executor


class TestJobExecutor(unittest.TestCase):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())

		logFile = 'test.log'

		if os.path.exists(logFile):
			os.remove(logFile)

		logging.basicConfig(filename=logFile, level=logging.DEBUG)
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
	

	async def __schedule(self, jobs, scheduler, executor):
		for job in jobs:
			allocation = scheduler.allocateJob(job.resources)
			self.assertIsNotNone(allocation)

			executor.execute(allocation, job)

		await executor.waitForUnfinished()


	def test_ExecutorSimple(self):
		res = self.createLocalResources()
		scheduler = Scheduler(res)

		testSandbox = 'test-sandbox'
		if exists(testSandbox):
			shutil.rmtree(testSandbox)
		os.makedirs(testSandbox)

		for dir in [ 'hostname.sandbox', 'env.sandbox', 'sleep.sandbox', 'script.sandbox' ]:
			dPath = join(testSandbox, dir)
			if exists(dPath):
				shutil.rmtree(dPath)

		scriptFile = abspath(join(testSandbox, 'script.sh'))
		if exists(scriptFile):
			os.remove(scriptFile)

		with open(scriptFile, 'w') as f:
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

		hostnameStdinFile = abspath(join(testSandbox, 'hostname.stdin'))
		if exists(hostnameStdinFile):
			os.remove(hostnameStdinFile)

		with open(hostnameStdinFile, 'w') as f:
			f.write('some host name')


		jobs = [
			Job('job1',
				JobExecution(
					'/usr/bin/wc',
					args = [ '-m' ],
					wd = abspath(join(testSandbox, 'hostname.sandbox')),
					stdin = hostnameStdinFile,
					stdout = 'hostname.stdout',
					stderr = 'hostname.stderr',
					),
				JobResources(
					numCores = ResourceSize(2)
					)),
			Job('job2',
				JobExecution(
					'/usr/bin/env',
					wd = abspath(join(testSandbox, 'env.sandbox')),
					stdout = 'env.stdout',
					stderr = 'env.stderr',
					),
				JobResources(
					numCores = ResourceSize(1)
					)),
			Job('sleep',
				JobExecution(
					'/usr/bin/sleep',
					args = [ '5s' ],
					wd = abspath(join(testSandbox, 'sleep.sandbox')),
					stdout = 'sleep.stdout',
					stderr = 'sleep.stderr',
					),
				JobResources(
					numCores = ResourceSize(1)
					)),
			Job('script',
				JobExecution(
					'/usr/bin/bash',
					args = [ scriptFile ],
					wd = abspath(join(testSandbox, 'script.sandbox')),
					stdout = 'script.stdout',
					stderr = 'script.stderr',
					),
				JobResources(
					numCores = ResourceSize(1)
					))

				]

		executor = Executor(None)

		startTime = datetime.datetime.now()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__schedule(jobs, scheduler, executor)
			))
		asyncio.get_event_loop().close()

		duration = datetime.datetime.now() - startTime
		self.assertTrue(duration.total_seconds() > 5 and duration.total_seconds() < 10)

		for job in jobs:
			self.assertTrue(os.path.exists(job.execution.wd))
			self.assertTrue(os.path.exists(os.path.join(job.execution.wd, job.execution.stdout)))
			self.assertTrue(os.path.exists(os.path.join(job.execution.wd, job.execution.stderr)))

			stderrStat = os.stat(os.path.join(job.execution.wd, job.execution.stderr))
			self.assertTrue(stderrStat.st_size == 0)
