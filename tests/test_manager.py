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
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies, Job
from qcg.appscheduler.manager import Manager
from appschedulertest import AppSchedulerTest


class JobCounter:
	def __init__(self, cnt):
		self.notFinished = cnt


class TestManager(AppSchedulerTest):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())

		self.testSandbox = 'test-sandbox'
		self.scriptFile = 'script.sh'
		self.hostnameStdinFile = 'hostname-in'

		self.setupLogging()


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

		self.hostnameStdinFile = abspath(join(self.testSandbox, 'hostname.stdin'))
		if exists(self.hostnameStdinFile):
			os.remove(self.hostnameStdinFile)

		with open(self.hostnameStdinFile, 'w') as f:
			f.write('some host name')


	def __setupJobs(self):
		self.jobs = [
			Job('mjob1',
				JobExecution(
					'/usr/bin/wc',
					args = [ '-m' ],
					wd = abspath(join(self.testSandbox, 'hostname.sandbox')),
					stdin = self.hostnameStdinFile,
					stdout = 'hostname.stdout',
					stderr = 'hostname.stderr',
					),
				JobResources(
					numCores = ResourceSize(2)
					)),
			Job('msleep1',
				JobExecution(
					'/usr/bin/sleep',
					args = [ '2s' ],
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
					args = [ '2s' ],
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
					args = [ '2s' ],
					wd = abspath(join(self.testSandbox, 'sleep3.sandbox')),
					stdout = 'sleep3.stdout',
					stderr = 'sleep3.stderr',
					),
				JobResources(
					numCores = ResourceSize(self.__ncores)
					))
				]

	def __startTiming(self):
		self.startTime = datetime.datetime.now()

	
	def __stopTiming(self):
		self.duration = datetime.datetime.now() - self.startTime
		self.durationSecs = self.duration.total_seconds()


	@staticmethod
	def __jobNotify(jobId, state, counter):
		logging.info("got job %s status change to %s" % (jobId, state))

		if state.isFinished():
			counter.notFinished -= 1


	async def __waitForJobs(self, manager, counter):
		while counter.notFinished > 0:
			logging.info("waiting for finishing %d jobs ..." % (counter.notFinished))

			await asyncio.sleep(1)

		await manager.waitForFinish()


	def test_ManagerSimple(self):
		self.__prepareEnvironment()

		res = self.createLocalResources()

		self.__setupJobs()

		self.__startTiming()

		manager = Manager(res)

		notFinished = JobCounter(len(self.jobs))

		logging.info("# of jobs to submit (unfinished) %d" % (notFinished.notFinished))

		notifId = manager.registerNotifier(self.__jobNotify, notFinished)

		logging.info("callback registered %s" % (notifId))

		manager.enqueue(self.jobs)
		
		logging.info("jobs enqueued")

		logging.info("waiting for finish")

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__waitForJobs(manager, notFinished)
			))

		logging.info("all tasks finished")

		asyncio.get_event_loop().close()

		self.__stopTiming()

		self.assertTrue(self.durationSecs > 4 and self.durationSecs < 6)

		for job in self.jobs:
			self.assertTrue(os.path.exists(job.execution.wd))
			self.assertTrue(os.path.exists(os.path.join(job.execution.wd, job.execution.stdout)))
			self.assertTrue(os.path.exists(os.path.join(job.execution.wd, job.execution.stderr)))

			stderrStat = os.stat(os.path.join(job.execution.wd, job.execution.stderr))
			self.assertTrue(stderrStat.st_size == 0)
