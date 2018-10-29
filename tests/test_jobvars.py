import unittest
import json
import logging
import context
import asyncio
import shutil
import os
import stat
import datetime
from datetime import timedelta
from os.path import join, exists, abspath
import mmap

from qcg.appscheduler.errors import NotSufficientResources, InvalidResourceSpec
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.allocation import NodeAllocation, Allocation
from qcg.appscheduler.joblist import JobExecution, ResourceSize, JobResources, JobFiles, JobDependencies, Job
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.executor import Executor
from qcg.appscheduler.request import SubmitReq
from appschedulertest import AppSchedulerTest


class TestJobVars(AppSchedulerTest):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())
		self.setupLogging()

		self.testSandbox = os.path.abspath('test-sandbox')
		if exists(self.testSandbox):
			shutil.rmtree(self.testSandbox)
		os.makedirs(self.testSandbox)

		self.config = {
			Executor.EXECUTOR_WD: self.testSandbox,
			Executor.EXECUTION_SCHEMA: 'direct'
		}

	def tearDown(self):
		self.closeLogging()


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
	

	async def __schedule(self, jobs, manager):
		reqJobs = [ job.toDict() for job in jobs ]
		manager.enqueue(SubmitReq({ 'jobs': reqJobs }).jobs)
		await manager.waitForFinish()


	def test_JobVars(self):
		res = self.createLocalResources()
		manager = Manager(res, self.config)

		scriptFile = join(self.testSandbox, 'script.sh')
		if exists(scriptFile):
			os.remove(scriptFile)

		with open(scriptFile, 'w') as f:
			f.write('''
#!/bin/env bash

echo "*** environment ***"
env

			''')
		os.chmod(scriptFile, stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)

		jobs = [
			Job('job1',
				JobExecution(
					'bash',
					args = [ scriptFile ],
					wd = '${root_wd}/job1',
					stdout = '${nnodes}.${ncores}-${jname}.stdout',
					stderr = '${nnodes}.${ncores}-${jname}.stderr',
					),
				JobResources(
					numNodes = ResourceSize(1),
					numCores = ResourceSize(2)
					)
			)
		]

		startTime = datetime.datetime.now()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__schedule(jobs, manager)
			))
		asyncio.get_event_loop().close()

		duration = datetime.datetime.now() - startTime
		self.assertTrue(duration.total_seconds() > 0 and duration.total_seconds() < 4)

		job1_wd = join(self.testSandbox, 'job1')
		self.assertTrue(os.path.exists(job1_wd))
		self.assertTrue(os.path.exists(os.path.join(job1_wd, '1.2-job1.stdout')))
		self.assertTrue(os.path.exists(os.path.join(job1_wd, '1.2-job1.stderr')))

		stderrStat = os.stat(os.path.join(job1_wd, '1.2-job1.stderr'))
		self.assertTrue(stderrStat.st_size == 0)

		with open(os.path.join(job1_wd, '1.2-job1.stdout'), 'r', 1) as file, \
		     mmap.mmap(file.fileno(), 0, prot=mmap.PROT_READ) as s:
				 self.assertTrue(s.find('QCG_PM_NTASKS=2'.encode('UTF-8')) != -1)
				 self.assertTrue(s.find('QCG_PM_TASKS_PER_NODE=2'.encode('UTF-8')) != -1)
				 self.assertTrue(s.find('QCG_PM_NNODES=1'.encode('UTF-8')) != -1)
				 self.assertTrue(s.find('QCG_PM_NPROCS=2'.encode('UTF-8')) != -1)

