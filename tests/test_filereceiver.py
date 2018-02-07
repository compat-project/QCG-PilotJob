import unittest
import json
import logging
import asyncio
import os
from os.path import exists, join, abspath
from string import Template
import shutil

import context
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, CancelJobReq, ListJobsReq, ResourcesInfoReq
from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.fileinterface import FileInterface
from qcg.appscheduler.receiver import Receiver

from appschedulertest import AppSchedulerTest


class TestFileReceiver(AppSchedulerTest):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())

		self.testSandbox = 'test-sandbox'
		self.setupLogging()
		self.__prepareEnvironment()


	def tearDown(self):
		self.closeLogging()


	def __prepareEnvironment(self):
		if exists(self.testSandbox):
			shutil.rmtree(self.testSandbox)
		os.makedirs(self.testSandbox)

	def __createSampleRequests(self):
		reqsFileName = 'requests.json'

		reqsFilePath = abspath(join(self.testSandbox, reqsFileName))
		if exists(reqsFilePath):
			os.remove(reqsFilePath)

		self.jobSandbox = abspath(join(self.testSandbox, 'date.sanbox'))
		self.jobStdoutName = 'date.stdout'
		self.jobStderrName = 'date.stderr'
		with open(reqsFilePath, 'w') as f:
			reqs = '''[
		{
			"request": "submit",
			"jobs": [ {
                "name": "date",
                "execution": {
                  "exec": "date",
                  "wd": "%s",
                  "stdout": "%s",
                  "stderr": "%s"
                },
                "resources": {
                  "numCores": {
                        "exact": 1
                  }
                }
			} ]
		},
		{
			"request": "jobStatus",
			"jobName": "date"
		},
		{
			"request": "listJobs"
		},
		{
			"request": "resourcesInfo"
		},
		{
			"request": "cancelJob"
		}
		]'''
			freqs = reqs % (self.jobSandbox, self.jobStdoutName, self.jobStderrName)
			logging.info("formatted requests: %s" % freqs)
			f.write(freqs)

		return reqsFilePath


	def __createSubmitIteratableRequests(self):
		reqsFileName = 'submitIter.json'

		submitReqsFilePath = abspath(join(self.testSandbox, reqsFileName))
		if exists(submitReqsFilePath):
			os.remove(submitReqsFilePath)

		self.iterJobSandbox = abspath(join(self.testSandbox, 'date_iter_${it}.sanbox'))
		self.iterJobStdoutName = 'date.stdout'
		self.iterJobStderrName = 'date.stderr'
		self.startIter = 1
		self.endIter = 11
		with open(submitReqsFilePath, 'w') as f:
			reqs = '''[
		{
			"request": "submit",
			"jobs": [ {
                "name": "date_${it}",
				"iterate": [ %d, %d ],
                "execution": {
                  "exec": "env",
                  "wd": "%s",
                  "stdout": "%s",
                  "stderr": "%s"
                },
                "resources": {
                  "numCores": {
                        "min": 1,
						"split-into": 2
                  }
                }
			} ]
		}
		]'''
			freqs = reqs % (self.startIter, self.endIter, self.iterJobSandbox,
					self.iterJobStdoutName, self.iterJobStderrName)
			logging.info("formatted requests: %s" % freqs)
			f.write(freqs)

		return submitReqsFilePath
	
	async def __stopInterfaces(self, receiver, delay = 3):
		await asyncio.sleep(delay)
		receiver.stop()


	async def __waitForFinish(self, manager):
		await manager.waitForFinish()


	async def __delayStop(self, delay = 5):
		await asyncio.sleep(delay)


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
	

	def test_FileInterfacesInit(self):
		res = self.createLocalResources()

		reqsFilePath = self.__createSampleRequests()

		manager = Manager(res)

		ifaces = [ FileInterface() ]
		ifaces[0].setup( { FileInterface.CONF_FILE_PATH: reqsFilePath } )

		receiver = Receiver(manager, ifaces)

		receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__waitForFinish(manager)
#			self.__stopInterfaces(receiver, 2)
#			self.__delayStop(5)
			))

		asyncio.get_event_loop().close()

		self.assertTrue(os.path.exists(self.jobSandbox))
		self.assertTrue(os.path.exists(os.path.join(self.jobSandbox, self.jobStdoutName)))
		self.assertTrue(os.path.exists(os.path.join(self.jobSandbox, self.jobStderrName)))


	def test_FileInterfaceIterateSubmit(self):
		res = self.createLocalResources()

		reqsFilePath = self.__createSubmitIteratableRequests()

		manager = Manager(res)

		ifaces = [ FileInterface() ]
		ifaces[0].setup( { FileInterface.CONF_FILE_PATH: reqsFilePath } )

		receiver = Receiver(manager, ifaces)

		receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__waitForFinish(manager)
#			self.__stopInterfaces(receiver, 8)
#			self.__delayStop(5)
			))

		asyncio.get_event_loop().close()

		for i in range(self.startIter, self.endIter):
			jobWdDir = Template(self.iterJobSandbox).safe_substitute({ 'it': i })
			self.assertTrue(jobWdDir)
			self.assertTrue(os.path.exists(os.path.join(jobWdDir, self.iterJobStdoutName)))
			self.assertTrue(os.path.exists(os.path.join(jobWdDir, self.iterJobStderrName)))

