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
from qcg.appscheduler.slurmenv import parse_slurm_resources
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, CancelJobReq, ListJobsReq, ResourcesInfoReq
from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.fileinterface import FileInterface
from qcg.appscheduler.receiver import Receiver

from appschedulertest import AppSchedulerTest


class TestFileService(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()
		self.jobReportFile = "jobs.report"

		if exists(self.jobReportFile):
			os.remove(self.jobReportFile)


	def tearDown(self):
		self.closeLogging()


	async def __stopInterfaces(self, fileConf, receiver):
		while not receiver.isFinished:
			await asyncio.sleep(1)

		logging.info("stopping receiver ...")
		receiver.stop()


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
	

	def createSlurmResources(self):
		return parse_slurm_resources()


	def __jobNotify(self, jobId, state, manager):
		if self.jobReportFile is not None:
			if state.isFinished():
				with open(self.jobReportFile, 'a') as f:
					job = manager.jobList.get(jobId)
					f.write("%s (%s)\n\t%s\n\t%s\n" % (jobId, state.name,
							"\n\t".join([ "%s: %s" % (str(en[1]), en[0].name) for en in job.history ]),
							"\n\t".join([ "%s: %s" % (k, v) for k, v in job.runtime.items() ])))


	def Notest_FileInterfacesService(self):
		res = self.createLocalResources()
#		res = self.createSlurmResources()

		manager = Manager(res)
		notifId = manager.registerNotifier(self.__jobNotify, manager)

		fileConf = {
			FileInterface.CONF_FILE_PATH: "reqs.json"
		} 

		if 'QCG_PM_REQS_FILE' in os.environ:
			fileConf[FileInterface.CONF_FILE_PATH] = os.environ['QCG_PM_REQS_FILE']

		ifaces = [ FileInterface() ]
		ifaces[0].setup( fileConf )

		receiver = Receiver(manager, ifaces)

		receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__stopInterfaces(fileConf, receiver)
			))

		asyncio.get_event_loop().close()
