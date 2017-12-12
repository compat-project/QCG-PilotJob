import unittest
import json
import logging
import asyncio
import os
from os.path import exists, join, abspath
from string import Template
import shutil
import zmq
from zmq.asyncio import Context

import context
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, CancelJobReq, ListJobsReq, ResourcesInfoReq
from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.zmqinterface import ZMQInterface
from qcg.appscheduler.receiver import Receiver

from appschedulertest import AppSchedulerTest


class TestZMQService(AppSchedulerTest):

	def setUp(self):
		asyncio.set_event_loop(asyncio.new_event_loop())

		self.setupLogging()


	def tearDown(self):
		pass


	async def __stopInterfaces(self, zmqConf, receiver):
		while True:
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
	

	def test_ZMQInterfacesInit(self):
		res = self.createLocalResources()

		manager = Manager(res)

		zmqConf = {
			ZMQInterface.CONF_IP_ADDRESS: "*",
			ZMQInterface.CONF_PORT: "5555"
		} 

		ifaces = [ ZMQInterface() ]
		ifaces[0].setup( zmqConf )

		receiver = Receiver(manager, ifaces)

		receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__stopInterfaces(zmqConf, receiver)
			))

		asyncio.get_event_loop().close()
