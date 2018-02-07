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


class TestZMQReceiver(AppSchedulerTest):

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

	
	async def __stopInterfaces(self, zmqConf, receiver, manager):
		await self.__zmqSimpleClient(zmqConf)
		logging.info("stopping receiver ...")
		await manager.waitForFinish()
		receiver.stop()


	async def __zmqSimpleClient(self, zmqConf):
		self.assertTrue(ZMQInterface.CONF_IP_ADDRESS in zmqConf)
		self.assertTrue(ZMQInterface.CONF_PORT in zmqConf)

		await asyncio.sleep(1)

		try:
			zmqCtx = Context.instance()

			address = 'tcp://%s:%s' % (
					str(zmqConf[ZMQInterface.CONF_IP_ADDRESS]),
					str(zmqConf[ZMQInterface.CONF_PORT])
					)

			socket = zmqCtx.socket(zmq.REQ)
			socket.connect(address)

			await socket.send(str.encode(json.dumps({
				"request": "resourcesInfo"
			})))

			logging.info("request sent - waiting for response")

			resp = await socket.recv()

			logging.info("received response: %s" % bytes.decode(resp))
		except:
			logging.exception("Client failed")

		socket.close()

		logging.info("client finishing")


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
			ZMQInterface.CONF_IP_ADDRESS: "127.0.0.1",
			ZMQInterface.CONF_PORT: "5555"
		} 

		ifaces = [ ZMQInterface() ]
		ifaces[0].setup( zmqConf )

		receiver = Receiver(manager, ifaces)

		receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__stopInterfaces(zmqConf, receiver, manager)
			))

		asyncio.get_event_loop().close()
