import asyncio
import zmq
import json
import logging
from zmq.asyncio import Context


class ZMQInterface:

	CONF_IP_ADDRESS = 'ip.address'
	CONF_PORT = 'port'

	CONF_DEFAULT = {
			CONF_IP_ADDRESS: '127.0.0.1',
			CONF_PORT: '5555'
			}


	@classmethod
	def name(cls):
		return "ZMQ"

	def __init__(self):
		pass


	def setup(self, conf):
		zmq.asyncio.install()
		self.zmqCtx = Context.instance()

		self.address = 'tcp://%s:%s' % (
				str(conf.get(ZMQInterface.CONF_IP_ADDRESS,
					ZMQInterface.CONF_DEFAULT[ZMQInterface.CONF_IP_ADDRESS])),
				str(conf.get(ZMQInterface.CONF_PORT,
					ZMQInterface.CONF_DEFAULT[ZMQInterface.CONF_PORT]))
				)

		self.socket = self.zmqCtx.socket(zmq.REP)
		self.socket.bind(self.address)

		logging.info("ZMQ interface configured (address %s)" % (self.address))


	def close(self):
		pass


	async def receive(self):
		logging.info("ZMQ interface listening for requests ...")

		req = await self.socket.recv()

		logging.info("ZMQ interface received request ...")

		return json.loads(req)


	async def reply(self, replyMsg):
		await self.socket.send(str.encode(replyMsg))

