import asyncio
import zmq
import json
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
				str(conf.get(CONF_IP_ADDRESS, CONF_DEFAULT[CONF_IP_ADDRESS])),
				str(conf.get(CONF_PORT, CONF_DEFAULT[CONF_PORT]))
				)

		self.socket = self.zmqCtx.socket(zmq.REP)
		self.socket.connect(self.address)


	def close(self):
		pass


	async def receive(self):
		req = await self.socket.recv()
		return json.loads(req)


	async def reply(self, replyMsg):
		await self.socket.send(str.encode(replyMsg))

