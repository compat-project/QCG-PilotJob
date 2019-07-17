import json
import logging
import os

import zmq
from zmq.asyncio import Context
from qcg.appscheduler.config import Config


class ZMQInterface:
    @classmethod
    def name(cls):
        return "ZMQ"

    def __init__(self):
        pass

    def setup(self, conf):
        zmq.asyncio.install()
        self.zmqCtx = Context.instance()

        self.address = Config.ZMQ_IFACE_ADDRESS.get(conf)

        self.socket = self.zmqCtx.socket(zmq.REP)
        self.socket.bind(self.address)

        self.real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT)))

        logging.info("ZMQ interface configured (address %s) @ %s" % (
            self.address, self.real_address))

    def close(self):
        pass

    async def receive(self):
        logging.info("ZMQ interface listening for requests with pid {}...".format(os.getpid()))

        req = await self.socket.recv()

        logging.info("ZMQ interface received request ...")

        return json.loads(bytes.decode(req))

    async def reply(self, replyMsg):
        await self.socket.send(str.encode(replyMsg))
