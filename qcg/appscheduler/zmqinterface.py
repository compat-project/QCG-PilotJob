import json
import logging

import zmq
from zmq.asyncio import Context


class ZMQInterface:
    CONF_IP_ADDRESS = 'ip.address'
    CONF_PORT = 'port'

    CONF_DEFAULT = {
        CONF_IP_ADDRESS: '127.0.0.1',
        CONF_PORT: '5555'
    }

    CONF_ZMQ_IFACE_ADDRESS = 'zmq.address'


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

        conf[ZMQInterface.CONF_ZMQ_IFACE_ADDRESS] = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT)))

        logging.info("ZMQ interface configured (address %s) @ %s" % (
            self.address, conf[ZMQInterface.CONF_ZMQ_IFACE_ADDRESS]))


    def close(self):
        pass

    async def receive(self):
        logging.info("ZMQ interface listening for requests ...")

        req = await self.socket.recv()

        logging.info("ZMQ interface received request ...")

        return json.loads(bytes.decode(req))

    async def reply(self, replyMsg):
        await self.socket.send(str.encode(replyMsg))
