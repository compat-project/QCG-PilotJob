import json
import logging
import os
import socket

import zmq
from zmq.asyncio import Context
from qcg.appscheduler.config import Config


class ZMQInterface:
    @classmethod
    def name(cls):
        return "ZMQ"

    def __init__(self):
        self.zmqCtx = None
        self.socket = None
        self.address = None
        self.local_port = None
        self.real_address = None
        self.external_address = None


    def setup(self, conf):
#        zmq.asyncio.install()
        self.zmqCtx = Context.instance()

        self.address = Config.ZMQ_IFACE_ADDRESS.get(conf)

        self.socket = self.zmqCtx.socket(zmq.REP)

        if Config.ZMQ_PORT.get(conf):
            self.socket.bind(self.address)
        else:
            self.local_port = self.socket.bind_to_random_port(self.address,
                    min_port=int(Config.ZMQ_PORT_MIN_RANGE.get(conf)),
                    max_port=int(Config.ZMQ_PORT_MAX_RANGE.get(conf)))

        self.real_address = str(bytes.decode(self.socket.getsockopt(zmq.LAST_ENDPOINT)))

        # the real address might contain the 0.0.0.0 IP address which means that it listens on all
        # interfaces, sadly this address is not valid for external services to communicate, so we
        # need to replace 0.0.0.0 with the real address IP
        self.external_address = self.real_address
        if '//0.0.0.0:' in self.real_address:
            self.external_address = self.real_address.replace('//0.0.0.0:', '//{}:'.format(
                socket.gethostbyname(socket.gethostname())))

        logging.info('ZMQ interface configured (address {}) @ {}, external address @ {}'.format(
            self.address, self.real_address, self.external_address))

    def close(self):
        if self.socket:
            try:
                logging.info('closing ZMQ socket')
                self.socket.close()
            except:
                pass

    async def receive(self):
        logging.info("ZMQ interface listening for requests with pid {}...".format(os.getpid()))

        req = await self.socket.recv()

        logging.info("ZMQ interface received request ...")

        return json.loads(bytes.decode(req))

    async def reply(self, replyMsg):
        await self.socket.send(str.encode(replyMsg))
