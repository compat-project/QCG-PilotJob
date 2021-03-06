import json
import logging
from os.path import exists

from qcg.appscheduler.errors import JobFileNotExist, IllegalJobDescription


class FileInterface:
    CONF_FILE_PATH = 'file.path'

    CONF_DEFAULT = {
        CONF_FILE_PATH: 'jobs.json'
    }

    def __init__(self):
        pass

    @classmethod
    def name(cls):
        return "FILE"

    def setup(self, conf):
        self.data = []
        self.path = str(conf.get(self.CONF_FILE_PATH, self.CONF_DEFAULT[self.CONF_FILE_PATH]))

        if not exists(self.path):
            raise JobFileNotExist(self.path)

        try:
            with open(self.path) as jsonData:
                self.data = json.load(jsonData)
        except Exception as e:
            logging.error("Fail to parse job description: %s" % (e.args[0]))
            raise IllegalJobDescription("Wrong job description: %s" % (e.args[0]))

        if not isinstance(self.data, list):
            raise IllegalJobDescription("Not an array of requests in json file")

    def close(self):
        self.data = []

    async def receive(self):
        if len(self.data) > 0:
            return self.data.pop(0)

        return None

    async def reply(self, replyMsg):
        logging.info("FileInterface reply message: %s" % (replyMsg))
