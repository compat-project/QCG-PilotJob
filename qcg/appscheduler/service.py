import argparse
import logging
import asyncio
import os
import sys
from os.path import exists, join, abspath
import shutil
import traceback

from qcg.appscheduler.slurmenv import parse_slurm_resources
from qcg.appscheduler.errors import InvalidArgument
from qcg.appscheduler.executor import Executor
from qcg.appscheduler.manager import Manager
from qcg.appscheduler.fileinterface import FileInterface
from qcg.appscheduler.zmqinterface import ZMQInterface
from qcg.appscheduler.receiver import Receiver


class QCGPMService:

	def __init__(self):
		parser = argparse.ArgumentParser()
		parser.add_argument("--net",      
				help="enable network interface",
				action="store_true")
		parser.add_argument("--net-port",
				help="port to listen for network interface",
				type=int, default=5555)
		parser.add_argument("--file",
				help="enable file interface",
				action="store_true")
		parser.add_argument("--file-path",
				help="path to the request file",
				default="qcg_pm_reqs.json")
		parser.add_argument("--wd",
				help="working directory for the service",
				default=".")
		parser.add_argument("--exschema",
				help="execution schema [slurm|direct] (direct by default)",
				default="direct")
		self.__args = parser.parse_args()

		if not self.__args.net and not self.__args.file:
			raise InvalidArgument("no interface enabled - finishing")

		self.__wd = self.__args.wd

		self.__setupLogging()
		self.__setupReports()

		self.__conf = {
				Executor.EXECUTOR_WD: self.__args.wd,
				Executor.EXECUTION_SCHEMA: self.__args.exschema,
				FileInterface.CONF_FILE_PATH: self.__args.file_path,
				ZMQInterface.CONF_IP_ADDRESS: "*",
				ZMQInterface.CONF_PORT:       self.__args.net_port
			}

		self.__ifaces = []
		if self.__args.net:
			iface = ZMQInterface()
			iface.setup(self.__conf)
			self.__ifaces.append(iface)

		if self.__args.file:
			iface = FileInterface()
			iface.setup(self.__conf)
			self.__ifaces.append(iface)

		self.__manager = Manager(parse_slurm_resources(), self.__conf)
		self.__notifId = self.__manager.registerNotifier(self.__jobNotify, self.__manager)
		self.__receiver = Receiver(self.__manager, self.__ifaces)


	def __setupReports(self):
		self.__jobReportFile = join(self.__wd, 'jobs.report')

		if exists(self.__jobReportFile):
			os.remove(self.__jobReportFile)


	def __setupLogging(self):
		self.__logFile = join(self.__wd, 'service.log')

		if exists(self.__logFile):
			os.remove(self.__logFile)

		rootLogger = logging.getLogger()
		handler = logging.FileHandler(filename=self.__logFile, mode='a', delay=False)
		handler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
		rootLogger.addHandler(handler)
		rootLogger.setLevel(logging.DEBUG)


	async def __stopInterfaces(self, receiver):
		while not receiver.isFinished:
			await asyncio.sleep(1)

		logging.info("stopping receiver ...")
		receiver.stop()


	def __jobNotify(self, jobId, state, manager):
		if self.__jobReportFile is not None:
			if state.isFinished():
				with open(self.__jobReportFile, 'a') as f:
					job = manager.jobList.get(jobId)
					f.write("%s (%s)\n\t%s\n\t%s\n" % (jobId, state.name,
							"\n\t".join([ "%s: %s" % (str(en[1]), en[0].name) for en in job.history ]),
							"\n\t".join([ "%s: %s" % (k, v) for k, v in job.runtime.items() ])))


	def start(self):
		self.__receiver.run()

		asyncio.get_event_loop().run_until_complete(asyncio.gather(
			self.__stopInterfaces(self.__receiver)
			))

		asyncio.get_event_loop().close()


if __name__ == "__main__":
	try:
		QCGPMService().start()
	except Exception as e:
		sys.stderr.write('Error: %s\n' % (str(e)))
#		traceback.print_exc()
		exit(1)

