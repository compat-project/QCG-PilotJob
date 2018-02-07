import unittest
import logging
import string

from os import makedirs, remove
from os.path import join, exists



class AppSchedulerTest(unittest.TestCase):

	def setupLogging(self):
		if not hasattr(self, 'logSetup'):
			self.logsDir = 'test-logs'
			if not exists(self.logsDir):
				makedirs(self.logsDir)

			self.logFile = join(self.logsDir, '%s.log' % (self.__class__.__name__))

	#		if exists(self.logFile):
	#			remove(self.logFile)

	#		print("setup logging to %s" % (self.logFile))
			rootLogger = logging.getLogger()
			rootLogger.handlers = []

			self.fileHandler = logging.FileHandler(filename=self.logFile, mode='a', delay=False)
			self.fileHandler.setFormatter(logging.Formatter('%(asctime)-15s: %(message)s'))
			rootLogger.addHandler(self.fileHandler)
			rootLogger.setLevel(logging.DEBUG)
	#		logging.basicConfig(filename=self.logFile, level=logging.DEBUG)
			self.logSetup = True


	def closeLogging(self):
		if self.fileHandler is not None:
			self.fileHandler.close()
			self.fileHandler = None

			rootLogger = logging.getLogger()
			rootLogger.handlers = []
			

	def compareIgnoringWhiteSpaces(self, str1, str2):
		self.assertEqual(str1.translate(str.maketrans(dict.fromkeys(string.whitespace))),
					     str2.translate(str.maketrans(dict.fromkeys(string.whitespace))))


