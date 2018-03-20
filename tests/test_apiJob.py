import unittest
import json
import string
import logging

import context
from qcg.appscheduler.api.job import Jobs
from qcg.appscheduler.api.errors import *
from qcg.appscheduler.request import Request

from appschedulertest import AppSchedulerTest


class TestApiJob(AppSchedulerTest):

    def setUp(self):
        self.setupLogging()

    def tearDown(self):
        self.closeLogging()

    def test_JobNameUniq(self):
        j = Jobs()

        # validate job name uniqness
        j.add(name = 'j1', exec = '/bin/date')

        self.assertEqual(len(j.jobNames()), 1)
        self.assertEqual(j.jobNames()[0], 'j1')

        try:
            j.add(name = 'j1', exec = '/bin/date' )
            self.fail("Duplicated job names in job list")
        except InvalidJobDescription:
            pass

        try:
            j.addStd(name = 'j1', execution = { 'exec': '/bin/date' })
            self.fail("Duplicated job names in job list")
        except InvalidJobDescription:
            pass

        j.remove('j1')
        j.addStd(name = 'j1', execution = { 'exec': '/bin/date' })

        self.assertEqual(len(j.jobNames()), 1)
        self.assertEqual(j.jobNames()[0], 'j1')

        try:
            j.add(name = 'j1', exec = '/bin/date')
            self.fail("Duplicated job names in job list")
        except InvalidJobDescription:
            pass


    def test_JobValidation(self):
        j = Jobs()

        try:
            j.add(name ='j1', args = [ '/bin/date' ])
            self.fail('Missing exec name accepted')
        except InvalidJobDescription:
            pass
        self.assertEqual(len(j.jobNames()), 0)

        try:
            j.addStd( name = 'j1' )
            self.fail('Missing exec name accepted')
        except InvalidJobDescription:
            pass
        self.assertEqual(len(j.jobNames()), 0)

        try:
            j.addStd( execution = { 'exec': '/bin/date' })
            self.fail('Missing job name accepted')
        except InvalidJobDescription:
            pass
        self.assertEqual(len(j.jobNames()), 0)


    def testJobConversion(self):
        j = Jobs()

        log = logging.getLogger('testJobConversion')

        # create a simple job description
        j1Data = { 'exec': '/bin/date', 'args': [ '1', '2' ], 'stdin': 'std.in',
            'stdout': 'std.out', 'stderr': 'std.err', 'wd': 'wd_path', 'wt': '2h',
            'numNodes': { 'min': 2, 'max': 3 }, 'numCores': { 'min': 1, 'max': 2 },
            'iterate': [ 1, 10 ], 'after': 'j2' }
        j.add(j1Data, name = 'j1')
        self.assertEqual(len(j.jobNames()), 1)
        self.assertEqual(j.jobNames()[0], 'j1')

        # get document with standard descriptions
        stdJobs = j.jobs()
        log.debug('got output document: %s' % (str(stdJobs)))
        self.assertEqual(len(stdJobs), 1)
        self.assertEqual(stdJobs[0]['name'], 'j1')

        # compate standard job description with original, simple one
        stdJob = stdJobs[0]
        self.assertEqual(stdJob['execution']['exec'], j1Data['exec'])
        self.assertEqual(len(stdJob['execution']['args']), len(j1Data['args']))
#        for idx, arg in enumerate(stdJob['execution']['args']):
#            self.assertEqual(arg, j1Data['args'][idx])
        self.assertEqual(stdJob['execution']['args'], j1Data['args'])
        self.assertEqual(stdJob['execution']['stdin'], j1Data['stdin'])
        self.assertEqual(stdJob['execution']['stdout'], j1Data['stdout'])
        self.assertEqual(stdJob['execution']['stderr'], j1Data['stderr'])
        self.assertEqual(stdJob['execution']['wd'], j1Data['wd'])
        self.assertEqual(stdJob['resources']['wt'], j1Data['wt'])
        self.assertEqual(stdJob['resources']['numCores'], j1Data['numCores'])
        self.assertEqual(stdJob['resources']['numNodes'], j1Data['numNodes'])
        self.assertEqual(stdJob['iterate'], j1Data['iterate'])
        self.assertEqual(stdJob['dependencies']['after'], j1Data['after'])

        # reverse convertion - std -> simple
        j1Data_conv_name, j1Data_conv = j.convertStdToSimple(stdJob)
        self.assertEqual(j1Data_conv_name, 'j1')
        self.assertEqual(j1Data_conv, j1Data)


    def testSubmitRequest(self):
        log = logging.getLogger('testSubmitRequest')

        j = Jobs()
        j1Data = { 'exec': '/bin/date', 'args': [ '1', '2' ], 'stdin': 'std.in',
            'stdout': 'std.out', 'stderr': 'std.err', 'wd': 'wd_path', 'wt': '2h',
            'numNodes': { 'min': 2, 'max': 3 }, 'numCores': { 'min': 1, 'max': 2 },
            'iterate': [ 1, 10 ], 'after': 'j2' }
        j.add(name = 'j1', **j1Data)

        req = json.dumps( { 'request': 'submit',
                            'jobs': j.jobs() })

        log.debug('submit request: %s' % str(req))

        req = Request.Parse(json.loads(req))


    def testSerialization(self):
        log = logging.getLogger('testSerialization')

        j1 = Jobs()
        j1Data = { 'name': 'j1', 'exec': '/bin/date', 'args': [ '1', '2' ], 'stdin': 'std.in',
            'stdout': 'std.out', 'stderr': 'std.err', 'wd': 'wd_path', 'wt': '2h',
            'numNodes': { 'min': 2, 'max': 3 }, 'numCores': { 'min': 1, 'max': 2 },
            'iterate': [ 1, 10 ], 'after': 'j2' }
        j1.add(**j1Data)
        j2Data = { 'name': 'j2', 'exec': '/bin/echo', 'args': [ '--verbose' ], 'stdin': 'echo.in',
            'stdout': 'echo.out', 'stderr': 'echo.err', 'wd': 'echo.wd', 'wt': '10m',
            'numNodes': { 'exact': 1 }, 'numCores': { 'exact': 4 },
            'iterate': [ 1, 2 ], 'after': 'j1' }
        j1.add(**j2Data)

        j1.saveToFile('jobs.json')

        j2 = Jobs()
        j2.loadFromFile('jobs.json')

        self.assertEqual(j1.jobNames(), j2.jobNames())
        self.assertEqual(j1.jobs(), j2.jobs())


if __name__ == '__main__':
	unittest.main()
