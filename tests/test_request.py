import unittest
import json
import logging

import context
from qcg.appscheduler.resources import Node, Resources
from qcg.appscheduler.joblist import JobState, Job, JobExecution, ResourceSize, JobResources
from qcg.appscheduler.request import Request, SubmitReq, JobStatusReq, CancelJobReq, ListJobsReq, ResourcesInfoReq
from qcg.appscheduler.errors import InvalidRequest

from appschedulertest import AppSchedulerTest


class TestRequest(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()

	def tearDown(self):
		pass


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
	

	def test_SubmitReq(self):
		sreq_json = '''{
	"request": "submit",
	"jobs": [ {
		"name": "msleep2",
		"execution": {
		  "exec": "/usr/bin/sleep",
		  "args": [
			"5s"
		  ],
		  "env": {},
		  "wd": "/home/kieras/app-scheduler/src/repo/test-sandbox/sleep2.sandbox",
		  "stdout": "sleep2.stdout",
		  "stderr": "sleep2.stderr"
		},
		"resources": {
		  "numCores": {
			"exact": 2
		  }
		}
	  }
	  ]
  }'''

		sreq_dict = json.loads(sreq_json)

		sreq = SubmitReq(sreq_dict)
		self.assertIsNotNone(sreq)

		sreq_json_copy = sreq.toJSON()
		self.compareIgnoringWhiteSpaces(sreq_json, sreq_json_copy)

		sreq_copy = Request.Parse(sreq_dict)

		self.assertIsNotNone(sreq_copy)
		self.assertTrue(isinstance(sreq_copy, SubmitReq))

		sreq_json_copy2 = sreq_copy.toJSON()
		self.compareIgnoringWhiteSpaces(sreq_json, sreq_json_copy2)

		try:
			sreq2 = SubmitReq({ 'request': 'submit2', 'jobs': None })
			self.fail('Invalid data for submit request')
		except InvalidRequest:
			pass	

		try:
			sreq2 = SubmitReq({ 'request': 'submit', 'jobs': None })
			self.fail('Invalid data for submit request')
		except InvalidRequest:
			pass

		try:
			sreq2 = SubmitReq({ 'request': 'submit', 'jobs': [ { 'somebullshit': None } ] })
			self.fail('Invalid data for submit request')
		except InvalidRequest as e:
			logging.exception(e)
			pass

		sreq2 = SubmitReq({ 'request': 'submit', 'jobs': [ {
			'name': 'job1',
			'execution': {
				'exec': '/bin/echo'
				},
			'resources': {
				'numCores': {
					'exact': 1
					}
				}
			} ] } )
		self.assertIsNotNone(sreq2)


		sreq3 = SubmitReq({ 'request': 'submit', 'jobs': [ {
			'name': 'job_${it}',
			'iterate': [ 1, 3 ],
			'execution': {
				'exec': '/bin/echo',
		  		'args': [ '${its}', '${it_start}', '${it_stop}' ]
				},
			'resources': {
				'numCores': {
					'exact': 1
					}
				}
			} ] } )
		self.assertIsNotNone(sreq3)
		self.assertTrue(len(sreq3.jobs) == 2)
		self.assertTrue(sreq3.jobs[0].name == 'job_1')
		self.assertTrue(sreq3.jobs[1].name == 'job_2')
		self.assertTrue(sreq3.jobs[0].execution.args[0] == '2')
		self.assertTrue(sreq3.jobs[0].execution.args[1] == '1')
		self.assertTrue(sreq3.jobs[0].execution.args[2] == '3')
		logging.info("serialized iteratable submit request: %s" % (sreq3.toJSON()))


	def test_SubmitReqWithSplitInto(self):
		resources = self.createLocalResources()
		reqEnv = { 'resources': resources }

		logging.info("resources with total nodes (%d), total cores (%d)" %
				(resources.totalNodes, resources.totalCores))
		sreq_json = '''{
	"request": "submit",
	"jobs": [ {
		"name": "job_1",
		"execution": {
		  "exec": "/usr/bin/sleep",
		  "args": [
			"5s"
		  ],
		  "stdout": "sleep2.stdout",
		  "stderr": "sleep2.stderr"
		},
		"resources": {
		  "numCores": {
			"min": 2,
			"split-into": 3
		  }
		}
	  }
	  ]
  }'''

		sreq_dict = json.loads(sreq_json)

		sreq = SubmitReq(sreq_dict, reqEnv)
		logging.info("submit request with split-into: %s" % (sreq.toJSON()))
		self.assertIsNotNone(sreq)
		self.assertTrue(len(sreq.jobs) == 1)
		self.assertTrue(sreq.jobs[0].resources.hasCores())
		self.assertTrue(sreq.jobs[0].resources.numCores.max is not None and
				isinstance(sreq.jobs[0].resources.numCores.max, int))

		sreq_json = '''{
	"request": "submit",
	"jobs": [ {
		"name": "job_1",
		"execution": {
		  "exec": "/usr/bin/sleep",
		  "args": [
			"5s"
		  ],
		  "stdout": "sleep2.stdout",
		  "stderr": "sleep2.stderr"
		},
		"resources": {
		  "numCores": {
			"max": 2,
			"split-into": 3
		  }
		}
	  }
	  ]
  }'''

		try:
			sreq_dict = json.loads(sreq_json)
			sreq = SubmitReq(sreq_dict, reqEnv)
			self.fail("Invalid job description parsed (max & split-into)")
		except InvalidRequest:
			pass
		except Exception:
			self.fail("Incorrect exception class for invalid job description (max & split-into)")

		sreq_json = '''{
	"request": "submit",
	"jobs": [ {
		"name": "job_1",
		"execution": {
		  "exec": "/usr/bin/sleep",
		  "args": [
			"5s"
		  ],
		  "stdout": "sleep2.stdout",
		  "stderr": "sleep2.stderr"
		},
		"resources": {
		  "numCores": {
			"min": 2,
			"split-into": 20
		  }
		}
	  }
	  ]
  }'''

		try:
			sreq_dict = json.loads(sreq_json)
			sreq = SubmitReq(sreq_dict, reqEnv)
			self.fail("Invalid job description parsed (to many split-into parts)")
		except InvalidRequest:
			pass
		except Exception:
			self.fail("Incorrect exception class for invalid job description (to many split-into parts)")


	def test_JobStatusReq(self):
		jsreq_json = '''{
			"request": "jobStatus",
			"jobName": "job1"
 	 	}'''

		jsreq_dict = json.loads(jsreq_json)

		jsreq = JobStatusReq(jsreq_dict)
		self.assertIsNotNone(jsreq)

		jsreq_json_copy = jsreq.toJSON()
		self.compareIgnoringWhiteSpaces(jsreq_json, jsreq_json_copy)

		jsreq_copy = Request.Parse(jsreq_dict)
		self.assertIsNotNone(jsreq_copy)
		self.assertTrue(isinstance(jsreq_copy, JobStatusReq))

		jsreq_json_copy2 = jsreq_copy.toJSON()
		self.compareIgnoringWhiteSpaces(jsreq_json, jsreq_json_copy2)

		try:
			sreq2 = JobStatusReq({ 'request': 'jobStatus', 'jobName': None })
			self.fail('Invalid data for job status request')
		except InvalidRequest:
			pass	

		try:
			sreq2 = JobStatusReq({ 'request': 'jobStatus', 'jobName': '' })
			self.fail('Invalid data for job status request')
		except InvalidRequest:
			pass	

		try:
			sreq2 = JobStatusReq({ 'request': 'jobStatus' })
			self.fail('Invalid data for job status request')
		except InvalidRequest:
			pass	

		try:
			sreq2 = JobStatusReq({ 'request': 'jobStatus', 'jobName': [ 'ble', 'ble2' ] })
			self.fail('Invalid data for job status request')
		except InvalidRequest:
			pass	

		sreq2 = JobStatusReq({ 'request': 'jobStatus', 'jobName': 'job1', 'duperele': None })


	def test_CancelReq(self):
		creq_json = '''{
			"request": "cancelJob",
			"jobName": "job1"
 	 	}'''

		creq_dict = json.loads(creq_json)

		creq = CancelJobReq(creq_dict)
		self.assertIsNotNone(creq)

		creq_json_copy = creq.toJSON()
		self.compareIgnoringWhiteSpaces(creq_json, creq_json_copy)

		creq_copy = Request.Parse(creq_dict)
		self.assertIsNotNone(creq_copy)
		self.assertTrue(isinstance(creq_copy, CancelJobReq))

		creq_json_copy2 = creq_copy.toJSON()
		self.compareIgnoringWhiteSpaces(creq_json, creq_json_copy2)

		try:
			creq2 = CancelJobReq({ 'request': 'cancelJob', 'jobName': None })
			self.fail('Invalid data for job cancel request')
		except InvalidRequest:
			pass	

		try:
			creq2 = CancelJobReq({ 'request': 'cancelJob', 'jobName': '' })
			self.fail('Invalid data for job cancel request')
		except InvalidRequest:
			pass	

		try:
			creq2 = CancelJobReq({ 'request': 'cancelJob' })
			self.fail('Invalid data for job cancel request')
		except InvalidRequest:
			pass	

		try:
			creq2 = CancelJobReq({ 'request': 'cancelJob', 'jobName': [ 'ble', 'ble2' ] })
			self.fail('Invalid data for job cancel request')
		except InvalidRequest:
			pass	

		creq2 = CancelJobReq({ 'request': 'cancelJob', 'jobName': 'job1', 'duperele': None })


	def test_ListJobsReq(self):
		ljreq_json = '''{
			"request": "listJobs"
 	 	}'''

		ljreq_dict = json.loads(ljreq_json)

		creq = ListJobsReq(ljreq_dict)
		self.assertIsNotNone(creq)

		ljreq_json_copy = creq.toJSON()
		self.compareIgnoringWhiteSpaces(ljreq_json, ljreq_json_copy)

		ljreq_copy = Request.Parse(ljreq_dict)
		self.assertIsNotNone(ljreq_copy)
		self.assertTrue(isinstance(ljreq_copy, ListJobsReq))

		ljreq_json_copy2 = ljreq_copy.toJSON()
		self.compareIgnoringWhiteSpaces(ljreq_json, ljreq_json_copy2)

		ljreq2 = ListJobsReq({ 'request': 'listJobs', 'jobName': 'job1', 'duperele': None })


	def test_ResourcesInfoReq(self):
		rireq_json = '''{
			"request": "resourcesInfo"
 	 	}'''

		rireq_dict = json.loads(rireq_json)

		creq = ResourcesInfoReq(rireq_dict)
		self.assertIsNotNone(creq)

		rireq_json_copy = creq.toJSON()
		self.compareIgnoringWhiteSpaces(rireq_json, rireq_json_copy)

		rireq_copy = Request.Parse(rireq_dict)
		self.assertIsNotNone(rireq_copy)
		self.assertTrue(isinstance(rireq_copy, ResourcesInfoReq))

		rireq_json_copy2 = rireq_copy.toJSON()
		self.compareIgnoringWhiteSpaces(rireq_json, rireq_json_copy2)

		rireq2 = ResourcesInfoReq({ 'request': 'resourcesInfo', 'jobName': 'job1', 'duperele': None })

