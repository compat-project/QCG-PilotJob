import unittest
import json
import logging

import context
from qcg.appscheduler.response import Response, ResponseCode

from appschedulertest import AppSchedulerTest


class TestResponse(AppSchedulerTest):

	def setUp(self):
		self.setupLogging()


	def tearDown(self):
		pass


	def test_ResponseOk(self):
		r = Response.Ok()

		r_d = r.toDict()
		r_json = r.toJSON()
		logging.info("serialized response ok: %s" % (r_json))
		r_dict = json.loads(r_json)

		self.assertTrue(len(r_dict) == 1)
		self.assertTrue(r_dict['code'] == ResponseCode.OK)

		r = Response.Ok('job canceled')
		r_json = r.toJSON()
		logging.info("serialized response ok: %s" % (r_json))
		r_dict = json.loads(r_json)
		self.assertTrue(len(r_dict) == 2)
		self.assertTrue(r_dict['code'] == ResponseCode.OK)
		self.assertTrue(r_dict['message'] == 'job canceled')


	def test_ResponseData(self):
		r = Response.Ok(data = { 'jobId': 101 })
		r_json = r.toJSON()
		logging.info("serialized response with data: %s" % (r_json))
		r_dict = json.loads(r_json)

		self.assertTrue(r_dict['code'] == ResponseCode.OK)
		self.assertTrue(r_dict.get('message') is None)
		self.assertTrue(r_dict.get('data') is not None)
		self.assertTrue(r_dict['data'].get('jobId') is not None)
		self.assertTrue(r_dict['data']['jobId'] == 101)


	def test_ResponseError(self):
		r = Response.Error()

		r_d = r.toDict()
		r_json = r.toJSON()
		logging.info("serialized response error: %s" % (r_json))
		r_dict = json.loads(r_json)

		self.assertTrue(len(r_dict) == 1)
		self.assertTrue(r_dict['code'] == ResponseCode.ERROR)

		r = Response.Error('internal error')
		r_json = r.toJSON()
		logging.info("serialized response error: %s" % (r_json))
		r_dict = json.loads(r_json)
		self.assertTrue(len(r_dict) == 2)
		self.assertTrue(r_dict['code'] == ResponseCode.ERROR)
		self.assertTrue(r_dict['message'] == 'internal error')

		r = Response.Error('job can\'t be canceled', { 'jobId': 101 })
		r_json = r.toJSON()
		logging.info("serialized response error: %s" % (r_json))
		r_dict = json.loads(r_json)
		self.assertTrue(r_dict['code'] == ResponseCode.ERROR)
		self.assertTrue(r_dict['message'] == 'job can\'t be canceled')
		self.assertTrue(r_dict.get('data') is not None)
		self.assertTrue(r_dict['data'].get('jobId') is not None)
		self.assertTrue(r_dict['data']['jobId'] == 101)

