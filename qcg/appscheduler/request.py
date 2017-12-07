import json
import logging
import datetime
import math
from string import Template

from qcg.appscheduler.joblist import Job
from qcg.appscheduler.errors import InvalidRequest


class Request:

	'''
	Parse request.

	Args:
		data (dict): parsed data

	Returns:
		req (Request): request object
		env (dict): some additional environment data, e.g. resources info used
			during parsing 'SubmitReq'

	Raises:
		InvalidRequest: in case of wrong or unknown request
	'''
	@classmethod
	def Parse(cls, data, env = None):
		if not isinstance(data, dict) or 'request' not in data or not data['request']:
			raise InvalidRequest('Invalid request')

		if data['request'] not in __REQS__:
			raise InvalidRequest('Unknown request name: %s' % data['request'])

		return __REQS__[data['request']](data, env)



class SubmitReq(Request):

	REQ_NAME = 'submit'

	def __init__(self, reqData, env = None):
		self.jobs = []

		assert reqData is not None

		if 'jobs' not in reqData or not reqData['jobs'] or not isinstance(reqData['jobs'], list):
			raise InvalidRequest('Wrong submit request - missing jobs data')

		newJobs = []
		vars = {
			'sname': 'local',
			'date': str(datetime.datetime.today()),
			'time': str(datetime.time()),
			'dateTime': str(datetime.datetime.now())
				}

		logging.debug("request data contains %d jobs" % (len(reqData['jobs'])))

		for reqJob in reqData['jobs']:
			if not isinstance(reqJob, dict):
				raise InvalidRequest('Wrong submit request - wrong job data')

			haveIterations = False
			start = 0
			end = 1

			# look for 'iterate' directive
			if 'iterate' in reqJob:
				if not isinstance(reqJob['iterate'], list) or len(reqJob['iterate']) != 2:
					raise InvalidRequest('Wrong format of iterative directive: not a two-element list')

				(start, end) = reqJob['iterate'][0:2]
				if start > end:
					raise InvalidRequest('Wrong format of iterative directive: start index larger then stop one')

				vars['its'] = end - start
				vars['it_start'] = start
				vars['it_stop'] = end
				haveIterations = True

				del reqJob['iterate']

			logging.debug("request job params: start(%d), end(%d), haveIters(%s)" %
					(start, end, haveIterations))

			# look for 'split-into' in resources->numCores
			if 'resources' in reqJob and 'numCores' in reqJob['resources']:
				if 'split-into' in reqJob['resources']['numCores']:
					if 'max' in reqJob['resources']['numCores']:
						raise InvalidRequest('Wrong submit request - split-into cores directive mixed with max directive')

					splitInto = reqJob['resources']['numCores']['split-into']
					if not isinstance(splitInto, int) or splitInto <= 0:
						raise InvalidRequest('Wrong submit request - wrong format of cores split-into directive')

					if env is None or not 'resources' in env or env['resources'] is None:
						raise InvalidRequest('Wrong submit request - failed to resolve split-into without resource information')

					splitPart = int(math.floor(env['resources'].totalCores / splitInto))
					if splitPart <= 0:
						raise InvalidRequest('Wrong submit request - split-into cores resolved to zero')

					reqJob['resources']['numCores']['max'] = splitPart

					del reqJob['resources']['numCores']['split-into']

			# look for 'split-into' in resources->numNodes
			if 'resources' in reqJob and 'numNodes' in reqJob['resources']:
				if 'split-into' in reqJob['resources']['numNodes']:
					if 'max' in reqJob['resources']['numNodes']:
						raise InvalidRequest('Wrong submit request - split-into nodes directive mixed with max directive')

					splitInto = reqJob['resources']['numNodes']['split-into']
					if not isinstance(splitInto, int) or splitInto <= 0:
						raise InvalidRequest('Wrong submit request - wrong format of nodes split-into directive')

					if not 'resources' in env or env['resources'] is None:
						raise InvalidRequest('Wrong submit request - failed to resolve split-into without resource information')

					splitPart = int(math.floor(env['resources'].totalNodes / splitInto))
					if splitPart <= 0:
						raise InvalidRequest('Wrong submit request - split-into nodes resolved to zero')

					reqJob['resources']['numNodes']['max'] = splitPart

					del reqJob['resources']['numNodes']['split-into']

			for idx in range(start, end):
				if haveIterations:
					vars['it'] = idx

				try:
					newJobs.append(Job(**self.__replaceVariables(reqJob, vars)))
				except Exception as e:
					logging.exception('Wrong submit request')
					raise InvalidRequest('Wrong submit request - problem with variables') from e

		logging.debug("appending %d jobs to request job list" % (len(newJobs)))
		self.jobs.extend(newJobs)


	def __replaceVariables(self, data, vars):
		return json.loads(Template(json.dumps(data)).safe_substitute(vars))


	def toDict(self):
		res = { 'request': self.REQ_NAME, 'jobs': [ ] }
		for job in self.jobs:
			res['jobs'].append(job.toDict())

		return res


	def toJSON(self):
		return json.dumps(self.toDict(), indent=2)


class JobStatusReq(Request):

	REQ_NAME = 'jobStatus'

	def __init__(self, reqData, env = None):
		assert reqData is not None

		if 'jobName' not in reqData or not isinstance(reqData['jobName'], str) or not reqData['jobName']:
			raise InvalidRequest('Wrong job status request - missing job name')

		self.jobName = reqData['jobName']

	def toDict(self):
		return { 'request': self.REQ_NAME, 'jobName': self.jobName }

	def toJSON(self):
		return json.dumps(self.toDict())


class CancelJobReq(Request):

	REQ_NAME = 'cancelJob'

	def __init__(self, reqData, env = None):
		assert reqData is not None

		if 'jobName' not in reqData or not isinstance(reqData['jobName'], str) or not reqData['jobName']:
			raise InvalidRequest('Wrong job status request - missing job name')

		self.jobName = reqData['jobName']

	def toDict(self):
		return { 'request': self.REQ_NAME, 'jobName': self.jobName }

	def toJSON(self):
		return json.dumps(self.toDict())


class ListJobsReq(Request):

	REQ_NAME = 'listJobs'

	def __init__(self, reqData, env = None):
		pass

	def toDict(self):
		return { 'request': self.REQ_NAME }

	def toJSON(self):
		return json.dumps(self.toDict())


class ResourcesInfoReq(Request):

	REQ_NAME = 'resourcesInfo'

	def __init__(self, reqData, env = None):
		pass

	def toDict(self):
		return { 'request': self.REQ_NAME }

	def toJSON(self):
		return json.dumps(self.toDict())


__REQS__ = {
		SubmitReq.REQ_NAME:			SubmitReq,
		JobStatusReq.REQ_NAME:		JobStatusReq,
		CancelJobReq.REQ_NAME:		CancelJobReq,
		ListJobsReq.REQ_NAME:		ListJobsReq,
		ResourcesInfoReq.REQ_NAME:	ResourcesInfoReq
}


