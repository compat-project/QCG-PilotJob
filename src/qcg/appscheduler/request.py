import datetime
import json
import logging
import math
import uuid
from string import Template

from qcg.appscheduler.errors import InvalidRequest
from qcg.appscheduler.joblist import Job
from qcg.appscheduler.iterscheduler import IterScheduler


class Request:

    @classmethod
    def Parse(cls, data, env=None):
        """
        Parse request.

        Args:
            data (dict): parsed data

        Returns:
            req (Request): request object
            env (dict): some additional environment data, e.g. resources info used
                during parsing 'SubmitReq'

        Raises:
            InvalidRequest: in case of wrong or unknown request
        """
        if not isinstance(data, dict) or 'request' not in data or not data['request']:
            raise InvalidRequest('Invalid request')

        if data['request'] not in __REQS__:
            raise InvalidRequest('Unknown request name: %s' % data['request'])

        return __REQS__[data['request']](data, env)


class ControlReq(Request):
    REQ_NAME = 'control'

    REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE = 'finishAfterAllTasksDone'
    REQ_CONTROL_CMDS = [
        REQ_CONTROL_CMD_FINISHAFTERALLTASKSDONE
    ]

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'command' not in reqData or not isinstance(reqData['command'], str):
            raise InvalidRequest('Wrong control request - missing command')

        if reqData['command'] not in self.REQ_CONTROL_CMDS:
            raise InvalidRequest('Wrong control request - unknown command "%s"' % (reqData['command']))

        self.command = reqData['command']

    def toDict(self):
        return {'request': self.REQ_NAME, 'command': self.command}

    def toJSON(self):
        return json.dumps(self.toDict())


class SubmitReq(Request):
    REQ_NAME = 'submit'
    REQ_CNT = 1

    def __init__(self, reqData, env=None):
        self.jobs = []

        assert reqData is not None

        if 'jobs' not in reqData or not reqData['jobs'] or not isinstance(reqData['jobs'], list):
            raise InvalidRequest('Wrong submit request - missing jobs data')

        newJobs = []
        vars = {
            'rcnt': str(SubmitReq.REQ_CNT),
            'uniq': str(uuid.uuid4()),
            'sname': 'local',
            'date': str(datetime.datetime.today()),
            'time': str(datetime.time()),
            'dateTime': str(datetime.datetime.now())
        }

        SubmitReq.REQ_CNT += 1

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

                vars['uniq'] = str(uuid.uuid4())
                vars['its'] = end - start
                vars['it_start'] = start
                vars['it_stop'] = end
                haveIterations = True

                del reqJob['iterate']

            logging.debug("request job params: start(%d), end(%d), haveIters(%s)" %
                          (start, end, haveIterations))

            if 'resources' not in env or env['resources'] is None:
                raise InvalidRequest(
                    'Wrong submit request - failed to resolve split-into without resource information')

            numCoresPlans = []
            # look for 'split-into' in resources->numCores
            if 'resources' in reqJob and 'numCores' in reqJob['resources']:
                if 'split-into' in reqJob['resources']['numCores']:
                    numCoresPlans = IterScheduler.GetScheduler('split-into').Schedule(reqJob['resources']['numCores'],
                            end - start, env['resources'].totalCores)
                elif 'scheduler' in reqJob['resources']['numCores']:
                    Scheduler = IterScheduler.GetScheduler(reqJob['resources']['numCores']['scheduler'])
                    del reqJob['resources']['numCores']['scheduler']
                    numCoresPlans = Scheduler.Schedule(reqJob['resources']['numCores'],
                            end - start, env['resources'].totalCores)

            numNodesPlans = []
            # look for 'split-into' in resources->numNodes
            if 'resources' in reqJob and 'numNodes' in reqJob['resources']:
                if 'split-into' in reqJob['resources']['numNodes']:
                    numNodesPlans = IterScheduler.GetScheduler('split-into').Schedule(reqJob['resources']['numNodes'],
                            end - start, env['resources'].totalNodes)
                elif 'scheduler' in reqJob['resources']['numNodes']:
                    Scheduler = IterScheduler.GetScheduler(reqJob['resources']['numNodes']['scheduler'])
                    del reqJob['resources']['numNodes']['scheduler']
                    numNodesPlans = Scheduler.Schedule(reqJob['resources']['numNodes'],
                            end - start, env['resources'].totalNodes)

            # default value for missing 'resources' definition
            if 'resources' not in reqJob:
                reqJob['resources'] = { 'numCores': { 'exact': 1 } }

            for idx in range(start, end):
                if haveIterations:
                    vars['it'] = idx

                try:
                    reqJob_vars = self.__replaceVariables(reqJob, vars)

                    varsStep2 = {
                        'jname': reqJob_vars['name']
                    }

                    logging.debug("replacing jname variable with %s" % (reqJob['name']))

                    reqJob_vars = self.__replaceVariables(reqJob_vars, varsStep2)

                    if numCoresPlans is not None and len(numCoresPlans) > idx - start:
                        reqJob_vars['resources']['numCores'] = numCoresPlans[idx - start]

                    if numNodesPlans is not None and len(numNodesPlans) > idx - start:
                        reqJob_vars['resources']['numNodes'] = numNodesPlans[idx - start]

                    newJobs.append(Job(**reqJob_vars))
                except Exception as e:
                    logging.exception('Wrong submit request')
                    raise InvalidRequest('Wrong submit request - problem with variables') from e

        logging.debug("appending %d jobs to request job list" % (len(newJobs)))
        self.jobs.extend(newJobs)

    def __replaceVariables(self, data, vars):
        if vars is not None and len(vars) > 0:
            return json.loads(Template(json.dumps(data)).safe_substitute(vars))
        else:
            return data

    def toDict(self):
        res = {'request': self.REQ_NAME, 'jobs': []}
        for job in self.jobs:
            res['jobs'].append(job.toDict())

        return res

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)


class JobStatusReq(Request):
    REQ_NAME = 'jobStatus'

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'jobNames' not in reqData or not isinstance(reqData['jobNames'], list) or len(reqData['jobNames']) < 1:
            raise InvalidRequest('Wrong job status request - missing job names')

        self.jobNames = reqData['jobNames']

    def toDict(self):
        return {'request': self.REQ_NAME, 'jobNames': self.jobNames}

    def toJSON(self):
        return json.dumps(self.toDict())


class JobInfoReq(Request):
    REQ_NAME = 'jobInfo'

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'jobNames' not in reqData or not isinstance(reqData['jobNames'], list) or len(reqData['jobNames']) < 1:
            raise InvalidRequest('Wrong job status request - missing job names')

        self.jobNames = reqData['jobNames']

    def toDict(self):
        return {'request': self.REQ_NAME, 'jobNames': self.jobNames}

    def toJSON(self):
        return json.dumps(self.toDict())


class CancelJobReq(Request):
    REQ_NAME = 'cancelJob'

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'jobName' not in reqData or not isinstance(reqData['jobName'], str) or not reqData['jobName']:
            raise InvalidRequest('Wrong job status request - missing job name')

        self.jobName = reqData['jobName']

    def toDict(self):
        return {'request': self.REQ_NAME, 'jobName': self.jobName}

    def toJSON(self):
        return json.dumps(self.toDict())


class RemoveJobReq(Request):
    REQ_NAME = 'removeJob'

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'jobNames' not in reqData or not isinstance(reqData['jobNames'], list) or len(reqData['jobNames']) < 1:
            raise InvalidRequest('Wrong remove job request - missing job names')

        self.jobNames = reqData['jobNames']

    def toDict(self):
        return {'request': self.REQ_NAME, 'jobNames': self.jobNames }

    def toJSON(self):
        return json.dumps(self.toDict())


class ListJobsReq(Request):
    REQ_NAME = 'listJobs'

    def __init__(self, reqData, env=None):
        pass

    def toDict(self):
        return {'request': self.REQ_NAME}

    def toJSON(self):
        return json.dumps(self.toDict())


class ResourcesInfoReq(Request):
    REQ_NAME = 'resourcesInfo'

    def __init__(self, reqData, env=None):
        pass

    def toDict(self):
        return {'request': self.REQ_NAME}

    def toJSON(self):
        return json.dumps(self.toDict())


class FinishReq(Request):
    REQ_NAME = 'finish'

    def __init__(self, reqData, env=None):
        pass

    def toDict(self):
        return {'request': self.REQ_NAME}

    def toJSON(self):
        return json.dumps(self.toDict())


__REQS__ = {
    ControlReq.REQ_NAME: ControlReq,
    SubmitReq.REQ_NAME: SubmitReq,
    JobStatusReq.REQ_NAME: JobStatusReq,
    JobInfoReq.REQ_NAME: JobInfoReq,
    CancelJobReq.REQ_NAME: CancelJobReq,
    RemoveJobReq.REQ_NAME: RemoveJobReq,
    ListJobsReq.REQ_NAME: ListJobsReq,
    ResourcesInfoReq.REQ_NAME: ResourcesInfoReq,
    FinishReq.REQ_NAME: FinishReq
}
