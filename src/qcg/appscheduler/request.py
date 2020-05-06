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


class RegisterReq(Request):
    REQ_NAME = 'register'

    REQ_REGISTER_ENTITIES = [
        'manager'
    ]

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'entity' not in reqData or not reqData['entity'] in self.REQ_REGISTER_ENTITIES:
            raise InvalidRequest('Wrong register request - missing/unknown entity')

        if 'params' not in reqData:
            raise InvalidRequest('Wrong register request - missing register parameters')

        if not all(e in reqData['params'] for e in ['id', 'address', 'resources']):
            raise InvalidRequest('Wrong register request - missing key register parameters')

        self.entity = reqData['entity']
        self.params = reqData['params']

    def toDict(self):
        return {'request': self.REQ_NAME, 'entity': self.entity, 'params': self.params}

    def toJSON(self):
        return json.dumps(self.toDict())


class SubmitReq(Request):
    REQ_NAME = 'submit'
    REQ_CNT = 1

    def __init__(self, reqData, env=None):
        self.jobReqs = [ ]

        assert reqData is not None

        if 'jobs' not in reqData or not reqData['jobs'] or not isinstance(reqData['jobs'], list):
            raise InvalidRequest('Wrong submit request - missing jobs data')

        # watch out for values - this data can be copied with the 'shallow' method
        # so complex structures should be omited
        vars = {
            'rcnt': str(SubmitReq.REQ_CNT),
            'uniq': str(uuid.uuid4()),
            'sname': 'local',
            'date': str(datetime.datetime.today()),
            'time': str(datetime.time()),
            'dateTime': str(datetime.datetime.now())
        }

        SubmitReq.REQ_CNT += 1

        logging.debug('request data contains {} jobs'.format(len(reqData['jobs'])))
        newJobs = []

        for reqJob in reqData['jobs']:
            if not isinstance(reqJob, dict):
                raise InvalidRequest('Wrong submit request - wrong job data')

            if not 'name' in reqJob:
                raise InvalidRequest('Missing name in job description')

            if not 'execution' in reqJob:
                raise InvalidRequest('Missing execution element in job description')

            # look for 'iterate' directive
            if 'iteration' in reqJob:
                if not isinstance(reqJob['iteration'], dict) or 'stop' not in reqJob['iteration']:
                    raise InvalidRequest('Wrong format of iteration directive: not a dictionary')

                start = reqJob['iteration'].get('start', 0)
                end = reqJob['iteration']['stop']
                if start > end:
                    raise InvalidRequest('Wrong format of iteration directive: start index larger then stop one')

            # default value for missing 'resources' definition
            if 'resources' not in reqJob:
                reqJob['resources'] = { 'numCores': { 'exact': 1 } }

            newJobs.append({ 'req': reqJob, 'vars': vars.copy() })

        self.jobReqs.extend(newJobs)


    def toDict(self):
        res = {'request': self.REQ_NAME, 'jobs': []}
        for job in self.jobReqs:
            res['jobs'].append(job['req'])

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
            raise InvalidRequest('Wrong job info request - missing job names')

        self.includeChilds = False
        if reqData.get('params'):
            if reqData['params'].get('withChilds'):
                self.includeChilds = True
        self.jobNames = reqData['jobNames']

    def toDict(self):
        result = {'request': self.REQ_NAME, 'jobNames': self.jobNames}
        if self.includeChilds:
            params = {'withChilds': True}
            result['params'] = params
        return result

    def toJSON(self):
        return json.dumps(self.toDict())


class CancelJobReq(Request):
    REQ_NAME = 'cancelJob'

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'jobNames' not in reqData or not isinstance(reqData['jobNames'], list) or len(reqData['jobNames']) < 1:
            raise InvalidRequest('Wrong cancel job request - missing job names')

        self.jobNames = reqData['jobNames']

    def toDict(self):
        return {'request': self.REQ_NAME, 'jobNames': self.jobNames}

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


class StatusReq(Request):
    REQ_NAME = 'status'

    def __init__(self, reqData, env=None):
        pass

    def toDict(self):
        return {'request': self.REQ_NAME}

    def toJSON(self):
        return json.dumps(self.toDict())


class NotifyReq(Request):
    REQ_NAME = 'notify'

    NOTIFY_ENTITY = [
        'job'
    ]

    def __init__(self, reqData, env=None):
        assert reqData is not None

        if 'entity' not in reqData or not reqData['entity'] in self.NOTIFY_ENTITY:
            raise InvalidRequest('Wrong notify request - missing/unknown entity')

        if 'params' not in reqData:
            raise InvalidRequest('Wrong notify request - missing register parameters')

        if not all(e in reqData['params'] for e in ['name', 'state', 'attributes']):
            raise InvalidRequest('Wrong notify request - missing key notify parameters')

        self.entity = reqData['entity']
        self.params = reqData['params']

    def toDict(self):
        return {'request': self.REQ_NAME, 'entity': self.entity, 'params': self.params}

    def toJSON(self):
        return json.dumps(self.toDict())


__REQS__ = {
    RegisterReq.REQ_NAME: RegisterReq,
    ControlReq.REQ_NAME: ControlReq,
    SubmitReq.REQ_NAME: SubmitReq,
    JobStatusReq.REQ_NAME: JobStatusReq,
    JobInfoReq.REQ_NAME: JobInfoReq,
    CancelJobReq.REQ_NAME: CancelJobReq,
    RemoveJobReq.REQ_NAME: RemoveJobReq,
    ListJobsReq.REQ_NAME: ListJobsReq,
    ResourcesInfoReq.REQ_NAME: ResourcesInfoReq,
    FinishReq.REQ_NAME: FinishReq,
    StatusReq.REQ_NAME: StatusReq,
    NotifyReq.REQ_NAME: NotifyReq,
}
