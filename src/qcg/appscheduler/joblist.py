import json
import re
import logging

from datetime import datetime, timedelta
from enum import Enum
from qcg.appscheduler.resources import CRType

from qcg.appscheduler.errors import JobAlreadyExist, IllegalResourceRequirements, \
    IllegalJobDescription


class JobState(Enum):
    QUEUED = 1
    SCHEDULED = 2
    EXECUTING = 3
    SUCCEED = 4
    FAILED = 5
    CANCELED = 6
    OMITTED = 7

    def isFinished(self):
        return self in [JobState.SUCCEED, JobState.FAILED, JobState.CANCELED,
                        JobState.OMITTED]


class JobExecution:
    def __init__(self, exec=None, args=None, env=None, script=None, wd=None, \
                 stdin=None, stdout=None, stderr=None, modules=None, venv=None, model=None):
        if all((not exec, not script)):
            raise IllegalJobDescription("Job execution (exec or script) not defined")

        if script and (exec or args or env):
            raise IllegalJobDescription("Job script and exec or args or env defined")

        self.exec = exec
        self.script = script

        self.args = []
        self.env = {}

        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        if modules and not isinstance(modules, list):
            self.modules = [ modules ]
        else:
            self.modules = modules

        self.venv = venv

        if model and isinstance(model, str):
            self.model = model.lower()
        else:
            self.model = model

        if args is not None:
            if not isinstance(args, list):
                raise IllegalJobDescription("Execution arguments must be an array")
            self.args = args

        if env is not None:
            if not isinstance(env, dict):
                raise IllegalJobDescription("Execution environment must be an dictionary")
            self.env = env

        self.wd = wd

    def toDict(self):
        result = {'exec': self.exec, 'args': self.args, 'env': self.env, 'script': self.script}
        if self.wd is not None:
            result['wd'] = self.wd

        if self.stdin is not None:
            result['stdin'] = self.stdin

        if self.stdout is not None:
            result['stdout'] = self.stdout

        if self.stderr is not None:
            result['stderr'] = self.stderr

        if self.modules is not None:
            result['modules'] = self.modules

        if self.venv is not None:
            result['venv'] = self.venv

        if self.model is not None:
            result['model'] = self.model

        return result

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)


class ResourceSize:
    def __init__(self, exact=None, min=None, max=None, scheduler=None):
        if exact is not None and (min is not None or max is not None or scheduler is not None):
            raise IllegalResourceRequirements(
                "Exact number of resources defined with min/max/scheduler")

        if max is not None and min is not None and min > max:
            raise IllegalResourceRequirements("Maximum number greater than minimal")

        if exact is None and min is None and max is None:
            raise IllegalResourceRequirements("No resources defined")

        if (exact is not None and exact < 0) or (min is not None and min < 0) or (max is not None and max < 0):
            raise IllegalResourceRequirements("Neative number of resources")

        self.__exact = exact
        self.__min = min
        self.__max = max
        self.__scheduler = scheduler

    @property
    def exact(self):
        return self.__exact

    @property
    def min(self):
        return self.__min

    @property
    def max(self):
        return self.__max

    @property
    def scheduler(self):
        return self.__scheduler

    @property
    def range(self):
        return (self.__min, self.__max)

    def isExact(self):
        return self.__exact is not None

    def toDict(self):
        result = {}

        if self.__exact is not None:
            result['exact'] = self.__exact

        if self.__min is not None:
            result['min'] = self.__min

        if self.__max is not None:
            result['max'] = self.__max

        if self.__scheduler is not None:
            result['scheduler'] = self.__scheduler

        return result

    def toJSON(self):
        return json.dumps(self.toDict())


class JobResources:
    __wtRegex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')

    def __parseWt(self, wt):
        parts = None

        parts = self.__wtRegex.match(wt)

        if not parts:
            raise IllegalResourceRequirements("Wrong wall time format")

        try:
            parts = parts.groupdict()
            timeParams = {}
            for name, param in parts.items():
                if param:
                    timeParams[name] = int(param)

            td = timedelta(**timeParams)
            if td.total_seconds() == 0:
                raise IllegalResourceRequirements("Wall time must be greater than 0")

            return td
        except IllegalResourceRequirements:
            raise
        except:
            raise IllegalResourceRequirements("Wrong wall time format")


    def __validateCrs(self, crs):
        """
        Validate consumable resources.
        Check if crs are known and well defined.

        Args:
            crs (dict(string, int)) - map with consumable resources.

        Returns:
            final map of crs
        """
        if crs:
            # already used crs
            result = dict()

            for name, count in crs.items():
                if not name.upper() in CRType.__members__:
                    raise IllegalResourceRequirements("Unknown consumable resource {}".format(name))

                crType = CRType[name.upper()]
                if crType in result:
                    raise IllegalResourceRequirements("Consumable resource {} already defined".format(name))

                if not isinstance(count, int):
                    raise IllegalResourceRequirements("Consumable resource {} count not a number {}".format(name, type(count).__name__))

                if count < 1:
                    raise IllegalResourceRequirements("Number of consumable resource {} must be greater than 0".format(name))

                result[crType] = count

        return result


    def __init__(self, numCores=None, numNodes=None, wt=None, nodeCrs=None):
        """
        Job resource requirements.

        * if numNodes > 1, then numCores relates to each of the node, so total number of
                required cores will be a product of numNodes and numCores
        * nodeCrs relates to each node available consumable resources

        Args:
            numCores - number of cores, either as exact number or as a range
            numNodes - number of nodes, either as exact number of as a range
            wt - wall time
            nodeCrs (dict(string,int)) - each node consumable resources
        """
        if numCores is None and numNodes is None:
            raise IllegalResourceRequirements("No resources defined")

        if numCores is not None:
            if isinstance(numCores, int):
                numCores = ResourceSize(numCores)
            elif isinstance(numCores, dict):
                numCores = ResourceSize(**numCores)
            elif not isinstance(numCores, ResourceSize):
                raise IllegalJobDescription("Wrong definition of number of cores (%s)" % (type(numCores).__name__))

        if numNodes is not None:
            if isinstance(numNodes, int):
                numNodes = ResourceSize(numNodes)
            elif isinstance(numNodes, dict):
                numNodes = ResourceSize(**numNodes)
            elif not isinstance(numNodes, ResourceSize):
                raise IllegalJobDescription("Wrong definition of number of nodes (%s)" % (type(numNodes).__name__))

        if wt is not None:
            self.wt = self.__parseWt(wt)
        else:
            self.wt = None

        self.nodeCrs = None
        if not nodeCrs is None:
            if not isinstance(nodeCrs, dict):
                raise IllegalJobDescription("Wrong definition of Consumable Resources {} (must be a dictionary)".format(type(nodeCrs).__name__))

            self.nodeCrs = self.__validateCrs(nodeCrs)

        self.numCores = numCores
        self.numNodes = numNodes

    def hasNodes(self):
        return self.numNodes is not None

    def hasCores(self):
        return self.numCores is not None

    def hasNodeCrs(self):
        return self.nodeCrs is not None

    @property
    def cores(self):
        return self.numCores

    @property
    def nodes(self):
        return self.numNodes

    @property
    def crs(self):
        return self.nodeCrs

    def getMinimumNumberOfCores(self):
        minCores = 1
        if self.hasCores():
            if self.cores.isExact():
                minCores = self.cores.exact
            else:
                minCores = self.cores.range[0]

        if self.hasNodes():
            if self.nodes.isExact():
                minCores = minCores * self.nodes.exact
            else:
                minCores = minCores * self.nodes.range[0]

        return minCores

    def toDict(self):
        result = {}
        if self.hasCores():
            result['numCores'] = self.numCores.toDict()

        if self.hasNodes():
            result['numNodes'] = self.numNodes.toDict()

        if self.hasNodeCrs():
            result['nodeCrs'] = { crtype.name:value for (crtype,value) in self.nodeCrs.items() }

        return result

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)


class JobDependencies:
    def __validateJobList(self, jobList, errorMessage):
        if not isinstance(jobList, list):
            raise IllegalJobDescription(errorMessage)

        for jobName in jobList:
            if not isinstance(jobName, str):
                raise IllegalJobDescription(errorMessage)

    def __init__(self, after=None):
        self.after = []

        if after is not None:
            self.__validateJobList(after, "Dependency task's list must be an array of job names")
            self.after = after

    def hasDependencies(self):
        return len(self.after) > 0

    def toDict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)


class JobIteration:
    def __init__(self, start=None, stop=None):
        if stop is None:
            raise IllegalJobDescription("Missing stop iteration value")

        if start is None:
            start = 0

        if start >= stop:
            raise IllegalJobDescription("Job iteration stop greater or equal than start")

        self.start = start
        self.stop = stop

    def inRange(self, index):
        return index >= self.start and index < self.stop

    def iterations(self):
        return self.stop - self.start

    def toDict(self):
        return self.__dict__

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)

    def __str__(self):
        return "{}-{}".format(self.start, self.stop)


class SubJobState:
    def __init__(self):
        self.__state = JobState.QUEUED
        self.__history = []
        self.__messages = None
        self.__runtime = {}

    def getState(self):
        return self.__state

    def setState(self, state, errorMsg=None):
        assert isinstance(state, JobState), "Wrong state type"

        self.__state = state
        self.__history.append((state, datetime.now()))
        if errorMsg:
            self.appendMessage(errorMsg)

    def appendRuntime(self, data):
        self.__runtime.update(data)

    def getHistory(self):
        return self.__history

    def getMessages(self):
        return self.__messages

    def getRuntime(self):
        return self.__runtime

    def appendMessage(self, msg):
        if self.__messages is None:
            self.__messages = msg
        else:
            self.__messages = '\n'.join([self.__messages, msg])


class Job:
    @staticmethod
    def validate_jobname(jobname):
        return not ':' in jobname

    def __init__(self, name, execution, resources, iteration=None, dependencies=None, attributes=None):
        if name is None:
            raise IllegalJobDescription("Job name not defined")

        if not Job.validate_jobname(name):
            raise IllegalJobDescription("Invalid job name {}".format(name))

        self.__name = name

        if isinstance(execution, JobExecution):
            self.__execution = execution
        elif isinstance(execution, dict):
            self.__execution = JobExecution(**execution)
        else:
            raise IllegalJobDescription("Job execution not defined or wrong type")

        if isinstance(resources, JobResources):
            self.__resources = resources
        elif isinstance(resources, dict):
            self.__resources = JobResources(**resources)
        else:
            raise IllegalJobDescription("Job resources not defined or wrong type")

        if isinstance(iteration, JobIteration) or iteration is None:
            self.__iteration = iteration
        elif isinstance(iteration, dict):
            try:
                self.__iteration = JobIteration(**iteration)
            except IllegalJobDescription:
                raise
            except:
                raise IllegalJobDescription("Job iteration wrong specification")
        else:
            raise IllegalJobDescription("Job iteration wrong type")

        if isinstance(dependencies, JobDependencies) or dependencies is None:
            self.dependencies = dependencies
        elif isinstance(dependencies, dict):
            try:
                self.dependencies = JobDependencies(**dependencies)
            except IllegalJobDescription:
                raise
            except:
                raise IllegalJobDescription("Job dependencies wrong specification")
        else:
            raise IllegalJobDescription("Job dependencies wrong type")

        if not attributes is None and not isinstance(attributes, dict):
            raise IllegalJobDescription("Job attributes must be dictionary")
        self.attributes = attributes

        if self.__iteration:
            self.__subjobs = [ SubJobState() for i in range(self.__iteration.start, self.__iteration.stop)]
            self.__subjobs_notFinished = self.__iteration.iterations()
            self.__subjobs_failed = 0
        self.__history = []

        self.__runtime = {}

        # history must be initialized before
        self.__state = None
        self.setState(JobState.QUEUED)

        self.__messages = None

        # position in scheduling queue - None if not set
        self.__queuePos = None

    @property
    def name(self):
        return self.__name

    def getName(self, iteration=None):
        return self.__name if iteration is None else '{}:{}'.format(self.__name, iteration)

    @property
    def execution(self):
        return self.__execution

    @property
    def resources(self):
        return self.__resources

    def getSubjobs(self):
        return self.__subjobs

    def getSubjobsNotfinished(self):
        return self.__subjobs_notFinished

    def getSubjobsFailed(self):
        return self.__subjobs_failed

    def getHistory(self, iteration=None):
        if iteration is None:
            return self.__history
        else:
            return self.__getSubJob(iteration).getHistory()

    def getMessages(self, iteration=None):
        if iteration is None:
            return self.__messages
        else:
            return self.__getSubJob(iteration).getMessages()

    def getRuntime(self, iteration=None):
        if iteration is None:
            return self.__runtime
        else:
            return self.__getSubJob(iteration).getRuntime()

    def isIterative(self):
        return self.__iteration is not None

    def getIteration(self):
        return self.__iteration

    def getState(self, iteration=None):
        if iteration is None:
            return self.__state
        else:
            return self.__getSubJob(iteration).getState()

    def getStateStr(self, iteration=None):
        return self.getState(iteration).name

    def __getSubJob(self, iteration):
        return self.__subjobs[iteration - self.__iteration.start]

    def setState(self, state, iteration=None, errorMsg=None):
        """
        Change job/iteration state.

        :param state: the new state
        :param iteration: iteration index
        :param errorMsg: optional error message description

        :return: True if job iteration status change triggered job status change (for example the last iteration job
          finished, so the whole job also finished)
                 False - the parent job state has not been changed
        """
        assert isinstance(state, JobState), "Wrong state type"

        logging.debug('job {} iteration {} status changed to {} (final ? {})'.format(self.__name, iteration, state.name,
                                                                             state.isFinished()))

        if not iteration is None:
            self.__getSubJob(iteration).setState(state, errorMsg)

            if state.isFinished():
                self.__subjobs_notFinished = self.__subjobs_notFinished - 1

                if state == JobState.FAILED or state == JobState.OMITTED or state == JobState.CANCELED:
                    self.__subjobs_failed += 1

                logging.debug('currently not finished subjobs {}, failed {}'.format(self.__subjobs_notFinished,
                                                                                    self.__subjobs_failed))

                if self.__subjobs_notFinished == 0 and not self.__state.isFinished():
                    # all subjobs finished - change whole job state
                    final_state = JobState.SUCCEED if self.__subjobs_failed == 0 else JobState.FAILED
                    self.setState(final_state)
                    return final_state
        else:
            self.__state = state
            self.__history.append((state, datetime.now()))
            if errorMsg:
                self.appendMessage(errorMsg)

        return None

    def hasDependencies(self):
            return self.dependencies is not None and self.dependencies.hasDependencies()

    def appendMessage(self, msg):
        if self.__messages is None:
            self.__messages = msg
        else:
            self.__messages = '\n'.join([self.__messages, msg])

    def getQueuePos(self):
        return self.__queuePos

    def setQueuePos(self, pos):
        self.__queuePos = pos

    def clearQueuePos(self):
        self.__queuePos = None

    def appendRuntime(self, data, iteration):
        if not iteration is None:
            self.__getSubJob(iteration).appendRuntime(data)
        else:
            self.__runtime.update(data)

    def toDict(self):
        result = {
            'name': self.__name,
            'execution': self.__execution.toDict(),
            'resources': self.__resources.toDict()}

        if not self.__iteration is None:
            result['iteration'] = self.__iteration.toDict()

        if not self.dependencies is None:
            result['dependencies'] = self.dependencies.toDict()

        if not self.attributes is None:
            result['attributes'] = self.attributes

        return result

    def toJSON(self):
        return json.dumps(self.toDict(), indent=2)


class JobList:
    def __init__(self):
        self.__jmap = {}

    def parse_jobname(self, jobname):
        parts = jobname.split(':', 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else None

    def add(self, job):
        assert isinstance(job, Job), "Wrong job type '%s'" % (type(job).__name__)

        if self.exist(job.name):
            raise JobAlreadyExist(job.name)

        self.__jmap[job.name] = job

    def exist(self, jobName):
        return jobName in self.__jmap

    def get(self, jobName):
        return self.__jmap.get(jobName, None)

    def jobs(self):
        return self.__jmap.keys()

    def remove(self, jobName):
        del self.__jmap[jobName]
