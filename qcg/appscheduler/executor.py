import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from os.path import abspath
from string import Template

from qcg.appscheduler.executionschema import ExecutionSchema
from qcg.appscheduler.joblist import JobExecution
from qcg.appscheduler.zmqinterface import ZMQInterface


class ExecutorFinish:
    pass


class ExecutorJob:

    def __init__(self, executor, schema, allocation, job):
        assert allocation is not None
        assert job is not None
        assert schema is not None

        self.allocation = allocation
        self.job = job
        self.__schema = schema
        self.id = uuid.uuid4()
        self.__processTask = None
        self.__stdinF = None
        self.__stdoutF = None
        self.__stderrF = None
        self.exitCode = None
        self.__executor = executor
        self.errorMessage = None

        # temporary
        self.wdPath = '.'

        # inherit environment variables from parent process
        self.env = os.environ.copy()

        self.nnodes = len(self.allocation.nodeAllocations)
        self.ncores = sum([node.cores for node in self.allocation.nodeAllocations])
        self.nlist = ','.join([node.node.name for node in self.allocation.nodeAllocations])
        self.tasks_per_node = ','.join([str(node.cores) for node in self.allocation.nodeAllocations])

        self.__setupJobVariables()

        # job execution description with variables replaced
        self.jobExecution = JobExecution(
            **json.loads(
                self.__substituteJobVariables(self.job.execution.toJSON())
            ))

    def __setupJobVariables(self):
        self.__jobVars = {
            'root_wd': self.__executor.base_wd,
            'ncores': str(self.ncores),
            'nnodes': str(self.nnodes),
            'nlist': self.nlist
        }

    def __substituteJobVariables(self, data):
        if isinstance(data, str):
            return Template(data).safe_substitute(self.__jobVars)
        else:
            return json.loads(Template(json.dumps(data)).safe_substitute(self.__jobVars))

    """
    Set a job's working directory based on execution description and root working directory of executor.
    An attribute 'wdPath' is set as an output of this method and directory is created
    """

    def __setupSandbox(self):
        if self.jobExecution.wd is None:
            self.wdPath = self.__executor.base_wd
        else:
            self.wdPath = self.jobExecution.wd

            if not os.path.isabs(self.wdPath):
                self.wdPath = os.path.join(self.__executor.base_wd, self.wdPath)

        logging.info("preparing job %s sanbox at %s" % (self.job.name, self.wdPath))
        if not os.path.exists(self.wdPath):
            logging.info("creating directory for job %s at %s" % (self.job.name, self.wdPath))
            os.makedirs(self.wdPath)

    """
    Setup an execution environment.
    Mostly environment variables are set
    """

    def __prepareEnv(self):
        if self.jobExecution.env is not None:
            self.env.update(self.jobExecution.env)

        self.env.update({
            'QCG_PM_NNODES': str(self.nnodes),
            'QCG_PM_NODELIST': self.nlist,
            'QCG_PM_NPROCS': str(self.ncores),
            'QCG_PM_NTASKS': str(self.ncores),
            'QCG_PM_STEP_ID': str(self.id),
            'QCG_PM_TASKS_PER_NODE': self.tasks_per_node
        })

        if hasattr(self.__executor, 'zmq_address'):
            self.env.update({
                'QCG_PM_ZMQ_ADDRESS': self.__executor.zmq_address
            })

    """
    Prepare environment for job execution.
    Setup sandbox, environment variables and modification of exec according to the execution schema
    is made
    """

    def preprocess(self):
        self.__setupSandbox()
        self.__prepareEnv()

        self.__schema.preprocess(self)

    async def __launch(self):
        je = self.jobExecution
        stdoutP = asyncio.subprocess.DEVNULL
        stderrP = asyncio.subprocess.DEVNULL
        stdinP = asyncio.subprocess.DEVNULL
        startedDt = datetime.now()

        exitCode = -1

        try:
            if je.stdin is not None:
                stdinP = self.__stdinF = open(je.stdin, 'r')

            if je.stdout is not None:
                stdoutP = self.__stdoutF = open(os.path.join(self.wdPath, je.stdout), 'w')

            if je.stderr is not None:
                stderrP = self.__stderrF = open(os.path.join(self.wdPath, je.stderr), 'w')

            logging.info("creating process for job %s" % (self.job.name))
            logging.info("with args %s" % (str([je.exec, *je.args])))

            self.job.appendRuntime({'wd': self.wdPath})

            process = await asyncio.create_subprocess_exec(
                je.exec, *je.args,
                stdin=stdinP,
                stdout=stdoutP,
                stderr=stderrP,
                cwd=self.wdPath,
                env=self.env
            )

            logging.info("job %s launched" % (self.job.name))

            await process.wait()

            await asyncio.sleep(1)

            exitCode = process.returncode
        except Exception as e:
            logging.exception("Process for job %s terminated" % (self.job.name))
            self.errorMessage = str(e)
            exitCode = -1

        try:
            runTime = datetime.now() - startedDt
            self.job.appendRuntime({'rtime': str(runTime)})
        except Exception as e:
            logging.exception("Failed to set runtime for job %s" % (self.job.name))

        self.__postprocess(exitCode)

    def __postprocess(self, exitCode):
        self.exitCode = exitCode
        logging.info("Postprocessing job %s with exit code %d" % (self.job.name, self.exitCode))

        for f in [self.__stdinF, self.__stdoutF, self.__stderrF]:
            if f is not None:
                f.close()

        self.__stdinF = self.__stdoutF = self.__stderrF = None

        self.__processTask = None

        self.__executor.taskFinished(self)

    def run(self):
        try:
            logging.info("launching job %s" % (self.job.name))

            self.preprocess()

            self.__processTask = asyncio.get_event_loop().create_task(self.__launch())
        except Exception as ex:
            logging.exception("failed to start job %s" % (self.job.name))
            self.errorMessage = str(ex)
            self.exitCode = -1
            self.__executor.taskFinished(self)


class Executor:
    EXECUTOR_WD = 'executor.wd'
    EXECUTION_SCHEMA = 'direct'

    CONF_DEFAULT = {
        EXECUTOR_WD: '.',
        EXECUTION_SCHEMA: 'direct'
    }

    """
    Execute jobs inside allocations.
    """

    def __init__(self, manager, config):
        self.__manager = manager
        self.__notFinished = {}
        self.__config = config

        self.base_wd = abspath(config.get(self.EXECUTOR_WD, self.CONF_DEFAULT[self.EXECUTOR_WD]))
        self.schemaName = config.get(self.EXECUTION_SCHEMA, self.CONF_DEFAULT[self.EXECUTION_SCHEMA])

        logging.info("executor base working directory set to %s" % (self.base_wd))

        self.schema = ExecutionSchema.GetSchema(self.schemaName, config)

        if ZMQInterface.CONF_ZMQ_IFACE_ADDRESS in config:
            self.zmq_address = config[ZMQInterface.CONF_ZMQ_IFACE_ADDRESS]


    """
    Asynchronusly execute job inside allocation.
    After successfull prepared environment, a new task will be created
    for the job, this function call will return before job finish.

    Args:
        allocation (Allocation): allocation of resources for job
        job (Job): job execution details
    """

    def execute(self, allocation, job):
        logging.info("executing job %s" % (job.name))

        job.appendRuntime({'allocation': allocation.description()})

        try:
            execTask = ExecutorJob(self, self.schema, allocation, job)
            self.__notFinished[execTask.id] = execTask
            execTask.run()
        except Exception as e:
            logging.exception("Failed to launch job %s" % (job.name))
            self.__manager.jobFinished(job, allocation, -1, str(e))

    """
    Wait for all job finish execution.
    """

    async def waitForUnfinished(self):
        while len(self.__notFinished) > 0:
            logging.info("Still %d unfinished jobs" % (len(self.__notFinished)))

            if len(self.__notFinished) > 0:
                await asyncio.sleep(1)

    """
    Signal job finished.
    This function should be called by a task which created a process for job.

    Args:
        task (ExecutorJob): task data
    """

    def taskFinished(self, task):
        del self.__notFinished[task.id]

        if self.__manager is not None:
            self.__manager.jobFinished(task.job, task.allocation, task.exitCode, task.errorMessage)
