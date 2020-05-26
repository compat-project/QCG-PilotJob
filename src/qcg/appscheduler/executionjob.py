import asyncio
import logging
import uuid
import os
import json

from datetime import datetime
from string import Template

from qcg.appscheduler.joblist import JobExecution
from qcg.appscheduler.launcher.launcher import Launcher
import qcg.appscheduler.profile



class ExecutionJob:

    @profile
    def __init__(self, executor, envs, allocation, jobIteration):
        """
        Class is responsible for executing a single job iteration inside allocation.

        :param executor (Executor): object for tracing job iteration executions
        :param envs (Environment[]): job iteration environments
        :param allocation (Allocation) : scheduled allocation for job iteration
        :param jobIteration (SchedulingIteration) : job iteration to execute
        """
        assert allocation is not None
        assert jobIteration is not None

        self.allocation = allocation
        self.jobIteration = jobIteration
        self.__envs = envs
        self.id = str(uuid.uuid4().hex)
        self.exitCode = None
        self.__executor = executor
        self.errorMessage = None
        self.envOpts = { }

        # temporary
        self.wdPath = '.'

        # inherit environment variables from parent process
        self.env = os.environ.copy()
#        self.env = {}

        self.__jobStartTime = None
        self.__jobStopTime = None

        self.nnodes = len(self.allocation.nodeAllocations)
        self.ncores = sum([node.ncores for node in self.allocation.nodeAllocations])
        self.nlist = ','.join([node.node.name for node in self.allocation.nodeAllocations])
        self.tasks_per_node = ','.join([str(node.ncores) for node in self.allocation.nodeAllocations])

        #logging.info("job resource's initialized {}, {}, {}, {}".format(self.nnodes, self.ncores, self.nlist, self.tasks_per_node))

        self.__setupJobVariables()

        # job execution description with variables replaced
        self.jobExecution = JobExecution(
            **json.loads(
                self.__substituteJobVariables(self.jobIteration.job.execution.toJSON())
            ))

    def __setupJobVariables(self):
        """
        Set variables that can be used in job description.
        """
        self.__jobVars = {
            'root_wd': self.__executor.base_wd,
            'ncores': str(self.ncores),
            'nnodes': str(self.nnodes),
            'nlist': self.nlist
        }

        if self.jobIteration.iteration is not None:
            self.__jobVars['it'] = self.jobIteration.iteration

    def __substituteJobVariables(self, data):
        """
        Replace any variables used in job description.
        """
        return Template(data).safe_substitute(self.__jobVars)

    def __setupSandbox(self):
        """
        Set a job's working directory based on execution description and root working directory of executor.
        An attribute 'wdPath' is set as an output of this method and directory is created
        """
        if self.jobExecution.wd is None:
            self.wdPath = self.__executor.base_wd
        else:
            self.wdPath = self.jobExecution.wd

            if not os.path.isabs(self.wdPath):
                self.wdPath = os.path.join(self.__executor.base_wd, self.wdPath)

#        logging.info("preparing job %s sanbox at %s" % (self.jobIteration.name, self.wdPath))
        if not os.path.exists(self.wdPath):
#            logging.info("creating directory for job %s at %s" % (self.jobIteration.name, self.wdPath))
            os.makedirs(self.wdPath)


    def __prepareEnv(self):
        """
        Setup an execution environment.
        Mostly environment variables are set
        """
        if self.jobExecution.env is not None:
            self.env.update(self.jobExecution.env)

        if self.__executor.getZmqAddress():
            self.env.update({
                'QCG_PM_ZMQ_ADDRESS': self.__executor.getZmqAddress()
            })

#        logging.info('updating job\'s environment from {} objects'.format(str(self.__envs)))
        if self.__envs:
            for env in self.__envs:
                env.updateEnv(self, self.env, self.envOpts)

#        logging.info('environment after update: {}'.format(str(self.env)))


    def preprocess(self):
        """
        Prepare environment for job execution.
        Setup sandbox and environment variables. Resolve module loading and virtual environment activation.
        """
        self.__setupSandbox()
        self.__prepareEnv()
        self.__resolveModsVenv()


    def __resolveModsVenv(self):
        # in case of modules or virtualenv we have to run bash first with proper application
        # after the modules and virtualenv activation.
        if self.jobExecution.modules is not None or self.jobExecution.venv is not None:
            job_exec = self.jobExecution.exec
            job_args = self.jobExecution.args

            self.jobExecution.exec = 'bash'

            bash_cmd = ''
            if self.jobExecution.modules:
                bash_cmd += ' '.join(['source /etc/profile && module load {};'.format(mod) for mod in self.jobExecution.modules])

            if self.jobExecution.venv:
                bash_cmd += 'source {}/bin/activate;'.format(self.jobExecution.venv)

            if self.jobExecution.script:
                bash_cmd += self.jobExecution.script
            else:
                bash_cmd += 'exec {} {}'.format(
                    job_exec,
                    ' '.join([str(arg).replace(" ", "\ ") for arg in job_args]))

            self.jobExecution.args = [ '-c', bash_cmd ]
        else:
            if self.jobExecution.script:
                self.jobExecution.exec = 'bash'
                self.jobExecution.args = ['-c', self.jobExecution.script]


    def preStart(self):
        """
        This method should be executed before job will be started.
        The job statistics are updated (workdir, job start time) and executor is notified about job start.
        """
        self.__jobStartTime = datetime.now()

        self.__executor.jobIterationExecuting(self)
        self.jobIteration.job.appendRuntime({'wd': self.wdPath}, iteration=self.jobIteration.iteration)


    @profile
    async def launch(self):
        raise NotImplementedError('This method must be implemened in subclass')


    def postprocess(self, exitCode, errorMessage=None):
        """
        Postprocess job execution.
        Update job statistics (runtime), set the exit code and optionally error message and notify executor about job finish.
        """
        self.__jobStopTime = datetime.now()
        if self.__jobStartTime:
            self.__jobRunTime = self.__jobStopTime - self.__jobStartTime
        else:
            self.__jobRunTime = 0

        self.jobIteration.job.appendRuntime({'rtime': str(self.__jobRunTime)}, iteration=self.jobIteration.iteration)

        self.exitCode = exitCode
        self.errorMessage = errorMessage
        self.__executor.jobIterationFinished(self)


    async def run(self):
        """
        Prepare environment for a job and launch job process.
        """
        try:
            self.preprocess()

            self.preStart()

            await self.launch()
        except Exception as ex:
            logging.exception("failed to start job {}".format(self.jobIteration.name))
            self.postprocess(-1, str(ex))


class LocalSchemaExecutionJob(ExecutionJob):

    @profile
    def __init__(self, executor, envs, allocation, jobIteration, schema):
        """
        Run application as a local process according to defined schema.
        """
        super().__init__(executor, envs, allocation, jobIteration)
        
        self.__schema = schema
        self.__processTask = None

        if schema:
            self.envOpts = schema.getEnvOpts()


    def preprocess(self):
        """
        Prepare environment for job execution.
        Setup environment and modify exec according to the execution schema.
        """
        super().preprocess()
        self.__schema.preprocess(self)


    async def __executeLocalProcess(self):
        try:
            je = self.jobExecution
            stdinP = None
            stdoutP = asyncio.subprocess.DEVNULL
            stderrP = asyncio.subprocess.DEVNULL


            if je.stdin:
                stdin_path = je.stdin if os.path.isabs(je.stdin) else os.path.join(self.wdPath, je.stdin)
                stdinP = open(stdin_path, 'r')

            if je.stdout and je.stderr and je.stdout == je.stderr:
                stdout_path = je.stdout if os.path.isabs(je.stdout) else os.path.join(self.wdPath, je.stdout)
                stdoutP = stderrP = open(stdout_path, 'w')
            else:
                if je.stdout:
                    stdout_path = je.stdout if os.path.isabs(je.stdout) else os.path.join(self.wdPath, je.stdout)
                    stdoutP = open(stdout_path, 'w')

                if je.stderr:
                    stderr_path = je.stderr if os.path.isabs(je.stderr) else os.path.join(self.wdPath, je.stderr)
                    stderrP = open(stderr_path, 'w')

            logging.debug("launching job {}: {} {}".format(self.job.name, je.exec, str(je.args)))

            process = await asyncio.create_subprocess_exec(
                je.exec, *je.args,
                stdin=stdinP,
                stdout=stdoutP,
                stderr=stderrP,
                cwd=self.wdPath,
                env=self.env,
                shell=False,
            )

            logging.info("local process {} for job {} launched".format(process.pid, self.job.name))

            await process.wait()

            logging.info("local process for job {} finished".format(self.jobIteration.name))

            exitCode = process.returncode

            self.postprocess(exitCode)
        except Exception as e:
            logging.exception("execution failed: {}".format(str(e)))
            self.postprocess(-1, str(e))
        finally:
            try:
                if stdinP:
                    stdinP.close()
                if stdoutP != asyncio.subprocess.DEVNULL:
                    stdoutP.close()
                if stderrP != asyncio.subprocess.DEVNULL and stderrP != stdoutP:
                    stderrP.close()
            except Exception as e:
                logging.error("cleanup failed: {}".format(str(e)))


    @profile
    async def launch(self):
        self.__exec_task = asyncio.ensure_future(self.__executeLocalProcess())


    def postprocess(self, exitCode, errorMessage=None):
        super().postprocess(exitCode, errorMessage)


class LauncherExecutionJob(ExecutionJob):

    launcher = None

    @classmethod
    def StartAgents(cls, wdir, auxDir, nodes, binding):
        if cls.launcher:
            raise Exception('launcher agents already ininitialized')

        agents = [ { 'agent_id': node.name, 'slurm': { 'node': node.name }, 'options': { 'binding': binding, 'auxDir': auxDir } } for node in nodes ]
        cls.launcher = Launcher(wdir, auxDir)

        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(cls.launcher.start(agents)))


    @classmethod
    async def StopAgents(cls):
        if cls.launcher:
            await cls.launcher.stop()
            cls.launcher = None
            

    @profile
    def __init__(self, executor, envs, allocation, jobIteration):
        """
        Run application through the node launcher service.
        """
        super().__init__(executor, envs, allocation, jobIteration)
        self.envOpts = { 'nohostfile': True }


    @profile
    async def launch(self):
        """
        Submit job to the specific launcher agent.
        """
        je = self.jobExecution
        nodeAlloc = self.allocation.nodeAllocations[0]

        await self.__class__.launcher.submit(
                nodeAlloc.node.name,
                self.id,
                [ je.exec, *je.args ],
                stdin=os.path.join(self.wdPath, je.stdin) if je.stdin else None,
                stdout=os.path.join(self.wdPath, je.stdout) if je.stdout else None,
                stderr=os.path.join(self.wdPath, je.stderr) if je.stderr else None,
                env=self.env,
                wdir=self.wdPath,
                cores=nodeAlloc.cores,
                finish_cb=self.__finishCb)


    def __finishCb(self, message):
        self.postprocess(int(message.get('ec', -1)), message.get('message', None))

