import logging
from os.path import abspath

from qcg.appscheduler.executionschema import ExecutionSchema
from qcg.appscheduler.config import Config
from qcg.appscheduler.environment import get_environment
from qcg.appscheduler.executionjob import LocalSchemaExecutionJob, LauncherExecutionJob
from qcg.appscheduler.resources import ResourcesType
import qcg.appscheduler.profile


class Executor:
    """Class for tracing execution of job iterations.

    Attributes:
        _manager (Manager): the manager instance
        _not_finished (dict(str,ExecutionJob)): map with started but not finished job iterations
        _config (dict): QCG-PilotJob configuration
        base_wd (str): path to the working directory
        aux_dir (str): path to the aux directory
        schema (ExecutionSchema): the execution schema
        job_envs (list(Environment)): the environent instances
        _resources (Resources): available resources
        _is_node_launcher (bool): is a launcher service running
    """

    def __init__(self, manager, config, resources):
        """Initialize instance.

        Args:
            manager (Manager): the manager instance
            config (dict): QCG-PilotJob configuration
            resources (Resources): available resources
        """
        self._manager = manager
        self._not_finished = {}
        self._config = config

        self.base_wd = abspath(Config.EXECUTOR_WD.get(config))
        self.aux_dir = abspath(Config.AUX_DIR.get(config))

        logging.info("executor base working directory set to %s", self.base_wd)

        self.schema = ExecutionSchema.get_schema(resources, config)

        envs_set = set([get_environment('common')])
        for env_name in set([env.lower() for env in Config.ENVIRONMENT_SCHEMA.get(config).split(',')]):
            if env_name:
                envs_set.add(get_environment(env_name))
        logging.info('job\' environment contains %s elements', str(envs_set))
        self.job_envs = [env() for env in envs_set]

        self._resources = resources

        self._is_node_launcher = False

        if self._resources.rtype == ResourcesType.SLURM and not Config.DISABLE_NL.get(config):
            logging.info('initializing custom launching method (node launcher)')
            try:
                LauncherExecutionJob.start_agents(self.base_wd, self.aux_dir, self._resources.nodes,
                                                  self._resources.binding)
                self._is_node_launcher = True
                logging.info('node launcher succesfully initialized')
            except Exception as exc:
                logging.error('failed to initialize node launcher agents: %s', str(exc))
                raise exc
        else:
            logging.info('custom launching method (node launcher) disabled')

    @property
    def zmq_address(self):
        """str: address of ZMQ interface"""
        return self._manager.zmq_address if self._manager else None

    async def stop(self):
        """Stop executor.
        If launcher agents are running, they will be stopped.
        """
        if self._is_node_launcher:
            try:
                await LauncherExecutionJob.stop_agents()
                self._is_node_launcher = False
            except Exception as exc:
                logging.error('failed to stop node launcher agents: %s', str(exc))

    @profile
    async def execute(self, allocation, job_iteration):
        """Asynchronusly execute job iteration inside allocation.
        After successfull prepared environment, a new execution job will be created
        for the job iteration, this function call will return before job iteration finish.

        Args:
            allocation (Allocation): allocation of resources for job iteration
            job_iteration (SchedulingIteration): job iteration execution details
        """
        job_iteration.job.append_runtime({'allocation': allocation.description()}, job_iteration.iteration)

        try:
            if all((self._is_node_launcher, len(allocation.nodes) == 1, allocation.nodes[0].ncores == 1)):
                execution_job = LauncherExecutionJob(self, self.job_envs, allocation, job_iteration)
            else:
                execution_job = LocalSchemaExecutionJob(self, self.job_envs, allocation, job_iteration, self.schema)

            self._not_finished[execution_job.jid] = execution_job

            await execution_job.run()
        except Exception as exc:
            if Config.PROGRESS.get(self._config):
                print("failed to start job {}".format(job_iteration.name))

            logging.exception("Failed to launch job %s", job_iteration.name)
            self._manager.job_finished(job_iteration, allocation, -1, str(exc))

    def is_all_jobs_finished(self):
        """bool: true if all started job iterations finished already"""
        return len(self._not_finished) == 0

    def job_iteration_started(self, execution_job):
        """Signal job iteration executing start.
        This function should be called by a execution job which created a process for job iteration.

        Args:
            execution_job (ExecutorJob): execution job iteration data
        """
        if Config.PROGRESS.get(self._config):
            print("executing job {} ...".format(execution_job.job_iteration.name))

        if self._manager is not None:
            self._manager.job_executing(execution_job.job_iteration)

    def job_iteration_finished(self, execution_job):
        """Signal job iteration finished.
        This function should be called by a execution job which created a process for job iteration.

        Args:
            execution_job (ExecutorJob): execution job iteration data
        """
        if Config.PROGRESS.get(self._config):
            print("job {} finished".format(execution_job.job_iteration.name))

        del self._not_finished[execution_job.id]

        if self._manager is not None:
            self._manager.job_finished(execution_job.job_iteration, execution_job.allocation, execution_job.exitCode,
                                       execution_job.error_message)
