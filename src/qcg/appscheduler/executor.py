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
from qcg.appscheduler.config import Config
from qcg.appscheduler.environment import getEnvironment
from qcg.appscheduler.executionjob import LocalSchemaExecutionJob, LauncherExecutionJob
from qcg.appscheduler.resources import ResourcesType
import qcg.appscheduler.profile




class Executor:
    def __init__(self, manager, config, resources):
        """
        Execute jobs inside allocations.
        """
        self.__manager = manager
        self.__notFinished = {}
        self.__config = config

        self.base_wd = abspath(Config.EXECUTOR_WD.get(config))
        self.aux_dir = abspath(Config.AUX_DIR.get(config))

        logging.info("executor base working directory set to %s" % (self.base_wd))

        self.schema = ExecutionSchema.GetSchema(resources, config)

        envsSet = set([getEnvironment('common')])
        for envName in set([env.lower() for env in Config.ENVIRONMENT_SCHEMA.get(config).split(',') ]):
            if envName:
                envsSet.add(getEnvironment(envName))
        logging.info('job\' environment contains {} elements'.format(str(envsSet)))
        self.jobEnvs = [ env() for env in envsSet ]

        self.__resources = resources

        self.__is_node_launcher = False

        if self.__resources.rtype == ResourcesType.SLURM and not Config.DISABLE_NL.get(config):
            logging.info('initializing custom launching method (node launcher)')
            try:
                LauncherExecutionJob.StartAgents(self.base_wd, self.aux_dir, self.__resources.nodes, self.__resources.binding)
                self.__is_node_launcher = True
                logging.info('node launcher succesfully initialized')
            except Exception as e:
                logging.error('failed to initialize node launcher agents: {}'.format(str(e)))
                raise e
        else:
            logging.info('custom launching method (node launcher) disabled')
    

    def getZmqAddress(self):
        return self.__manager.zmq_address if self.__manager else None


    async def stop(self):
        if self.__is_node_launcher:
            try:
                await LauncherExecutionJob.StopAgents()
                self.__is_node_launcher = False
            except Exception as e:
                logging.error('failed to stop node launcher agents: {}'.format(str(e)))
 
    @profile
    async def execute(self, allocation, job):
        """
        Asynchronusly execute job inside allocation.
        After successfull prepared environment, a new task will be created
        for the job, this function call will return before job finish.

        Args:
            allocation (Allocation): allocation of resources for job
            job (Job): job execution details
        """
#        logging.info("executing job %s" % (job.name))

        job.appendRuntime({'allocation': allocation.description()})

        try:
            if all((self.__is_node_launcher, len(allocation.nodeAllocations) == 1, allocation.nodeAllocations[0].ncores == 1)):
                execTask = LauncherExecutionJob(self, self.jobEnvs, allocation, job)
            else:
                execTask = LocalSchemaExecutionJob(self, self.jobEnvs, allocation, job, self.schema)

            self.__notFinished[execTask.id] = execTask
            await execTask.run()
        except Exception as e:
            logging.exception("Failed to launch job %s" % (job.name))
            self.__manager.jobFinished(job, allocation, -1, str(e))


    def allJobsFinished(self):
        return len(self.__notFinished) == 0


    def taskExecuting(self, task):
        if self.__manager is not None:
            self.__manager.jobExecuting(task.job)


    def taskFinished(self, task):
        """
        Signal job finished.
        This function should be called by a task which created a process for job.

        Args:
            task (ExecutorJob): task data
        """
        del self.__notFinished[task.id]

        if self.__manager is not None:
            self.__manager.jobFinished(task.job, task.allocation, task.exitCode, task.errorMessage)
