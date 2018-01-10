from qcg.appscheduler.allocation import Allocation, NodeAllocation
from qcg.appscheduler.joblist import JobState, Job, JobExecution
from qcg.appscheduler.errors import *

from asyncio.queues import Queue
import asyncio
import logging
import os
from os.path import abspath
import uuid
from datetime import datetime, timedelta


class ExecutorFinish:
	pass


class ExecutorJob:

	def __init__(self, executor, allocation, job):
		assert allocation is not None
		assert job is not None

		self.allocation = allocation
		self.job = job
		self.id = uuid.uuid4()
		self.__processTask = None
		self.__stdinF = None
		self.__stdoutF = None
		self.__stderrF = None
		self.exitCode = None
		self.__executor = executor
		self.errorMessage = None

	
	def __prepareEnv(self):
		self.__env = os.environ.copy()

		if self.job.execution.env is not None:
			self.__env.update(self.job.execution.env)

		self.__env.update({
					'QCG_STEP_ID': str(self.id)
			})

		nnodes = len(self.allocation.nodeAllocations)
		ncores = sum([ node.cores for node in self.allocation.nodeAllocations ])
		nlist = ','.join([ node.node.name for node in self.allocation.nodeAllocations ])
		tasks_per_node = ','.join([ str(node.cores) for node in self.allocation.nodeAllocations ])
		self.__env.update({
					'SLURM_NNODES': str(nnodes),
					'SLURM_NODELIST': nlist,
					'SLURM_NPROCS': str(ncores),
					'SLURM_NTASKS': str(ncores),
					'SLURM_JOB_NODELIST': nlist,
					'SLURM_JOB_NUM_NODES': str(nnodes),
					'SLURM_STEP_NODELIST': nlist,
					'SLURM_STEP_NUM_NODES': str(nnodes), 
					'SLURM_STEP_NUM_TASKS': str(ncores),
					'SLURM_NTASKS_PER_NODE': tasks_per_node,
					'SLURM_STEP_TASKS_PER_NODE': tasks_per_node,
					'SLURM_TASKS_PER_NODE': tasks_per_node
				})

		# create host file
		self.hostfile=abspath(os.path.join(self.job.execution.wd, ".%s.hostfile" % self.job.name))
		with open(self.hostfile, 'w') as f:
			for node in self.allocation.nodeAllocations:
				for i in range(0, node.cores):
					f.write("%s\n" % node.node.name)

		self.__env.update({
					'SLURM_HOSTFILE': self.hostfile
				})

		# create run configuration
		self.runConfFile=abspath(os.path.join(self.job.execution.wd, ".%s.runconfig" % self.job.name))
		with open(self.runConfFile, 'w') as f:
			f.write("0\t%s %s\n" % (
					self.job.execution.exec,
					' '.join('{0}'.format(arg) for arg in self.job.execution.args)))
			if ncores > 1:
				if ncores > 2:
					f.write("1-%d /bin/true\n" % (ncores - 1))
				else:
					f.write("1 /bin/true\n")
				
		# run via srun
		self.modifiedExec = "srun"
#		self.modifiedArgs = [ "-n", str(ncores), "--export=NONE", "-m", "arbitrary", "--multi-prog", self.runConfFile ]
#		self.modifiedArgs = [ "-n", str(ncores), "-m", "arbitrary", "--mem-per-cpu=0", "--slurmd-debug=verbose", "--multi-prog", self.runConfFile ]
		self.modifiedArgs = [ "-n", str(ncores), "-m", "arbitrary", "--mem-per-cpu=0", "--multi-prog", self.runConfFile ]


	def __prepareSandbox(self):
		if self.job.execution.wd is not None:
			wd = self.job.execution.wd

			logging.info("preparing job %s sanbox at %s" % (self.job.name, wd))
			if not os.path.exists(wd):
				logging.info("creating directory for job %s at %s" % (self.job.name, wd))
				os.makedirs(wd)


	async def __launch(self):
		je = self.job.execution
		stdoutP = asyncio.subprocess.DEVNULL
		stderrP = asyncio.subprocess.DEVNULL
		stdinP = asyncio.subprocess.DEVNULL
		cwd = '.'
		startedDt = datetime.now()

		exitCode = -1

		try:
			if je.wd is not None:
				cwd = je.wd

			if je.stdin is not None:
				stdinP = self.__stdinF = open(je.stdin, 'r')

			if je.stdout is not None:
				stdoutP = self.__stdoutF = open(os.path.join(cwd, je.stdout), 'w')

			if je.stderr is not None:
				stderrP = self.__stderrF = open(os.path.join(cwd, je.stderr), 'w')

			logging.info("creating process for job %s" % (self.job.name))
			logging.info("with args %s" % (str([ je.exec, *je.args])))

			self.job.appendRuntime( { 'wd': cwd } )

			process = await asyncio.create_subprocess_exec(
#					je.exec, *je.args,
					self.modifiedExec, *self.modifiedArgs,
					stdin = stdinP,
					stdout = stdoutP,
					stderr = stderrP,
					cwd = cwd,
					env = self.__env
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
			self.job.appendRuntime( { 'rtime': str(runTime) } )
		except Exception as e:
			logging.exception("Failed to set runtime for job %s" % (self.job.name))

		self.__postprocess(exitCode)


	def __postprocess(self, exitCode):

		self.exitCode = exitCode
		logging.info("Postprocessing job %s with exit code %d" % (self.job.name, self.exitCode))

		for f in [ self.__stdinF, self.__stdoutF, self.__stderrF ]:
			if f is not None:
				f.close()

		self.__stdinF = self.__stdoutF = self.__stderrF = None

		self.__processTask = None

		self.__executor.taskFinished(self)


	def run(self):
		try:
			logging.info("launching job %s" % (self.job.name))

			self.__prepareSandbox()
			self.__prepareEnv()

			self.__processTask = asyncio.get_event_loop().create_task(self.__launch())
		except Exception as ex:
			logging.exception("failed to start job %s" % (self.job.name))
			self.errorMessage = str(ex)
			self.exitCode = -1
			self.__executor.taskFinished(self)


class Executor:

	"""
	Execute jobs inside allocations.
	"""
	def __init__(self, manager):
		self.__manager = manager
		self.__notFinished = { }


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

		job.appendRuntime({ 'allocation': allocation.description() })
		execTask = ExecutorJob(self, allocation, job)
		self.__notFinished[execTask.id] = execTask
		execTask.run()


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

