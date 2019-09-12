import os
import logging

from qcg.appscheduler.errors import *
from qcg.appscheduler.resources import ResourcesType


class ExecutionSchema:

    @classmethod
    def GetSchema(cls, resources, config):
        if resources.rtype not in __SCHEMAS__:
            raise InternalError('Unknown resources type: %s' % name)

        return __SCHEMAS__[resources.rtype](config)

    def __init__(self, config):
        self.config = config

    def preprocess(self, exJob):
        pass

    def getEnvOpts(self):
        return { }


class SlurmExecution(ExecutionSchema):
    EXEC_NAME = 'slurm'

    def __init__(self, config):
        super(SlurmExecution, self).__init__(config)

    def preprocess(self, exJob):
        job_exec = exJob.jobExecution.exec
        job_args = exJob.jobExecution.args

        # create run configuration
        runConfFile = os.path.join(exJob.wdPath, ".%s.runconfig" % exJob.job.name)
        with open(runConfFile, 'w') as f:
            f.write("0\t%s %s\n" % (
                job_exec,
                ' '.join('{0}'.format(str(arg).replace(" ", "\ ")) for arg in job_args)))
            if exJob.ncores > 1:
                if exJob.ncores > 2:
                    f.write("1-%d /bin/true\n" % (exJob.ncores - 1))
                else:
                    f.write("1 /bin/true\n")

        #        exJob.modifiedArgs = [ "-n", str(exJob.ncores), "--export=NONE", "-m", "arbitrary", "--multi-prog", runConfFile ]
        #        exJob.modifiedArgs = [ "-n", str(exJob.ncores), "-m", "arbitrary", "--mem-per-cpu=0", "--slurmd-debug=verbose", "--multi-prog", runConfFile ]

        exJob.jobExecution.exec = 'srun'
        exJob.jobExecution.args = [
            "-n", str(exJob.ncores),
            "-m", "arbitrary",
            "--mem-per-cpu=0",
#            "--cpu-bind=verbose,map_cpu:{}".format(','.join([cores for node in exJob.allocation.nodeAllocations]))
            "--cpu-bind=verbose,map_cpu:{}".format(','.join([str(core) for core in exJob.allocation.nodeAllocations[0].cores])),
#            "--cpu-bind=verbose,cores",
            "--multi-prog" ]

        if exJob.jobExecution.stdin:
            exJob.jobExecution.args.extend(["-i", os.path.join(exJob.wdPath, exJob.jobExecution.stdin)])
            exJob.jobExecution.stdin = None

        if exJob.jobExecution.stdout:
            exJob.jobExecution.args.extend(["-o", os.path.join(exJob.wdPath, exJob.jobExecution.stdout)])
            exJob.jobExecution.stdout = None

        if exJob.jobExecution.stderr:
            exJob.jobExecution.args.extend(["-e", os.path.join(exJob.wdPath, exJob.jobExecution.stderr)])
            exJob.jobExecution.stderr = None

        if exJob.job.resources.wt:
            exJob.jobExecution.args.extend(["--time", "0:{}".format(int(exJob.job.resources.wt.total_seconds()))])

        exJob.jobExecution.args.append(runConfFile)


class DirectExecution(ExecutionSchema):
    EXEC_NAME = 'direct'

    def __init__(self, config):
        super(DirectExecution, self).__init__(config)

    def preprocess(self, exJob):
        pass

    def getEnvOpts(self):
        return { 'nohostfile': True }


__SCHEMAS__ = {
    ResourcesType.SLURM: SlurmExecution,
    ResourcesType.LOCAL: DirectExecution
}
