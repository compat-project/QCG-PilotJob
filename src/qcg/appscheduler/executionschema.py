import os
import logging

from qcg.appscheduler.errors import *
from qcg.appscheduler.slurmenv import parse_slurm_resources, in_slurm_allocation
from qcg.appscheduler.localenv import parse_local_resources


class ExecutionSchema:

    @classmethod
    def GetSchema(cls, name, config):
        if name not in __SCHEMAS__:
            raise InternalError('Invalid execution schema name: %s' % name)

        logging.info('execution schema {}'.format(name))

        return __SCHEMAS__[name](config)

    def __init__(self, config):
        self.config = config

    def parseResources(self):
        raise NotImplementedError()

    def preprocess(self, exJob):
        pass


class SlurmExecution(ExecutionSchema):
    EXEC_NAME = 'slurm'

    def __init__(self, config):
        super(SlurmExecution, self).__init__(config)

    def parseResources(self):
        return parse_slurm_resources(self.config)

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
            "--multi-prog" ]

        if exJob.job.resources.wt:
            exJob.jobExecution.args.extend(["--time", "0:{}".format(int(exJob.job.resources.wt.total_seconds()))])

        exJob.jobExecution.args.append(runConfFile)


class DirectExecution(ExecutionSchema):
    EXEC_NAME = 'direct'

    def __init__(self, config):
        super(DirectExecution, self).__init__(config)

    def parseResources(self):
        return parse_local_resources(self.config)

    def preprocess(self, exJob):
        pass


def __detect_execution_schema(config):
    logging.info('determining execution schema ...')

    if in_slurm_allocation():
        logging.info('selected slurm execution schema ...')
        return SlurmExecution(config)
    else:
        logging.info('selected direct execution schema ...')
        return DirectExecution(config)
    

__SCHEMAS__ = {
    'auto': __detect_execution_schema,
    SlurmExecution.EXEC_NAME: SlurmExecution,
    DirectExecution.EXEC_NAME: DirectExecution
}
