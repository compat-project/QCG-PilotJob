import os

from qcg.pilotjob.errors import InternalError
from qcg.pilotjob.resources import ResourcesType


class ExecutionSchema:
    """Method of executing job.
    Currently two methods are supported:
        SlurmExecuition - jobs are run via 'srun' command
        DirectExecution - jobs are run as a normal processes

    Attributes:
        resources (Resources): available resources and their origin
        config (dict): QCG-PilotJob configuration
    """

    @classmethod
    def get_schema(cls, resources, config):
        """Create and return suitable instance of execution schema.

        Currently decision about type of execution schema is taken based on origin of resources - if QCG-PilotJob
        manager is run inside Slurm allocation, the SlurmExecution is selected. In other cases the DirectExecution
        schema is instantiated.

        Args:
            resources (Resources): available resources
            config (dict): QCG-PilotJob configuration

        Returns:
            ExecutionSchema: instance of execution schema
        """
        if resources.rtype not in __SCHEMAS__:
            raise InternalError('Unknown resources type: {}'.format(resources.rtype))

        return __SCHEMAS__[resources.rtype](resources, config)

    def __init__(self, resources, config):
        """Initialize instance.

        Args:
            resources (Resources): available resources
            config (dict): QCG-PilotJob configuration
        """
        self.resources = resources
        self.config = config

    def preprocess(self, ex_job):
        """Preprocess job iteration description before launching.
        This method might be implemented in child classes.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """

    def get_env_opts(self):
        """Return options for environment instances.
        This method might be implemented in child classes"""
        return {}


class SlurmExecution(ExecutionSchema):
    """The Slurm execution schema.
    The jobs are launched with ``srun`` command.
    """

    EXEC_NAME = 'slurm'

    def preprocess(self, ex_job):
        """"Preprocess job iteration description before launching.
        Prepare job iteration execution arguments.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """
        job_exec = ex_job.job_execution.exec
        job_args = ex_job.job_execution.args

        job_model = ex_job.job_execution.model

        # create run configuration
        if job_model != "threads":
            run_conf_file = os.path.join(ex_job.wd_path, ".{}.runconfig".format(ex_job.job_iteration.name))
            with open(run_conf_file, 'w') as conf_f:
                conf_f.write("0\t%s %s\n" % (
                    job_exec,
                    ' '.join('{0}'.format(str(arg).replace(" ", "\\ ")) for arg in job_args)))
                if ex_job.ncores > 1:
                    if ex_job.ncores > 2:
                        conf_f.write("1-%d /bin/true\n" % (ex_job.ncores - 1))
                    else:
                        conf_f.write("1 /bin/true\n")

            if self.resources.binding:
                core_ids = []
                for node in ex_job.allocation.nodes:
                    core_ids.extend([str(core) for core in node.cores])
                cpu_bind = "--cpu-bind=verbose,map_cpu:{}".format(','.join(core_ids))
            else:
                cpu_bind = "--cpu-bind=verbose,cores"

            ex_job.job_execution.args = [
                "-n", str(ex_job.ncores),
                "--overcommit",
                "--mem-per-cpu=0",
                cpu_bind,
                "--multi-prog"]
        else:
            cpu_mask = 0
            if self.resources.binding:
                core_ids = []
                for node in ex_job.allocation.nodes:
                    for core in node.cores:
                        cpu_mask = cpu_mask | 1 << core
                cpu_bind = "--cpu-bind=verbose,mask_cpu:{}".format(hex(cpu_mask))
            else:
                cpu_bind = "--cpu-bind=verbose,cores"

            ex_job.job_execution.args = [
                "-n", "1",
                "--cpus-per-task", str(ex_job.ncores),
                "--overcommit",
                "--mem-per-cpu=0",
                cpu_bind]

        ex_job.job_execution.exec = 'srun'

        if ex_job.job_execution.stdin:
            ex_job.job_execution.args.extend(["-i", os.path.join(ex_job.wd_path, ex_job.job_execution.stdin)])
            ex_job.job_execution.stdin = None

        if ex_job.job_execution.stdout:
            ex_job.job_execution.args.extend(["-o", os.path.join(ex_job.wd_path, ex_job.job_execution.stdout)])
            ex_job.job_execution.stdout = None

        if ex_job.job_execution.stderr:
            ex_job.job_execution.args.extend(["-e", os.path.join(ex_job.wd_path, ex_job.job_execution.stderr)])
            ex_job.job_execution.stderr = None

        if ex_job.job_iteration.resources.wt:
            ex_job.job_execution.args.extend(["--time", "0:{}".format(
                int(ex_job.job_iteration.resources.wt.total_seconds()))])

        if job_model != "threads":
            ex_job.job_execution.args.append(run_conf_file)
        else:
            ex_job.job_execution.args.extend([job_exec, *job_args])

        if self.resources.binding:
            ex_job.env.update({'QCG_PM_CPU_SET': ','.join([str(c) for c in sum(
                [alloc.cores for alloc in ex_job.allocation.nodes], [])])})


class DirectExecution(ExecutionSchema):
    """Directly execute job iteration without any proxy commands."""

    EXEC_NAME = 'direct'

    def preprocess(self, ex_job):
        """"Preprocess job iteration description before launching.
        Prepare job iteration execution arguments.

        Args
            ex_job (ExecutionJob): execution job iteration data
        """

    def get_env_opts(self):
        """Return options for environment instances.

        Set environments to not create 'hostfile'
        """
        return {'nohostfile': True}


__SCHEMAS__ = {
    ResourcesType.SLURM: SlurmExecution,
    ResourcesType.LOCAL: DirectExecution
}
