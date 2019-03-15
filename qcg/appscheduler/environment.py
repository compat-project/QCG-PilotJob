import os
import logging
import types

from qcg.appscheduler.slurmenv import in_slurm_allocation


class Environment:
    def __init__(self):
        pass

    def updateEnv(self, job, env):
        """
        Update job environment.

        :param job (ExecutorJob): job data
        :param env (dict): environment to update
        """
        raise NotImplementedError()


class CommonEnvironment(Environment):
    NAME = 'common'

    def __init__(self):
        super(CommonEnvironment, self).__init__()
        logging.info('initializing COMMON environment')

    def updateEnv(self, job, env):
        logging.info('updating common environment')

        env.update({
            'QCG_PM_NNODES': str(job.nnodes),
            'QCG_PM_NODELIST': job.nlist,
            'QCG_PM_NPROCS': str(job.ncores),
            'QCG_PM_NTASKS': str(job.ncores),
            'QCG_PM_STEP_ID': str(job.id),
            'QCG_PM_TASKS_PER_NODE': job.tasks_per_node
        })


class SlurmEnvironment(Environment):
    NAME = 'slurm'

    def __init__(self):
        super(SlurmEnvironment, self).__init__()
        logging.info('initializing SLURM environment')

    def __mergePerNodeSpec(self, strList):
        prev_value = None
        result = [ ]
        n = 1

        for t in strList.split(','):
            if prev_value is not None:
                if prev_value == t:
                    n += 1
                else:
                    if n > 1:
                        result.append("%s(x%d)" % (prev_value, n))
                    else:
                        result.append(prev_value)

                    prev_value = t
                    n = 1
            else:
                prev_value = t
                n = 1

        if prev_value is not None:
            if n > 1:
                result.append("%s(x%d)" % (prev_value, n))
            else:
                result.append(prev_value)

        return ','.join([str(el) for el in result])


    def __checkSameCores(self, tasksList):
        same = None

        for t in tasksList.split(','):
            if same is not None:
                if t != same:
                    return None
            else:
                same = t

        return same


    def updateEnv(self, job, env):
        merged_tasks_per_node = self.__mergePerNodeSpec(job.tasks_per_node)

        job.env.update({
            'SLURM_NNODES': str(job.nnodes),
            'SLURM_NODELIST': job.nlist,
            'SLURM_NPROCS': str(job.ncores),
            'SLURM_NTASKS': str(job.ncores),
            'SLURM_JOB_NODELIST': job.nlist,
            'SLURM_JOB_NUM_NODES': str(job.nnodes),
            'SLURM_STEP_NODELIST': job.nlist,
            'SLURM_STEP_NUM_NODES': str(job.nnodes),
            'SLURM_STEP_NUM_TASKS': str(job.ncores),
            'SLURM_JOB_CPUS_PER_NODE': merged_tasks_per_node,
            'SLURM_STEP_TASKS_PER_NODE': merged_tasks_per_node,
            'SLURM_TASKS_PER_NODE': merged_tasks_per_node 
        })

        same_cores = self.__checkSameCores(job.tasks_per_node)
        if same_cores is not None:
            job.env.update({ 'SLURM_NTASKS_PER_NODE': same_cores })

        # create host file
        hostfile = os.path.join(job.wdPath, ".%s.hostfile" % job.job.name)
        with open(hostfile, 'w') as f:
            for node in job.allocation.nodeAllocations:
                for i in range(0, node.cores):
                    f.write("%s\n" % node.node.name)

        job.env.update({
            'SLURM_HOSTFILE': hostfile
        })


def __select_auto_environment():
    if in_slurm_allocation():
        return SlurmEnvironment
    else:
        return CommonEnvironment


_available_envs = {
    'auto': __select_auto_environment,
    CommonEnvironment.NAME: CommonEnvironment,
    SlurmEnvironment.NAME: SlurmEnvironment
}


def getEnvironment(envName):
    if envName not in _available_envs:
        raise ValueError('environment {} not available'.format(envName))

    envType = _available_envs[envName]
#    logging.info('checking if {} is an function type {}'.format(str(envType), isinstance(envType, types.FunctionType)))
    return envType() if isinstance(envType, types.FunctionType) else envType
