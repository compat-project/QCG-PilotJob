import json

from datetime import datetime, timedelta

class JobInfo:

    def __init__(self):
        pass

    @staticmethod
    def fromChild(job_name, child_data):
        """
        Parse information about a sub job.
        """
        jinfo = JobInfo()
        jinfo.iteration = child_data['iteration']
        jinfo.name = '{}:{}'.format(job_name, jinfo.iteration)
        jinfo.state = child_data['state']

        jinfo.wdir = child_data.get('runtime', {}).get('wd')
        jinfo.nodes = JobInfo.__parse_allocation(child_data.get('runtime', {}).get('allocation', ''))
        jinfo.totalCores = sum(len(cores) for _, cores in jinfo.nodes.items())

        if child_data.get('runtime', {}).get('rtime'):
            jinfo.time = JobInfo.__parse_runtime(child_data.get('runtime', {}).get('rtime'))

        return jinfo

    @staticmethod
    def fromJob(job_data):
        """
        Parse information about a job.

        Available informations:
            name (str) - job name
            status (str) - job status
            nodes (dict(str, int[])) (optional) - dictionary with node names and list of allocated cores
            totalCores (int) - number of total allocated cores
            wdir (str) - working directory path
            time (timedelta) (optional) - job run time
            iterations (dict) (optional) - info about iterations
            childs (JobInfo[]) (optional) - a list of child jobs
            history (str[]) (optional) - list of job status change moments
            messages (str) - messages
        :param job_data: data obtained from QCG-PilotJob service
        """
        jinfo = JobInfo()

        jinfo.name = job_data.get('jobName')
        jinfo.status = job_data.get('status')

        jinfo.nodes = {}
        jinfo.totalCores = 0
        allocation = job_data.get('runtime', {}).get('allocation')
        if allocation:
            jinfo.nodes = JobInfo.__parse_allocation(allocation)
            jinfo.totalCores = sum(len(cores) for _, cores in jinfo.nodes.items())

        jinfo.wdir = job_data.get('runtime', {}).get('wd')
        time = job_data.get('runtime', {}).get('rtime')
        if time:
            jinfo.time = JobInfo.__parse_runtime(time)

        jinfo.iterations = job_data.get('iterations', {})

        if 'childs' in job_data:
            jinfo.childs = [ JobInfo.fromChild(jinfo.name, child_info) for child_info in job_data.get('childs') ]

        jinfo.history = [ event for event in job_data.get('runtime', {}).get('history', '').splitlines() if event ]
        jinfo.messages = job_data.get('messages', None)

        return jinfo


    @staticmethod
    def __parse_allocation(allocation):
        """
        Parse information about allocation in the form of nodes.
        
        :param allocation: allocation info from qcg-pm service
        :return: (dict) list of node names along with allocated cores on each of the node
        """
        nodes = {}
        for node in allocation.split(','):
            nodes[node[:node.index('[')]] = node[node.index('[') + 1:-1].split(':')

        return nodes

    @staticmethod
    def __parse_runtime(runtime):
        """
        Parse timedelta returned by qcg-pm service

        :param runtime: timedelta in string format
        :return: timedelta representation
        """
        dt = datetime.strptime(runtime, "%H:%M:%S.%f")
        return timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond)


    def __str__(self):
        """
        Return json string with all available fields

        :return: json string with job data
        """
        return json.dumps({ attr: str(getattr(self, attr)) for attr in dir(self) \
                            if not attr.startswith('_') and not callable(getattr(self, attr)) })
