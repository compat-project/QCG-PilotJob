import json

from datetime import datetime, timedelta

class JobInfo:

    def __init__(self, job_data):
        self.name = job_data.get('jobName')
        self.status = job_data.get('status')

        self.nodes = {}
        self.totalCores = 0
        allocation = job_data.get('runtime', {}).get('allocation')
        if allocation:
            for node in allocation.split(','):
                self.nodes[node[:node.index('[')]] = node[node.index('[') + 1:-1].split(':')

            self.totalCores = sum(len(cores) for _, cores in self.nodes.items())

        self.wdir = job_data.get('runtime', {}).get('wd')
        time = job_data.get('runtime', {}).get('rtime')
        if time:
            dt = datetime.strptime(time, "%H:%M:%S.%f")
            self.time = timedelta(hours=dt.hour, minutes=dt.minute, seconds=dt.second, microseconds=dt.microsecond)

        self.history = [ event for event in job_data.get('runtime', {}).get('history', '').splitlines() if event ]
        self.messages = job_data.get('messages', None)

    def __str__(self):
        return json.dumps({ attr: str(getattr(self, attr)) for attr in dir(self) if not attr.startswith('_')})
