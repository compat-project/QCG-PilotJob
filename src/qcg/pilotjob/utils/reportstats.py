import argparse
import json
import sys
import statistics
import traceback
import re

from qcg.pilotjob.utils.auxdir import find_single_aux_dir, find_report_files, find_log_files
from qcg.pilotjob.utils.util import parse_datetime

from json import JSONDecodeError
from datetime import datetime, timedelta
from os.path import exists, join, abspath, isdir
from os import listdir
from math import ceil



class JobsReportStats:

    def __init__(self, report_files, log_files=None, rt_files=None, verbose=False):
        """
        Analyze QCG-PJM execution.

        Args:
            report_files (list(str)) - list of paths to the report files
            log_files (list(str)) - list of paths to the log files
        """
        self.report_files = report_files
        self.log_files = log_files or []
        self.rt_files = rt_files or []

        self.verbose = verbose

        self.jstats = {}
        self.res = {}

        self._analyze()

    def job_stats(self):
        return self.jstats

    def resources(self):
        return self.res

    def _analyze(self):
        self._read_report_files(self.report_files)

        if self.log_files:
            self._parse_service_logs(self.log_files)

        if self.rt_files:
            self._parse_rt_logs(self.log_files)

    @staticmethod
    def _parse_allocation(allocation):
        nodes = {}
        if allocation:
            for node in allocation.split(','):
                nodes[node[:node.index('[')]] = node[node.index('[') + 1:-1].split(':')

        return nodes

    def _read_report_files(self, report_files):
        """
        Read QCG-PJM json report file.
        The read data with statistics data are written to the self.jstats dictionary.

        Args:
            report_file (str) - path to the QCG-PJM json report file
        """
        self.jstats = {'jobs': {}}
        min_queue, max_queue, min_start, max_finish = None, None, None, None

        if self.verbose:
            print(f'reading reports from {",".join(report_files)} files ...')

        for report_file in report_files:
            if self.verbose:
                print('parsing report file {} ...'.format(report_file))

            with open(report_file, 'r') as report_f:

                for line, entry in enumerate(report_f, 1):
                    try:
                        job_entry = json.loads(entry)
                    except JSONDecodeError as e:
                        raise Exception('wrong report "{}" file format: error in {} line: {}'.format(report_file, line, str(e)))

                    for attr in [ 'name', 'state', 'history', 'runtime' ]:
                        if not attr in job_entry:
                            raise Exception('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_file, attr))

                    rtime = None
                    if 'rtime' in job_entry['runtime']:
                        rtime_t = datetime.strptime(job_entry['runtime']['rtime'], "%H:%M:%S.%f")
                        rtime = timedelta(hours=rtime_t.hour, minutes=rtime_t.minute, seconds=rtime_t.second, microseconds=rtime_t.microsecond)

                    # find queued time
                    queued_state = list(filter(lambda st_en: st_en['state'] == 'QUEUED', job_entry['history']))

                    # find allocation creation time
                    schedule_state = list(filter(lambda st_en: st_en['state'] == 'SCHEDULED', job_entry['history']))

                    # find start executing time
                    exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job_entry['history']))

                    # find finish executing time
                    finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job_entry['history']))
                    assert len(finish_state) == 1, 'for job {} in line {}'.format(job_entry['name'], line)

                    job_nodes = None
                    allocation = job_entry.get('runtime', {}).get('allocation', None)
                    if allocation is not None:
                        job_nodes = JobsReportStats._parse_allocation(allocation)

                    queued_time = parse_datetime(queued_state[0]['date']) if queued_state else None
                    schedule_time = parse_datetime(schedule_state[0]['date']) if schedule_state else None
                    start_time = parse_datetime(exec_state[0]['date']) if exec_state else None
                    finish_time = parse_datetime(finish_state[0]['date']) if finish_state else None

                    self.jstats['jobs'][job_entry['name']] = {
                        'r_time': rtime,
                        'queue_time': queued_time,
                        'sched_time': schedule_time,
                        's_time': start_time,
                        'f_time': finish_time,
                        'name': job_entry['name'],
                        'nodes': job_nodes,
                        'pid': job_entry['runtime'].get('pid', None),
                        'pname': job_entry['runtime'].get('pname', None),
                        'runtime': job_entry['runtime'],
                        'history': job_entry['history'],
                        'state': job_entry['state'],
                        'messages': job_entry.get('messages'),
                    }

                    if queued_time:
                        if not min_queue or queued_time < min_queue:
                            min_queue = queued_time

                        if not max_queue or queued_time > max_queue:
                            max_queue = queued_time

                    if start_time:
                        if not min_start or start_time < min_start:
                            min_start = start_time

                        if not max_finish or finish_time > max_finish:
                            max_finish = finish_time

        self.jstats['first_queue'] = min_queue
        self.jstats['last_queue'] = max_queue
        self.jstats['queue_time'] = max_queue - min_queue if all((max_queue is not None, min_queue is not None)) else None
        self.jstats['first_start'] = min_start
        self.jstats['last_finish'] = max_finish
        self.jstats['execution_time'] = max_finish - min_start if all((max_finish is not None, min_start is not None)) else None
        self.jstats['total_time'] = max_finish - min_queue if all((max_finish is not None, min_queue is not None)) else None

        rtimes = [job['r_time'].total_seconds() for job in self.jstats['jobs'].values() if job['r_time']]

        launchtimes = [(job['s_time'] - job['sched_time']).total_seconds() for job in self.jstats['jobs'].values() if job['s_time'] and job['sched_time']]

        if rtimes:
            self.jstats['rstats'] = self._generate_series_stats(rtimes)

        if launchtimes:
            self.jstats['launchstats'] = self._generate_series_stats(launchtimes)

    def _generate_series_stats(self, serie):
        """
        Generate statistics about given data serie.

        Args:
            serie (float[]) - serie data

        Return:
            serie statistics in form of dictionary
        """
        stats = {}
        stats['max'] = max(serie)
        stats['min'] = min(serie)
        stats['mean'] = statistics.mean(serie)
        stats['median'] = statistics.median(serie)
        stats['median_lo'] = statistics.median_low(serie)
        stats['median_hi'] = statistics.median_high(serie)
        stats['stdev'] = statistics.stdev(serie)
        stats['pstdev'] = statistics.pstdev(serie)
        stats['var'] = statistics.variance(serie)
        stats['pvar'] = statistics.pvariance(serie)
        return stats

    def _parse_service_logs(self, service_log_files):
        res_regexp = re.compile('available resources: (\d+) \((\d+) used\) cores on (\d+) nodes')
        gov_regexp = re.compile('starting governor manager ...')
        resinfo_regexp = re.compile('selected (\w+) resources information')
        self.res = {}

        log_file = None

        if len(service_log_files) > 1:
            governor_logs = []

            # find log of governor manager
            for log_file in service_log_files:
                with open(log_file, 'r') as l_f:
                    for line in l_f:
                        m = gov_regexp.search(line.strip())
                        if m:
                            # found governor log
                            governor_logs.append(log_file)
                            break

            if len(governor_logs) != 1:
                print('warning: can not find single governor log (found files: {})'.format(','.join(governor_logs)))
                print('warning: selecting the first log file: {}'.format(service_log_files[0]))
                log_file = service_log_files[0]
            else:
                log_file = governor_logs[0]
        else:
            if service_log_files:
                log_file = service_log_files[0]

        if self.verbose:
            print('parsing log file {} ...'.format(log_file))

        with open(log_file, 'r') as s_f:
            for line in s_f:
                m = res_regexp.search(line.strip())
                if m:
                    self.res = { 'cores': int(m.group(1)) - int(m.group(2)), 'nodes': int(m.group(3)) }
                    if self.verbose:
                        print('found resources: {}'.format(str(self.res)))
                    break

    def _parse_rt_logs(self, rt_logs):
        min_real_start = None
        max_real_finish = None

        rt_jobs = 0

        for rt_log in rt_logs:
            try:
                if self.verbose:
                    print('reading real time log file {rt_log} ...')

                with open(rt_log, 'rt') as rt_file:
                    node_rtimes = json.load(rt_file)

                if 'rt' not in node_rtimes:
                    raise ValueError('wrong format - missing "rt" element')

                rtimes = node_rtimes.get('rt', {})
                for job_name, job_rtimes in rtimes.items():
                    if all((elem in job_rtimes for elem in ['s', 'f'])):
                        job_data = self.jstats.setdefault('jobs', {}).setdefault(job_name, {})

                        job_real_start = parse_datetime(job_rtimes.get('s'))
                        job_real_finish = parse_datetime(job_rtimes.get('f'))

                        job_data['real_start'] = job_real_start
                        job_data['real_finish'] = job_real_finish

                        if min_real_start is None or job_real_start < min_real_start:
                            min_real_start = job_real_start

                        if max_real_finish is None or job_real_finish > max_real_finish:
                            max_real_finish = job_real_finish

                        rt_jobs += 1
                    else:
                        if self.verbose:
                            print(f'warning: missing required elements for job {job_name} in rt log file {rt_log}')
            except Exception as exc:
                print(f'warning: can not read real time log file {rt_log}: {str(exc)}')

        if self.verbose:
            print(f'read {rt_jobs} jobs real time entries')

        if min_real_start is not None:
            self.jstats['min_real_start'] = min_real_start

        if max_real_finish is not None:
            self.jstats['max_real_finish'] = max_real_finish

        if all((min_real_start is not None, max_real_finish is not None)):
            self.jstats['total_real_time'] = (max_real_finish - min_real_start).total_seconds()

    def job_start_finish_launch_overheads(self, detail=False):
        total_start_overhead = 0
        total_finish_overhead = 0
        total_jobs = 0

        result = {}
        for job_name, job_data in self.jstats['jobs'].items():
            if all((elem in job_data for elem in ['real_start', 'real_finish', 's_time', 'f_time'])):
                real_job_start = job_data['real_start']
                real_job_finish = job_data['real_finish']
                qcg_job_start = job_data['s_time']
                qcg_job_finish = job_data['f_time']

                start_overhead = (real_job_start - qcg_job_start).total_seconds()
                finish_overhead = (real_job_finish - qcg_job_finish).total_seconds()

                if detail:
                    result.setdefault('jobs', {})[job_name] = {'start': start_overhead, 'finish': finish_overhead}

                total_start_overhead += start_overhead
                total_finish_overhead += finish_overhead
                total_jobs += 1

        if self.verbose:
            print('generated start/finish launch overheads for {total_jobs} jobs')

        result['start'] = total_start_overhead
        result['finish'] = total_finish_overhead
        result['total'] = total_start_overhead + total_finish_overhead
        result['job_start_avg'] = total_start_overhead/total_jobs
        result['job_finish_avg'] = total_finish_overhead/total_jobs
        result['job_avg'] = (total_start_overhead + total_finish_overhead)/total_jobs
        result['analyzed_jobs'] = total_jobs

        return result

    def _generate_gantt_dataframe(self, start_metric_name, finish_metric_name):
        jobs_chart = []

        for job_name, job_data in self.jstats.get('jobs', {}):
            if all(elem in job_data for elem in ['nodes', start_metric_name, finish_metric_name]):
                for node_name, cores in job_data.get('nodes', {}).items():
                    jobs_chart.extend([{'Job': job_name,
                                        'Start': str(job_data.get(start_metric_name)),
                                        'Finish': str(job_data.get(finish_metric_name)),
                                        'Core': f'{node_name}:{core}'} for core in cores])

        return jobs_chart

    def gantt(self, output_file, real=True):

        try:
            import plotly.express as px
            import pandas as pd
        except ImportError:
            raise ImportError('To generate gantt chart the following packages must be installed: '\
                    'plotly.express, pandas, kaleido')

        start_metric_name = 's_time'
        finish_metric_name = 'f_time'
        if real:
            start_metric_name = 'real_start'
            finish_metric_name = 'real_finish'

        jobs_chart = self._generate_gantt_dataframe(start_metric_name, finish_metric_name)

        for job_name, job_data in self.jstats.get('jobs', {}):
            if all(elem in job_data for elem in ['nodes', 'real_start', 'real_finish']):
                for node_name, cores in job_data.get('nodes', {}).items():
                    jobs_chart.extend([{'Job': job_name,
                                        'Start': job_data.get('real_start'),
                                        'Finish': job_data.get('real_finish'),
                                        'Core': f'{node_name}:{core}'} for core in cores])

        if self.verbose:
            print(f'generated {len(jobs_chart)} dataframes')

        df = pd.DataFrame(jobs_chart)
        fig = px.timeline(df, x_start='Start', x_end='Finish', y='Core', color='Task')
        fig.write_image(output_file)

    def resource_usage(self):
        resource_nodes = {}
        jobs = {}

        # assign jobs to cores
        for job_name, job_data in self.jstats.get('jobs', {}).items():
            if 'nodes' in job_data:
                for node_name, cores in job_data.get('nodes', {}).items():
                    for core in cores:
                        resource_nodes.setdefault(node_name, {}).setdefault(core, []).append(job_data)

        min_start_moment = self.jstats.get('min_real_start')
        max_finish_moment = self.jstats.get('max_real_finish')
        total_time = (max_finish_moment - min_start_moment).total_seconds()

        # sort jobs in each of the core by the start time
        for node_name, cores in resource_nodes.items():
            for core_name, core_jobs in cores.items():
                core_jobs.sort(key=lambda job: job['real_start'])

                core_unused = 0
                if core_jobs:
                    # moment between total scenario start and first job
                    core_initial_wait = (core_jobs[0]['real_start'] - min_start_moment).total_seconds()
                    core_unused += core_initial_wait

                    core_injobs_wait = 0
                    for job_nr in range(1,len(core_jobs)):
                        curr_job = core_jobs[job_nr]
                        prev_job = core_jobs[job_nr-1]

                        # moments between current job start and last job finish
                        core_injobs_wait += (curr_job['real_start'] - prev_job['real_finish']).total_seconds()
                    core_unused += core_injobs_wait

                    # moment between last job finish and total scenario finish
                    core_finish_wait = (max_finish_moment - core_jobs[-1]['real_finish']).total_seconds()
                    core_unused += core_finish_wait

                core_utilization = ((total_time - core_unused) / total_time) * 100
                print(f'node [{node_name}][{core_name}]: unused: {core_unused} (initial {core_initial_wait}, between jobs {core_injobs_wait}, finish {core_finish_wait}), utilization: {core_utilization:.1f}%')

    def print_stats(self):
        if self.jstats:
            print('{} jobs executed in {} secs'.format(len(self.jstats.get('jobs', {})),
                self.jstats['total_time'].total_seconds() if 'total_time' in self.jstats else 0))
            print('\t{:>20}: {}'.format('first job queued', str(self.jstats.get('first_queue', 0))))
            print('\t{:>20}: {}'.format('last job queued', str(self.jstats.get('last_queue', 0))))
            print('\t{:>20}: {}'.format('total queuing time', self.jstats['queue_time'].total_seconds() if 'queue_time' in self.jstats else 0))
            print('\t{:>20}: {}'.format('first job start', str(self.jstats.get('first_start', 0))))
            print('\t{:>20}: {}'.format('last job finish', str(self.jstats.get('last_finish', 0))))
            print('\t{:>20}: {}'.format('total execution time', self.jstats['execution_time'].total_seconds() if 'execution_time' in self.jstats else 0))
            
            print('jobs runtime statistics:')
            for k, v in self.jstats.get('rstats', {}).items():
                print('\t{:>20}: {}'.format(k, v))

            print('jobs launching statistics:')
            for k, v in self.jstats.get('launchstats', {}).items():
                print('\t{:>20}: {}'.format(k, v))

        if self.res:
            print('available resources:')
            for k, v in self.res.items():
                print('\t{:>20}: {}'.format(k, v))

