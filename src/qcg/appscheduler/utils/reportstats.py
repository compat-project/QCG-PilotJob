import argparse
import json
import sys
import statistics
import traceback
import re

from json import JSONDecodeError
from datetime import datetime, timedelta
from os.path import exists, join
from math import ceil


class JobsReportStats:

    def __init__(self, args=None):
        """
        Analyze QCG-PJM execution.

        Args:
            args(str[]) - arguments, if None the command line arguments are parsed
        """
        self.__args = self.__get_argument_parser().parse_args(args)

        self.report_file = None
        self.log_file = None

        if self.__args.wdir and not exists(self.__args.wdir):
            raise Exception('working directory path "{}" not exists'.format(self.__args.wdir))

        if self.__args.report:
            if not exists(self.__args.report):
                raise Exception('report file {} not exists'.format(format(self.__args.report)))

            self.report_file = self.__args.report

        if self.__args.log:
            if not exists(self.__args.log):
                raise Exception('service log file "{}" not exists'.format(self.__args.log))

            self.log_file = self.__args.log

        if not self.report_file:
            if self.__args.wdir:
                self.report_file = join(self.__args.wdir, '.qcgpjm', 'jobs.report')

            if not self.report_file or not exists(self.report_file):
                raise Exception('report file not accessible or not defined')

        if not self.log_file:
            if self.__args.wdir:
                self.log_file = join(self.__args.wdir, '.qcgpjm', 'service.log')

        self.job_size = self.__args.job_size
        self.job_time = self.__args.job_time

        self.jstats = {}
        self.res = {}
        self.stats = {}


    def analyze(self):
        self.__read_report_file(self.report_file)

        if self.log_file:
            self.__parse_service_log(self.log_file)
        
        if self.job_size and self.job_time and self.res.get('cores', 0) > 0:
            self.__compute_perfect_runtime()

        self.__print_stats()


    def __get_argument_parser(self):
        """
        Create argument parser.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--report",
                            help="path to the jobs report file",
                            type=open, default=None)
        parser.add_argument("--log",
                            help="path to the service log file",
                            type=open, default=None)
        parser.add_argument("--wdir",
                            help="path to the service working directory",
                            default=None)
        parser.add_argument('--job-size', help='each job required number of cores',
                            type=int,
                            default=None)
        parser.add_argument('--job-time', help='each job expected runtime',
                            type=float,
                            default=None)

        return parser


    def __read_report_file(self, report_file):
        """
        Read QCG-PJM json report file.
        The read data with statistics data are written to the self.jstats dictionary.

        Args:
            report_file (str) - path to the QCG-PJM json report file
        """
        with open(report_file, 'r') as report_f:
            self.jstats = { 'jobs': { } }

            min_queue, max_queue, min_start, max_finish = None, None, None, None

            for line, entry in enumerate(report_f, 1):
                try:
                    job_entry = json.loads(entry)
                except JSONDecodeError as e:
                    raise Exception('wrong report "{}" file format: error in {} line: {}'.format(report_file, line, str(e)))

                for attr in [ 'name', 'state', 'history', 'runtime' ]:
                    if not attr in job_entry:
                        raise Exception('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_file, attr))

                rtime_t = datetime.strptime(job_entry['runtime']['rtime'], "%H:%M:%S.%f")
                rtime = timedelta(hours=rtime_t.hour, minutes=rtime_t.minute, seconds=rtime_t.second, microseconds=rtime_t.microsecond)

                # find queued time
                queued_state = list(filter(lambda st_en: st_en['state'] == 'QUEUED', job_entry['history']))
                assert len(queued_state) == 1

                # find allocation creation time
                schedule_state = list(filter(lambda st_en: st_en['state'] == 'SCHEDULED', job_entry['history']))
                assert len(schedule_state) == 1

                # find start executing time
                exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job_entry['history']))
                assert len(exec_state) == 1

                # find finish executing time
                finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job_entry['history']))
                assert len(finish_state) == 1

                queued_time = datetime.strptime(queued_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
                schedule_time = datetime.strptime(schedule_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
                start_time = datetime.strptime(exec_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
                finish_time = datetime.strptime(finish_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')

                self.jstats['jobs'][job_entry['name']] = { 'r_time': rtime, 'queue_time': queued_time, 'sched_time': schedule_time, 's_time': start_time, 'f_time': finish_time, 'name': job_entry['name'] }

                if not min_queue or queued_time < min_queue:
                    min_queue = queued_time

                if not max_queue or queued_time > max_queue:
                    max_queue = queued_time

                if not min_start or start_time < min_start:
                    min_start = start_time

                if not max_finish or finish_time > max_finish:
                    max_finish = finish_time

            self.jstats['first_queue'] = min_queue
            self.jstats['last_queue'] = max_queue
            self.jstats['queue_time'] = max_queue - min_queue
            self.jstats['first_start'] = min_start
            self.jstats['last_finish'] = max_finish
            self.jstats['execution_time'] = max_finish - min_start
            self.jstats['total_time'] = max_finish - min_queue

            rtimes = [ job['r_time'].total_seconds() for job in self.jstats['jobs'].values() ]

            launchtimes = [ (job['s_time'] - job['sched_time']).total_seconds() for job in self.jstats['jobs'].values() ]

            self.jstats['rstats'] = self.__generate_series_stats(rtimes)
            self.jstats['launchstats'] = self.__generate_series_stats(launchtimes)


    def __generate_series_stats(self, serie):
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


    def __parse_service_log(self, service_log_file):
        res_regexp = re.compile('available resources: (\d+) \((\d+) used\) cores on (\d+) nodes')
        resinfo_regexp = re.compile('selected (\w+) resources information')
        self.res = {}
        with open(service_log_file, 'r') as s_f:
            for line in s_f:
                m = res_regexp.search(line.strip())
                if m:
                    self.res = { 'cores': int(m.group(1)) - int(m.group(2)), 'nodes': int(m.group(3)) }
#                    print('found resources: {}'.format(str(self.res)))
                    break


    def __compute_perfect_runtime(self):
        job_cores = self.job_size * len(self.jstats.get('jobs', {}))
        res = self.res.get('cores', 0)
        self.stats['ideal_time'] = ceil(float(job_cores) / res) * self.job_time
        if 'total_time' in self.jstats:
            self.stats['efficiency'] = '{:.2f}%'.format(100.0 * self.stats['ideal_time'] / self.jstats['total_time'].total_seconds())


    def __print_stats(self):
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

        if self.stats:
            print('general statistics:')
            for k, v in self.stats.items():
                print('\t{:>20}: {}'.format(k, v))


if __name__ == "__main__":
    try:
        JobsReportStats().analyze()
    except Exception as e:
        sys.stderr.write('error: %s\n' % (str(e)))
        traceback.print_exc()
        exit(1)
