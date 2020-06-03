import argparse
import json
import sys
import statistics
import traceback
import re

from json import JSONDecodeError
from datetime import datetime, timedelta
from os.path import exists, join, abspath, isdir
from os import listdir
from math import ceil


AUX_DIR_PTRN = re.compile(r'\.qcgpjm-service-.*')

def find_aux_dirs(path):
    apath = abspath(path)
    return [join(apath, entry) for entry in listdir(apath) \
            if AUX_DIR_PTRN.match(entry) and isdir(join(apath, entry))]


def find_report_files(path):
    report_files = []

    print('looking for report files in path {} ...'.format(path))
    for aux_path in find_aux_dirs(path):
        report_file = join(aux_path, 'jobs.report')
        if exists(report_file):
            report_files.append(report_file)

    print('found report files: {}'.format(','.join(report_files)))
    return report_files


def find_log_files(path):
    log_files = []

    print('looking for log files in path {} ...'.format(path))
    for aux_path in find_aux_dirs(path):
        log_file = join(aux_path, 'service.log')
        if exists(log_file):
            log_files.append(log_file)

    print('found log files: {}'.format(','.join(log_files)))
    return log_files


class JobsReportStats:

    def __init__(self, args=None):
        """
        Analyze QCG-PJM execution.

        Args:
            args(str[]) - arguments, if None the command line arguments are parsed
        """
        self.__args = self.__get_argument_parser().parse_args(args)

        self.report_files = []
        self.log_files = []

        if self.__args.wdir and not exists(self.__args.wdir):
            raise Exception('working directory path "{}" not exists'.format(self.__args.wdir))

        if self.__args.reports:
            for report in self.__args.reports:
                if not exists(report):
                    raise Exception('report file {} not exists'.format(format(report)))

            self.report_files = self.__args.reports

        if self.__args.logs:
            for log_file in self.__args.logs:
                if not exists(log_file):
                    raise Exception('service log file "{}" not exists'.format(log_file))

            self.log_files = self.__args.logs

        if not self.report_files:
            if self.__args.wdir:
                self.report_files = find_report_files(self.__args.wdir)

            if not self.report_files or not all((exists(report) for report in self.report_files)):
                raise Exception('report file not accessible or not defined')

        if not self.log_files:
            if self.__args.wdir:
                self.log_files = find_log_files(self.__args.wdir)

        self.job_size = self.__args.job_size
        self.job_time = self.__args.job_time

        self.jstats = {}
        self.res = {}
        self.stats = {}


    def analyze(self):
        self.__read_report_files(self.report_files)

        if self.log_files:
            self.__parse_service_logs(self.log_files)
        
        if self.job_size and self.job_time and self.res.get('cores', 0) > 0:
            self.__compute_perfect_runtime()

        self.__print_stats()


    def __get_argument_parser(self):
        """
        Create argument parser.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--reports",
                            help="paths to the jobs report files",
                            type=open, default=None, nargs='*')
        parser.add_argument("--logs",
                            help="paths to the service log file",
                            type=open, default=None, nargs='*')
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


    def __parse_datetime(self, datetime_str):
        try:
            return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')


    def __read_report_files(self, report_files):
        """
        Read QCG-PJM json report file.
        The read data with statistics data are written to the self.jstats dictionary.

        Args:
            report_file (str) - path to the QCG-PJM json report file
        """
        self.jstats = {'jobs': {}}

        for report_file in report_files:
            print('parsing report file {} ...'.format(report_file))
            with open(report_file, 'r') as report_f:
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
                    assert len(finish_state) == 1, 'for job {} in line {}'.format(job_entry['name'], line)

                    queued_time = self.__parse_datetime(queued_state[0]['date'])
                    schedule_time = self.__parse_datetime(schedule_state[0]['date'])
                    start_time = self.__parse_datetime(exec_state[0]['date'])
                    finish_time = self.__parse_datetime(finish_state[0]['date'])

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


    def __parse_service_logs(self, service_log_files):
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

        print('parsing log file {} ...'.format(log_file))

        with open(log_file, 'r') as s_f:
            for line in s_f:
                m = res_regexp.search(line.strip())
                if m:
                    self.res = { 'cores': int(m.group(1)) - int(m.group(2)), 'nodes': int(m.group(3)) }
                    print('found resources: {}'.format(str(self.res)))
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
