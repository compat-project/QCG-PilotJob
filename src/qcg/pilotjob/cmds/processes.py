import click
import sys
import os
import json
import statistics
from termcolor import colored
from datetime import datetime, timedelta

from qcg.pilotjob.utils.auxdir import find_single_aux_dir, find_proc_traces_files, is_aux_dir
from qcg.pilotjob.utils.proc_traces import ProcTraces
from qcg.pilotjob.utils.util import parse_datetime
from qcg.pilotjob.utils.reportstats import JobsReportStats



def generate_series_stats(serie):
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


def print_stats(stats):
    print('{} jobs executed in {} secs'.format(len(stats.get('jobs', {})),
        stats['total_time'].total_seconds() if 'total_time' in stats else 0))
    print('\t{:>20}: {}'.format('first job queued', str(stats.get('first_queue', 0))))
    print('\t{:>20}: {}'.format('last job queued', str(stats.get('last_queue', 0))))
    print('\t{:>20}: {}'.format('total queuing time', stats['queue_time'].total_seconds() if 'queue_time' in stats else 0))
    print('\t{:>20}: {}'.format('first job start', str(stats.get('first_start', 0))))
    print('\t{:>20}: {}'.format('last job finish', str(stats.get('last_finish', 0))))
    print('\t{:>20}: {}'.format('total execution time', stats['execution_time'].total_seconds() if 'execution_time' in stats else 0))

    print('jobs runtime statistics:')
    for k, v in stats.get('rstats', {}).items():
        print('\t{:>20}: {}'.format(k, v))

    print('jobs launching statistics:')
    for k, v in stats.get('launchstats', {}).items():
        print('\t{:>20}: {}'.format(k, v))


def print_process_tree(procs, job_pid, pname=None, only_target=False):
    for process, level in procs.process_iterator(job_pid):
        created = process.get('created', None)
        if created is None:
            created = datetime.now()
        else:
            created = parse_datetime(created)

        if level == 0:
            root_start = created 
        
        start_after_root = (created - root_start).total_seconds()

        if level == 0:
            time_info = f'created {created}'
        else:
            time_info = f'after {start_after_root} secs'

        args = " ".join(process.get("cmdline", "X"))[:80]
        output=f'{" " * (2*(level+1))}{"-"*2}{process.get("pid", "X")}:{process.get("name", "X")} ({args}) ' +\
               f'node({process.get("node", "X")}) {time_info}'
        if process.get('name', 'X') == pname and not only_target:
            output=colored(output, attrs=['bold'])

        if not only_target or str(process.get('pid', 'X')) == str(job_pid) or process.get('name', 'X') == pname:
            print(output)


def find_child_processes_with_name(procs, pid, pname):
    result = []

    for process, _ in procs.process_iterator(job_pid):
        if process.get('name', 'X') == pname:
            result.append(process)

    return result


def read_logs(wdir, verbose):
    aux_dir = find_single_aux_dir(wdir)

    jobs_report_path = os.path.join(aux_dir, 'jobs.report')
    if not os.path.isfile(jobs_report_path):
        sys.stderr.write(f'error: pilotjob directory do not contain jobs.report file')
        sys.exit(1)

    proc_traces_paths = find_proc_traces_files(wdir)
    if not proc_traces_paths:
        sys.stderr.write(f'error: process traces log files not found in "{wdir}"')
        sys.exit(2)

    if verbose:
        print(f'found {len(proc_traces_paths)} process traces log files')

    stats = JobsReportStats([jobs_report_path]).job_stats()
    if verbose:
        print(f'job report file "{jobs_report_path}" read')

    procs = ProcTraces(proc_traces_paths)

    return stats, procs


@click.group()
def processes():
    pass


@processes.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('jobids', type=str, nargs=-1)
@click.option('--all', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def tree(wdir, jobids, all, verbose):
    stats, procs = read_logs(wdir, verbose)

    for job_name in jobids:
        job_data = stats.get('jobs', {}).get(job_name)
        if job_data is None:
            sys.stderr.write(f'warning: job "{job_name}" not found in jobs report')
            continue

        job_pid = job_data.get('pid')
        job_pname = job_data.get('pname')
        if not job_pid:
            sys.stderr.write(f'warning: not found job "{job_name}" PID')
            continue

        job_process = procs.get_process(job_pid)
        if job_process:
            print(f'job {job_name}, job process id {job_pid}, application name {job_pname}')
            print_process_tree(procs, job_pid, pname=job_pname, only_target=not all)
        else:
            sys.stderr.write(f'warning: process {job_pname or "unknown"} with PID {job_pid} not found on any node')


@processes.command()
@click.argument('wdir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('jobids', type=str, nargs=-1)
@click.option('--all', is_flag=True, default=False)
@click.option('--verbose', is_flag=True, default=False)
def apps(wdir, jobids, all, verbose):
    stats, procs = read_logs(wdir, verbose)

    for job_name in jobids:
        job_data = stats.get('jobs', {}).get(job_name)
        if job_data is None:
            sys.stderr.write(f'warning: job "{job_name}" not found in jobs report')
            continue

        job_pid = job_data.get('pid')
        job_pname = job_data.get('pname')
        if not job_pid:
            sys.stderr.write(f'warning: not found job "{job_name}" PID')
            continue

        job_process = procs.get_process(job_pid)
        if job_process:
            print(f'job {job_name}, job process id {job_pid}, application name {job_pname}')
            print_process_tree(procs, job_pid, pname=job_pname, only_target=not all)
        else:
            sys.stderr.write(f'warning: process {job_pname or "unknown"} with PID {job_pid} not found on any node')

        target_processes = find_child_processes_with_name(procs, job_pid, job_pname)
        print(f'found {len(target_processes)} target processes')


if __name__ == '__main__':
    processes()

